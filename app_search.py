# In file: app_search.py

import os
import logging
import threading
import secrets
from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg
from psycopg.rows import dict_row
import smtplib
import ssl
from email.message import EmailMessage

from db_manager import DatabaseConnection
from utils import normalize_search_term_for_hybrid
from config import APIConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

def _group_and_shape_results(all_rows, ordered_camis):
    if not all_rows:
        return []
    restaurant_details_map = {str(camis): [] for camis in ordered_camis}
    for row in all_rows:
        restaurant_details_map[str(row['camis'])].append(row)
    final_results = []
    for camis in ordered_camis:
        camis_str = str(camis)
        rows_for_restaurant = restaurant_details_map.get(camis_str)
        if not rows_for_restaurant:
            continue
        base_info = dict(rows_for_restaurant[0])
        inspections = {}
        for row in rows_for_restaurant:
            insp_date_str = row['inspection_date'].isoformat()
            if insp_date_str not in inspections:
                inspections[insp_date_str] = {
                    'inspection_date': insp_date_str,
                    'grade': row.get('grade'),
                    'grade_date': row['grade_date'].isoformat() if row.get('grade_date') else None,
                    'critical_flag': row.get('critical_flag'),
                    'inspection_type': row.get('inspection_type'),
                    'action': row.get('action'),
                    'violations': []
                }
            if row.get('violation_code'):
                v_data = {'violation_code': row['violation_code'], 'violation_description': row['violation_description']}
                if v_data not in inspections[insp_date_str]['violations']:
                    inspections[insp_date_str]['violations'].append(v_data)
        base_info['inspections'] = sorted(list(inspections.values()), key=lambda x: x['inspection_date'], reverse=True)
        for key in ['violation_code', 'violation_description', 'grade', 'grade_date', 'action', 'inspection_date', 'critical_flag', 'inspection_type']:
            base_info.pop(key, None)
        final_results.append(base_info)
    return final_results

def send_report_email(report_data):
    sender_email = os.environ.get("SENDER_EMAIL")
    receiver_email = os.environ.get("RECEIVER_EMAIL")
    password = os.environ.get("SENDER_PASSWORD")

    if not all([sender_email, receiver_email, password]):
        logger.error("Email credentials are not fully configured. Cannot send report.")
        return False

    camis = report_data.get("camis", "N/A")
    issue_type = report_data.get("issue_type", "N/A")
    comments = report_data.get("comments", "No comments.")
    subject = f"New Issue Report for Restaurant CAMIS: {camis}"
    body = f"A new issue has been reported by a user.\n\nRestaurant CAMIS: {camis}\nIssue Type: {issue_type}\n\nComments:\n{comments}"

    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = receiver_email

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.aol.com", 465, context=context) as server:
            server.login(sender_email, password)
            server.send_message(msg)
            logger.info(f"Successfully sent report email for CAMIS {camis}")
        return True
    except Exception as e:
        logger.error(f"Failed to send email: {e}", exc_info=True)
        return False

@app.route('/search', methods=['GET'])
def search():
    search_term = request.args.get('name', '').strip()
    grade_filter = request.args.get('grade', type=str)
    boro_filter = request.args.get('boro', type=str)
    cuisine_filter = request.args.get('cuisine', type=str)
    sort_option = request.args.get('sort', type=str)
    page = int(request.args.get('page', 1, type=int))
    per_page = int(request.args.get('per_page', 25, type=int))

    if not search_term:
        return jsonify([])

    normalized_search = normalize_search_term_for_hybrid(search_term)
    
    # --- MODIFIED LOGIC FOR FILTERS ---
    where_conditions = ["(dba_normalized_search ILIKE %s OR similarity(dba_normalized_search, %s) > 0.4)"]
    params = [f"%{normalized_search}%", normalized_search]

    if grade_filter:
        grade_upper = grade_filter.upper()
        if grade_upper == 'P':
            where_conditions.append("grade IN ('P', 'Z')")
        elif grade_upper == 'CLOSED':
            where_conditions.append("action ILIKE %s")
            params.append('%closed by dohmh%')
        else:
            where_conditions.append("grade = %s")
            params.append(grade_upper)
            
    if boro_filter:
        where_conditions.append("boro ILIKE %s")
        params.append(boro_filter)
    if cuisine_filter:
        where_conditions.append("cuisine_description ILIKE %s")
        params.append(f"%{cuisine_filter}%")
        
    where_clause = " AND ".join(where_conditions)
    # --- END MODIFIED LOGIC ---

    order_by_clause = ""
    order_by_params = []
    if sort_option == 'name_asc':
        order_by_clause = "ORDER BY dba ASC"
    elif sort_option == 'name_desc':
        order_by_clause = "ORDER BY dba DESC"
    elif sort_option == 'date_desc':
        order_by_clause = "ORDER BY inspection_date DESC"
    elif sort_option == 'grade_asc':
        order_by_clause = "ORDER BY CASE WHEN grade = 'A' THEN 1 WHEN grade = 'B' THEN 2 WHEN grade = 'C' THEN 3 ELSE 4 END, dba ASC"
    else:
        order_by_clause = "ORDER BY CASE WHEN dba_normalized_search = %s THEN 0 WHEN dba_normalized_search ILIKE %s THEN 1 ELSE 2 END, similarity(dba_normalized_search, %s) DESC, length(dba_normalized_search)"
        order_by_params = [normalized_search, f"{normalized_search}%", normalized_search]

    # --- REWRITTEN QUERY TO FIX FILTER BUG ---
    # This query now correctly finds the latest inspection for each restaurant first,
    # and only then applies the filters to that latest record.
    id_fetch_query = f"""
        WITH latest_restaurants AS (
            SELECT DISTINCT ON (camis) *
            FROM restaurants
            ORDER BY camis, inspection_date DESC
        )
        SELECT camis 
        FROM latest_restaurants 
        WHERE {where_clause}
        {order_by_clause}
        LIMIT %s OFFSET %s;
    """
    offset = (page - 1) * per_page
    id_fetch_params = tuple(params + order_by_params + [per_page, offset])
    # --- END REWRITTEN QUERY ---

    try:
        with DatabaseConnection() as conn:
            conn.row_factory = dict_row
            with conn.cursor() as cursor:
                cursor.execute(id_fetch_query, id_fetch_params)
                paginated_camis_tuples = cursor.fetchall()
            if not paginated_camis_tuples:
                return jsonify([])
            paginated_camis = [item['camis'] for item in paginated_camis_tuples]
            details_query = "SELECT r.*, v.violation_code, v.violation_description FROM restaurants r LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date WHERE r.camis = ANY(%s)"
            with conn.cursor() as details_cursor:
                details_cursor.execute(details_query, (paginated_camis,))
                all_rows = details_cursor.fetchall()
    except Exception as e:
        logger.error(f"DB search failed for '{search_term}': {e}", exc_info=True)
        return jsonify({"error": "Database query failed"}), 500

    final_results = _group_and_shape_results(all_rows, paginated_camis)
    return jsonify(final_results)

@app.route('/restaurant/<string:camis>', methods=['GET'])
def get_restaurant_by_camis(camis):
    """
    Fetches the full details and entire inspection history for a single
    restaurant identified by its CAMIS.
    """
    if not camis.isdigit():
        return jsonify({"error": "Invalid CAMIS format"}), 400

    logger.info(f"Request received for full details of CAMIS: {camis}")
    
    try:
        with DatabaseConnection() as conn:
            conn.row_factory = dict_row
            details_query = """
                SELECT r.*, v.violation_code, v.violation_description 
                FROM restaurants r 
                LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date 
                WHERE r.camis = %s
            """
            with conn.cursor() as details_cursor:
                details_cursor.execute(details_query, (camis,))
                all_rows = details_cursor.fetchall()

            if not all_rows:
                return jsonify({"error": "Restaurant not found"}), 404

    except Exception as e:
        logger.error(f"DB query failed for CAMIS '{camis}': {e}", exc_info=True)
        return jsonify({"error": "Database query failed"}), 500

    # We can reuse your existing helper function to shape the data perfectly.
    final_results = _group_and_shape_results(all_rows, [camis])
    if not final_results:
        return jsonify({"error": "Failed to shape restaurant data"}), 500

    return jsonify(final_results[0]) # Return the single restaurant object, not a list

@app.route('/lists/recently-graded', methods=['GET'])
def get_recently_graded():
    """
    Gets a list of unique restaurants based on their single most recent inspection.
    This final version fetches data efficiently AND shapes it into the nested format
    the iOS client expects ([Restaurant] with a nested [Inspection] array).
    """
    logger.info("Request received for /lists/recently-graded")
    limit = int(request.args.get('limit', 100, type=int))
    
    # This efficient query remains the same.
    query = """
        WITH latest_inspections_per_restaurant AS (
            SELECT
                r.*,
                ROW_NUMBER() OVER(PARTITION BY r.camis ORDER BY r.inspection_date DESC) as rn
            FROM
                restaurants r
        )
        SELECT *
        FROM
            latest_inspections_per_restaurant
        WHERE
            rn = 1 AND grade_date IS NOT NULL
        ORDER BY
            grade_date DESC, dba ASC
        LIMIT %s;
    """
    
    try:
        with DatabaseConnection() as conn:
            conn.row_factory = dict_row
            with conn.cursor() as cursor:
                cursor.execute(query, (limit,))
                results = cursor.fetchall()
            
            if not results:
                return jsonify([])
            shaped_results = []
            for row in results:
                inspection_data = {
                    'inspection_date': row.get('inspection_date').isoformat() if row.get('inspection_date') else None,
                    'critical_flag': row.get('critical_flag'),
                    'grade': row.get('grade'),
                    'inspection_type': row.get('inspection_type'),
                    'action': row.get('action'),
                    'violations': [] # Violations aren't needed for this view, so an empty array is fine.
                }

                # 2. Create the main restaurant object, copying all top-level fields
                restaurant_data = dict(row)

                # 3. Nest the inspection data
                restaurant_data['inspections'] = [inspection_data]

                # 4. Clean up by removing the keys that are now nested
                inspection_keys_to_remove = ['critical_flag', 'grade', 'inspection_type', 'action', 'violation_code', 'violation_description']
                for key in inspection_keys_to_remove:
                    if key in restaurant_data:
                        del restaurant_data[key]
                
                shaped_results.append(restaurant_data)

            return jsonify(shaped_results)
            
    except Exception as e:
        logger.error(f"DB query failed for recently-graded list: {e}", exc_info=True)
        return jsonify({"error": "Database query failed"}), 500

@app.route('/report-issue', methods=['POST'])
def report_issue():
    if not request.is_json:
        return jsonify({"error": "Request must be JSON"}), 400
    data = request.get_json()
    if not all(k in data for k in ["camis", "issue_type", "comments"]):
        return jsonify({"error": "Missing required fields"}), 400
    logger.info(f"Received issue report: {data}")
    if send_report_email(data):
        return jsonify({"status": "success", "message": "Report received."}), 200
    else:
        return jsonify({"status": "error", "message": "Failed to process report."}), 500

@app.route('/trigger-update', methods=['POST'])
def trigger_update():
    try:
        from update_database import run_database_update
    except ImportError:
        return jsonify({"status": "error", "message": "Update logic currently unavailable."}), 503
    provided_key = request.headers.get('X-Update-Secret')
    expected_key = APIConfig.UPDATE_SECRET_KEY
    if not expected_key or not provided_key or not secrets.compare_digest(provided_key, expected_key):
        return jsonify({"status": "error", "message": "Unauthorized."}), 403
    threading.Thread(target=run_database_update, args=(15,), daemon=True).start()
    return jsonify({"status": "success", "message": "Database update triggered in background."}), 202

@app.errorhandler(404)
def not_found_error_handler(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_server_error_handler(error):
    logger.error(f"Internal Server Error (500): {error}", exc_info=True)
    return jsonify({"error": "An internal server error occurred"}), 500

if __name__ == "__main__":
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", 8080))
    app.run(host=host, port=port)
