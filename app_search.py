# app_search.py - The Final, Simplified, and Working Version

import os
import re
import logging
import json
import threading
import secrets
from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import psycopg2.extras

# Local application imports
from db_manager import DatabaseConnection, get_redis_client
from config import APIConfig
from update_database import run_database_update

# --- Basic Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)
app = Flask(__name__)
CORS(app)

# --- Normalization Function ---
def normalize_search_term(text):
    if not isinstance(text, str): return ''
    # Using unaccent extension in the database is more efficient,
    # but this function can still be used for pre-processing if needed.
    normalized_text = text.lower()
    normalized_text = re.sub(r"[']", "", normalized_text)
    normalized_text = re.sub(r"[./-]", " ", normalized_text)
    return normalized_text.strip()

# --- ##### THE FINAL WORKING SEARCH ENDPOINT ##### ---
@app.route('/search', methods=['GET'])
def search():
    # 1. Get and Validate Parameters
    search_term = request.args.get('name', '').strip()
    grade_filter = request.args.get('grade', type=str)
    boro_filter = request.args.get('boro', type=str)
    sort_by = request.args.get('sort', 'relevance', type=str)
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 25))
    except (ValueError, TypeError):
        page, per_page = 1, 25

    if not search_term:
        return jsonify([])

    normalized_search = normalize_search_term(search_term)

    # 2. Build Query Parameters and Clauses
    params = []
    where_conditions = []

    # Search term filtering
    where_conditions.append("(unaccent(lr.dba) ILIKE %s OR similarity(unaccent(lr.dba), %s) > 0.4)")
    params.extend([f"%{normalized_search}%", normalized_search])

    # Optional filters
    if grade_filter:
        where_conditions.append("lr.grade = %s")
        params.append(grade_filter)
    if boro_filter:
        where_conditions.append("lr.boro = %s")
        params.append(boro_filter)
    
    where_clause = " AND ".join(where_conditions)

    # Sorting logic
    order_by_clause = ""
    if sort_by == 'name_asc':
        order_by_clause = "ORDER BY lr.dba ASC"
    elif sort_by == 'name_desc':
        order_by_clause = "ORDER BY lr.dba DESC"
    else: # Default to relevance
        order_by_clause = "ORDER BY similarity(unaccent(lr.dba), %s) DESC"
        params.append(normalized_search)

    # Pagination logic
    offset = (page - 1) * per_page
    params.extend([per_page, offset])
    
    # 3. Construct the Final, Simplified Query
    query = f"""
        WITH latest_restaurants AS (
            SELECT DISTINCT ON (camis) *
            FROM restaurants
            ORDER BY camis, inspection_date DESC
        ),
        paginated_camis AS (
            SELECT lr.camis
            FROM latest_restaurants lr
            WHERE {where_clause}
            {order_by_clause}
            LIMIT %s OFFSET %s
        )
        SELECT 
            r.*, 
            v.violation_code, 
            v.violation_description
        FROM paginated_camis pc
        JOIN restaurants r ON pc.camis = r.camis
        LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date
        ORDER BY 
            (SELECT find_order.dba FROM latest_restaurants find_order WHERE find_order.camis = pc.camis),
            r.inspection_date DESC;
    """
    
    # Modify the ORDER BY clause for different sort options
    if sort_by == 'name_desc':
        query = query.replace("find_order.dba", "find_order.dba DESC")
    elif sort_by == 'relevance':
        query = query.replace("find_order.dba", "similarity(unaccent(find_order.dba), %s) DESC")
        params.append(normalized_search)

    # 4. Execute and Process
    try:
        with DatabaseConnection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(query, tuple(params))
            results = cursor.fetchall()
    except Exception as e:
        logger.error(f"DB search failed for term '{search_term}': {e}", exc_info=True)
        return jsonify({"error": "Database query failed"}), 500

    if not results:
        return jsonify([])

    # 5. Format results into nested JSON
    restaurant_data = {}
    for row in results:
        camis = row['camis']
        if camis not in restaurant_data:
            restaurant_data[camis] = {k: v for k, v in row.items() if k not in ['violation_code', 'violation_description']}
            restaurant_data[camis]['inspections'] = {}

        insp_date_str = row['inspection_date'].isoformat()
        if insp_date_str not in restaurant_data[camis]['inspections']:
            restaurant_data[camis]['inspections'][insp_date_str] = {
                'inspection_date': insp_date_str, 'grade': row['grade'],
                'critical_flag': row['critical_flag'], 'inspection_type': row['inspection_type'],
                'violations': []
            }
        
        if row['violation_code'] and {'violation_code': row['violation_code'], 'violation_description': row['violation_description']} not in restaurant_data[camis]['inspections'][insp_date_str]['violations']:
            restaurant_data[camis]['inspections'][insp_date_str]['violations'].append({'violation_code': row['violation_code'], 'violation_description': row['violation_description']})

    final_results = [{**data, 'inspections': sorted(list(data['inspections'].values()), key=lambda x: x['inspection_date'], reverse=True)} for data in restaurant_data.values()]
    
    return jsonify(final_results)


# --- Other Endpoints (Unchanged) ---
@app.route('/recent', methods=['GET'])
def recent_restaurants():
    return jsonify([])

@app.route('/trigger-update', methods=['POST'])
def trigger_update():
    if not update_logic_imported:
        return jsonify({"status": "error", "message": "Update logic unavailable."}), 500
    provided_key = request.headers.get('X-Update-Secret')
    expected_key = APIConfig.UPDATE_SECRET_KEY
    if not expected_key or not provided_key or not secrets.compare_digest(provided_key, expected_key):
        return jsonify({"status": "error", "message": "Unauthorized."}), 403
    threading.Thread(target=run_database_update, args=(5,), daemon=True).start()
    return jsonify({"status": "success", "message": "Database update triggered in background."}), 202

# --- Error Handlers (Unchanged) ---
@app.errorhandler(404)
def not_found_error_handler(error):
    return jsonify({"error": "Not found"}), 404

@app.errorhandler(500)
def internal_server_error_handler(error):
    logger.error(f"500 Error: {error}", exc_info=True)
    return jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", 8080))
    app.run(host=host, port=port)
