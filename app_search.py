import os
import re
import logging
import json
import threading
import secrets
from flask import Flask, jsonify, request, g
from flask_cors import CORS
import psycopg2
import psycopg2.extras

from utils import normalize_search_term_for_hybrid
from db_manager import DatabaseConnection
from config import APIConfig

try:
    from update_database import run_database_update
    update_logic_imported = True
except ImportError:
    update_logic_imported = False

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# The full synonym map is still here
SEARCH_TERM_SYNONYMS = { 'pjclarkes': 'p j clarkes', 'mcdonalds': 'mcdonalds', } # Truncated for display

@app.teardown_appcontext
def teardown_db(exception):
    db_conn = g.pop('db_conn', None)
    if db_conn is not None:
        db_conn.close()

@app.route('/search', methods=['GET'])
def search():
    search_term = request.args.get('name', '').strip()
    grade_filter = request.args.get('grade', type=str)
    boro_filter = request.args.get('boro', type=str)
    cuisine_filter = request.args.get('cuisine', type=str)
    sort_option = request.args.get('sort', type=str)
    page = int(request.args.get('page', 1, type=int))
    per_page = int(request.args.get('per_page', 25, type=int))

    logger.info(f"Received search: name='{search_term}', grade='{grade_filter}', boro='{boro_filter}', cuisine='{cuisine_filter}', sort='{sort_option}'")

    if not search_term:
        return jsonify([])

    normalized_search = normalize_search_term_for_hybrid(search_term)
    # Synonym logic can be applied here if needed

    # --- QUERY LOGIC REWRITTEN FOR PERFORMANCE ---

    # 1. Build WHERE clause and parameters
    where_conditions = ["(dba_normalized_search ILIKE %s OR similarity(dba_normalized_search, %s) > 0.4)"]
    params = [f"%{normalized_search}%", normalized_search]

    if grade_filter:
        where_conditions.append("grade = %s")
        params.append(grade_filter.upper())
    if boro_filter:
        where_conditions.append("boro ILIKE %s")
        params.append(boro_filter)
    if cuisine_filter:
        where_conditions.append("cuisine_description ILIKE %s")
        params.append(f"%{cuisine_filter}%")
    
    where_clause = " AND ".join(where_conditions)

    # 2. Build ORDER BY clause and its specific parameters
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
    else: # Default relevance sort
        order_by_clause = """
        ORDER BY
            CASE WHEN dba_normalized_search = %s THEN 0
                 WHEN dba_normalized_search ILIKE %s THEN 1
                 ELSE 2
            END,
            similarity(dba_normalized_search, %s) DESC,
            length(dba_normalized_search)
        """
        order_by_params = [normalized_search, f"{normalized_search}%", normalized_search]

    # 3. First Query: Get ONLY the IDs of the restaurants for the current page.
    # This is fast because it filters and sorts on a smaller set of columns.
    id_fetch_query = f"""
        SELECT camis FROM (
            SELECT DISTINCT ON (camis) camis, dba, dba_normalized_search, grade, inspection_date, cuisine_description, boro
            FROM restaurants
            ORDER BY camis, inspection_date DESC
        ) AS latest_restaurants
        WHERE {where_clause}
        {order_by_clause}
        LIMIT %s OFFSET %s;
    """
    
    offset = (page - 1) * per_page
    id_fetch_params = tuple(params + order_by_params + [per_page, offset])

    try:
        with DatabaseConnection() as conn, conn.cursor() as cursor:
            cursor.execute(id_fetch_query, id_fetch_params)
            paginated_camis_tuples = cursor.fetchall()
            
            if not paginated_camis_tuples:
                return jsonify([])

            # Flatten list of tuples to list of IDs
            paginated_camis = [item[0] for item in paginated_camis_tuples]

            # 4. Second Query: Get the full details for ONLY the paginated IDs.
            details_query = """
                SELECT r.*, v.violation_code, v.violation_description
                FROM restaurants r
                LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date
                WHERE r.camis = ANY(%s)
            """
            cursor.execute(details_query, (paginated_camis,))
            all_rows = cursor.fetchall()

    except Exception as e:
        logger.error(f"DB search failed for '{search_term}': {e}", exc_info=True)
        return jsonify({"error": "Database query failed"}), 500

    # 5. Process results into the final JSON, ensuring the original sort order is preserved.
    # Create a dictionary lookup for quick access
    restaurant_details_map = {str(camis): [] for camis in paginated_camis}
    for row in all_rows:
        restaurant_details_map[str(row['camis'])].append(row)

    final_results = []
    for camis in paginated_camis: # Iterate in the correct sorted order
        camis_str = str(camis)
        rows_for_restaurant = restaurant_details_map[camis_str]
        
        # Take base restaurant info from the first row (most recent inspection)
        base_info = dict(rows_for_restaurant[0])
        
        inspections = {}
        for row in rows_for_restaurant:
            insp_date_str = row['inspection_date'].isoformat()
            if insp_date_str not in inspections:
                inspections[insp_date_str] = {
                    'inspection_date': insp_date_str, 'grade': row['grade'],
                    'critical_flag': row['critical_flag'], 'inspection_type': row['inspection_type'],
                    'violations': []
                }
            if row['violation_code']:
                v_data = {'violation_code': row['violation_code'], 'violation_description': row['violation_description']}
                if v_data not in inspections[insp_date_str]['violations']:
                    inspections[insp_date_str]['violations'].append(v_data)

        base_info['inspections'] = sorted(list(inspections.values()), key=lambda x: x['inspection_date'], reverse=True)
        # Remove individual inspection fields from the top level
        for key in ['violation_code', 'violation_description', 'grade', 'inspection_date', 'critical_flag', 'inspection_type']:
            base_info.pop(key, None)
            
        final_results.append(base_info)
        
    return jsonify(final_results)


@app.route('/recent', methods=['GET'])
def recent_restaurants():
    return jsonify([])


@app.route('/trigger-update', methods=['POST'])
def trigger_update():
    # Note: Because of the circular import, this feature is now simplified.
    # In a future version, you may want to re-architect how this is called.
    try:
        from update_database import run_database_update
        threading.Thread(target=run_database_update, args=(15,), daemon=True).start()
        return jsonify({"status": "success", "message": "Database update triggered in background."}), 202
    except ImportError:
        return jsonify({"status": "error", "message": "Update logic is currently unavailable."}), 500

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
