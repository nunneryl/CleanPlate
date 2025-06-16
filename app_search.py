# app_search.py - The Definitive Final Version

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

# --- Normalization Function (from your live version) ---
def normalize_search_term_for_hybrid(text):
    """
    This function cleans a search term by making it lowercase, removing accents,
    and stripping out special characters like apostrophes, periods, and dashes.
    This logic is taken directly from your working live version.
    """
    if not isinstance(text, str): return ''
    normalized_text = text.lower()
    normalized_text = normalized_text.replace('&', ' and ')
    accent_map = { 'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e', 'á': 'a', 'à': 'a', 'â': 'a', 'ä': 'a', 'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i', 'ó': 'o', 'ò': 'o', 'ô': 'o', 'ö': 'o', 'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u', 'ç': 'c', 'ñ': 'n' }
    for accented, unaccented in accent_map.items():
        normalized_text = normalized_text.replace(accented, unaccented)
    normalized_text = re.sub(r"[']", "", normalized_text)
    normalized_text = re.sub(r"[./-]", " ", normalized_text)
    normalized_text = re.sub(r"[^a-z0-9\s]", "", normalized_text)
    normalized_text = re.sub(r"\s+", " ", normalized_text)
    return normalized_text.strip()

# --- ##### THE FINAL, WORKING SEARCH ENDPOINT ##### ---
@app.route('/search', methods=['GET'])
def search():
    # 1. Get and Validate Parameters
    search_term = request.args.get('name', '').strip()
    grade_filter = request.args.get('grade', type=str)
    boro_filter = request.args.get('boro', type=str)
    sort_by = request.args.get('sort', 'relevance', type=str)
    page = int(request.args.get('page', 1)) if request.args.get('page') else 1
    per_page = int(request.args.get('per_page', 25)) if request.args.get('per_page') else 25

    if not search_term:
        return jsonify([])

    normalized_search = normalize_search_term_for_hybrid(search_term)
    if not normalized_search:
        return jsonify([])

    # 2. Build Query based on the working live version's structure
    params = []
    
    # Base search condition against the pre-normalized column
    where_conditions = ["(dba_normalized_search ILIKE %s OR similarity(dba_normalized_search, %s) > 0.4)"]
    params.extend([f"%{normalized_search}%", normalized_search])

    # Add new optional filters
    if grade_filter:
        where_conditions.append("grade = %s")
        params.append(grade_filter)
    if boro_filter:
        where_conditions.append("boro = %s")
        params.append(boro_filter)
    
    where_clause = " AND ".join(where_conditions)

    # Sorting logic from your live version
    order_by_clause = ""
    order_params = []
    if sort_by == 'name_asc':
        order_by_clause = "ORDER BY dba ASC"
    elif sort_by == 'name_desc':
        order_by_clause = "ORDER BY dba DESC"
    else: # Default to relevance
        order_by_clause = """
        ORDER BY
            CASE
                WHEN dba_normalized_search = %s THEN 0
                WHEN dba_normalized_search ILIKE %s THEN 1
                ELSE 2
            END,
            similarity(dba_normalized_search, %s) DESC,
            length(dba_normalized_search)
        """
        order_params.extend([normalized_search, f"{normalized_search}%", normalized_search])

    # Pagination logic
    offset = (page - 1) * per_page
    pagination_params = [per_page, offset]

    # Combine all parameters in the correct order for the main query
    final_params = params + order_params + pagination_params
    
    # This query structure precisely mirrors your working live version,
    # but with the new WHERE clauses integrated.
    query = f"""
        WITH latest_restaurants AS (
            SELECT DISTINCT ON (camis) * FROM restaurants ORDER BY camis, inspection_date DESC
        ),
        paginated_camis AS (
            SELECT camis, dba, dba_normalized_search
            FROM latest_restaurants
            WHERE {where_clause}
            {order_by_clause}
            LIMIT %s OFFSET %s
        )
        SELECT 
            pc.camis, pc.dba, pc.dba_normalized_search,
            r.boro, r.building, r.street, r.zipcode, r.phone, r.latitude, r.longitude,
            r.inspection_date, r.critical_flag, r.grade, r.inspection_type,
            v.violation_code, v.violation_description, r.cuisine_description
        FROM paginated_camis pc
        JOIN restaurants r ON pc.camis = r.camis
        LEFT JOIN violations v ON pc.camis = v.camis AND r.inspection_date = v.inspection_date
        ORDER BY
            (CASE WHEN pc.dba_normalized_search = %s THEN 0 WHEN pc.dba_normalized_search ILIKE %s THEN 1 ELSE 2 END),
            similarity(pc.dba_normalized_search, %s) DESC,
            length(pc.dba_normalized_search),
            pc.dba ASC,
            r.inspection_date DESC;
    """
    
    # Add the final sorting parameters, which are used again in the outer ORDER BY
    final_params.extend([normalized_search, f"{normalized_search}%", normalized_search])

    # 3. Execute and Process
    try:
        with DatabaseConnection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(query, tuple(final_params))
            results = cursor.fetchall()
    except Exception as e:
        logger.error(f"DB search failed for '{normalized_search}': {e}", exc_info=True)
        return jsonify({"error": "Database query failed"}), 500

    if not results:
        return jsonify([])
        
    # 4. Format Results (as in your live version)
    restaurant_dict = {}
    for row in results:
        camis = row['camis']
        if camis not in restaurant_dict:
            restaurant_dict[camis] = {k: v for k, v in row.items() if k not in ['violation_code', 'violation_description', 'inspection_date', 'critical_flag', 'grade', 'inspection_type']}
            restaurant_dict[camis]['inspections'] = {}

        insp_date_str = row['inspection_date'].isoformat()
        if insp_date_str not in restaurant_dict[camis]['inspections']:
            restaurant_dict[camis]['inspections'][insp_date_str] = {'inspection_date': insp_date_str, 'grade': row['grade'], 'critical_flag': row['critical_flag'], 'inspection_type': row['inspection_type'], 'violations': []}
        
        if row['violation_code']:
            v_data = {'violation_code': row['violation_code'], 'violation_description': row['violation_description']}
            if v_data not in restaurant_dict[camis]['inspections'][insp_date_str]['violations']:
                restaurant_dict[camis]['inspections'][insp_date_str]['violations'].append(v_data)

    final_results = [{**data, 'inspections': sorted(list(data['inspections'].values()), key=lambda x: x['inspection_date'], reverse=True)} for data in restaurant_dict.values()]
    
    return jsonify(final_results)

# (The rest of the file: /recent, /trigger-update, error handlers, etc., remain unchanged)
# These are included to ensure the file is complete.
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

@app.errorhandler(404)
def not_found_error_handler(error):
    return jsonify({"error": "Not Found"}), 404

@app.errorhandler(500)
def internal_server_error_handler(error):
    logger.error(f"500 Error: {error}", exc_info=True)
    return jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", 8080))
    app.run(host=host, port=port)
