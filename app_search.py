# app_search.py - v6 - Final Corrected and Simplified Query

# Standard library imports
import os
import re
import logging
import json
import threading
import secrets
import sys

# Third-party imports
from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import psycopg2.extras

# Local application imports
try:
    from db_manager import DatabaseConnection, get_redis_client
    from config import APIConfig
    from update_database import run_database_update
    update_logic_imported = True
except ImportError:
    class DatabaseConnection:
        def __enter__(self): return None
        def __exit__(self, t, v, tb): pass
    def get_redis_client(): return None
    class APIConfig: UPDATE_SECRET_KEY = "dummy"; HOST="0.0.0.0"; PORT=8080
    update_logic_imported = False
    def run_database_update(days_back=5): pass

# --- Basic Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)
SEARCH_TERM_SYNONYMS = {'pjclarkes': 'p j clarkes', 'xian': 'xi an'}
app = Flask(__name__)
CORS(app)

# --- Normalization Function (Unchanged) ---
def normalize_search_term_for_hybrid(text):
    if not isinstance(text, str): return ''
    normalized_text = text.lower().replace('&', ' and ')
    accent_map = { 'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e', 'á': 'a', 'à': 'a', 'â': 'a', 'ä': 'a', 'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i', 'ó': 'o', 'ò': 'o', 'ô': 'o', 'ö':'o', 'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u', 'ç': 'c', 'ñ': 'n' }
    for accented, unaccented in accent_map.items():
        normalized_text = normalized_text.replace(accented, unaccented)
    normalized_text = re.sub(r"[']", "", normalized_text)
    normalized_text = re.sub(r"[./-]", " ", normalized_text)
    normalized_text = re.sub(r"[^a-z0-9\s]", "", normalized_text)
    normalized_text = re.sub(r"\s+", " ", normalized_text)
    return normalized_text.strip()


# --- ##### REWRITTEN AND CORRECTED SEARCH ENDPOINT ##### ---
@app.route('/search', methods=['GET'])
def search():
    # 1. Get All Parameters
    search_term = request.args.get('name', '').strip()
    grade_filter = request.args.get('grade', None)
    boro_filter = request.args.get('boro', None)
    sort_by = request.args.get('sort', 'relevance')
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 25))
    except (ValueError, TypeError):
        page, per_page = 1, 25

    if not search_term: return jsonify([])

    # 2. Normalize and Prepare Search Terms
    normalized_search = normalize_search_term_for_hybrid(search_term)
    if normalized_search in SEARCH_TERM_SYNONYMS:
        normalized_search = SEARCH_TERM_SYNONYMS[normalized_search]
    if not normalized_search: return jsonify([])

    # 3. Build Cache Key
    cache_key = f"search_v8_final:{normalized_search}:g{grade_filter}:b{boro_filter}:s{sort_by}:p{page}:pp{per_page}"
    redis_conn = get_redis_client()
    if redis_conn:
        try:
            cached_result = redis_conn.get(cache_key)
            if cached_result: return jsonify(json.loads(cached_result))
        except Exception as e:
            logger.error(f"Redis GET error: {e}")

    # 4. Dynamically Build the SQL Query and Parameters
    
    # This simplified query first finds the unique restaurant IDs that match all criteria,
    # then gets all data for that paginated list of restaurants.
    
    query = """
        WITH unique_restaurants AS (
            SELECT DISTINCT ON (camis)
                camis,
                dba,
                dba_normalized_search,
                grade,
                boro
            FROM restaurants
            ORDER BY camis, inspection_date DESC
        ),
        paginated_camis AS (
            SELECT camis
            FROM unique_restaurants
            WHERE
                (dba_normalized_search ILIKE %s OR similarity(dba_normalized_search, %s) > 0.4)
                AND (%s IS NULL OR grade = %s)
                AND (%s IS NULL OR boro = %s)
            ORDER BY
                CASE WHEN %s = 'name_asc' THEN dba END ASC,
                CASE WHEN %s = 'name_desc' THEN dba END DESC,
                CASE WHEN %s = 'relevance' THEN
                    (CASE WHEN dba_normalized_search = %s THEN 0
                          WHEN dba_normalized_search ILIKE %s THEN 1
                          ELSE 2 END)
                END,
                CASE WHEN %s = 'relevance' THEN similarity(dba_normalized_search, %s) END DESC
            LIMIT %s OFFSET %s
        )
        SELECT
            pc.camis, r.dba, r.boro, r.building, r.street, r.zipcode, r.phone,
            r.latitude, r.longitude, r.inspection_date, r.critical_flag, r.grade,
            r.inspection_type, v.violation_code, v.violation_description, r.cuisine_description
        FROM paginated_camis pc
        JOIN restaurants r ON pc.camis = r.camis
        LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date
        ORDER BY
            (SELECT 
                CASE WHEN %s = 'name_asc' THEN r.dba END ASC,
                CASE WHEN %s = 'name_desc' THEN r.dba END DESC,
                CASE WHEN %s = 'relevance' THEN
                    (CASE WHEN r.dba_normalized_search = %s THEN 0
                          WHEN r.dba_normalized_search ILIKE %s THEN 1
                          ELSE 2 END)
                END,
                CASE WHEN %s = 'relevance' THEN similarity(r.dba_normalized_search, %s) END DESC
            ),
            r.inspection_date DESC;
    """
    
    # 5. Assemble the parameters in the correct order for the query
    params = (
        f"%{normalized_search}%", normalized_search,
        grade_filter, grade_filter,
        boro_filter, boro_filter,
        sort_by,
        sort_by,
        sort_by, normalized_search, f"{normalized_search}%",
        sort_by, normalized_search,
        per_page, (page - 1) * per_page,
        sort_by, # Start of final order by
        sort_by,
        sort_by, normalized_search, f"{normalized_search}%",
        sort_by, normalized_search
    )
    
    # 6. Execute Query and Process Results
    try:
        with DatabaseConnection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(query, params)
            db_results_raw = cursor.fetchall()
    except Exception as e:
        logger.error(f"DB error for search '{normalized_search}': {e}", exc_info=True)
        return jsonify({"error": "Database query failed"}), 500
    
    if not db_results_raw: return jsonify([])

    # Group results by restaurant and inspection (Unchanged)
    restaurant_dict = {}
    for row in db_results_raw:
        camis = row['camis']
        if not camis: continue
        if camis not in restaurant_dict:
            restaurant_dict[camis] = {k: v for k, v in row.items() if k not in ['violation_code', 'violation_description', 'inspection_date', 'critical_flag', 'grade', 'inspection_type']}
            restaurant_dict[camis]['inspections'] = {}
        inspection_date_str = row['inspection_date'].isoformat()
        if inspection_date_str not in restaurant_dict[camis]['inspections']:
            restaurant_dict[camis]['inspections'][inspection_date_str] = {'inspection_date': inspection_date_str, 'critical_flag': row['critical_flag'], 'grade': row['grade'], 'inspection_type': row['inspection_type'], 'violations': []}
        if row['violation_code']:
            violation = {'violation_code': row['violation_code'], 'violation_description': row['violation_description']}
            if violation not in restaurant_dict[camis]['inspections'][inspection_date_str]['violations']:
                restaurant_dict[camis]['inspections'][inspection_date_str]['violations'].append(violation)
    
    formatted_results = [dict(data, inspections=list(data['inspections'].values())) for data in restaurant_dict.values()]
    
    if redis_conn:
        try: redis_conn.setex(cache_key, 3600, json.dumps(formatted_results, default=str))
        except Exception as e: logger.error(f"Redis SETEX error: {e}")
            
    return jsonify(formatted_results)


# --- Other Endpoints ---

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

# --- Error Handlers ---
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
