# app_search.py - Complete file with Filter and Sort Capabilities

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
    from config import APIConfig, SentryConfig
    from update_database import run_database_update
    update_logic_imported = True
except ImportError:
    class DatabaseConnection:
        def __enter__(self): return None
        def __exit__(self, t, v, tb): pass
    def get_redis_client(): return None
    class APIConfig: DEBUG = False; UPDATE_SECRET_KEY = "dummy"; HOST = "0.0.0.0"; PORT = 8080
    class SentryConfig: SENTRY_DSN = None
    update_logic_imported = False
    def run_database_update(days_back=5): pass

# --- Basic Setup (Logging, Sentry, Flask App) ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)
# Sentry, etc. can be added here if needed.

SEARCH_TERM_SYNONYMS = {'pjclarkes': 'p j clarkes', 'xian': 'xi an'} # Add your other synonyms here
app = Flask(__name__)
CORS(app)

# --- Normalization Function (Unchanged) ---
def normalize_search_term_for_hybrid(text):
    if not isinstance(text, str): return ''
    normalized_text = text.lower().replace('&', ' and ')
    accent_map = { 'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e', 'á': 'a', 'à': 'a', 'â': 'a', 'ä': 'a', 'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i', 'ó': 'o', 'ò': 'o', 'ô': 'o', 'ö': 'o', 'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u', 'ç': 'c', 'ñ': 'n' }
    for accented, unaccented in accent_map.items():
        normalized_text = normalized_text.replace(accented, unaccented)
    normalized_text = re.sub(r"[']", "", normalized_text)
    normalized_text = re.sub(r"[./-]", " ", normalized_text)
    normalized_text = re.sub(r"[^a-z0-9\s]", "", normalized_text)
    normalized_text = re.sub(r"\s+", " ", normalized_text)
    return normalized_text.strip()

# --- API Routes ---

@app.route('/', methods=['GET'])
def root():
    return jsonify({"status": "ok", "message": "API is running"})

@app.route('/search', methods=['GET'])
def search():
    # Get Search and Pagination Parameters
    search_term = request.args.get('name', '').strip()
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 25))
    except (ValueError, TypeError):
        page, per_page = 1, 25
    
    # Get NEW Filter and Sort Parameters
    grade_filter = request.args.get('grade', None)
    boro_filter = request.args.get('boro', None)
    sort_by = request.args.get('sort', 'relevance')

    if not search_term: return jsonify([])

    # Normalization and Cache Key
    normalized_search = normalize_search_term_for_hybrid(search_term)
    if normalized_search in SEARCH_TERM_SYNONYMS:
        normalized_search = SEARCH_TERM_SYNONYMS[normalized_search]
    if not normalized_search: return jsonify([])

    cache_key = f"search_v4:{normalized_search}:g{grade_filter}:b{boro_filter}:s{sort_by}:p{page}:pp{per_page}"
    redis_conn = get_redis_client()
    if redis_conn:
        try:
            cached_result = redis_conn.get(cache_key)
            if cached_result: return jsonify(json.loads(cached_result))
        except Exception as e:
            logger.error(f"Redis GET error: {e}")

    # Build Dynamic SQL Query
    params = []
    where_clauses = ["(dba_normalized_search ILIKE %s OR similarity(dba_normalized_search, %s) > 0.4)"]
    params.extend([f"%{normalized_search}%", normalized_search])

    if grade_filter and grade_filter in ['A', 'B', 'C']:
        where_clauses.append("r_full.grade = %s") # Filter on the joined table
        params.append(grade_filter)

    if boro_filter:
        where_clauses.append("r_full.boro = %s")
        params.append(boro_filter.upper())

    where_string = " AND ".join(where_clauses)
    
    # Determine ORDER BY clause
    order_by_clause = ""
    if sort_by == 'name_asc':
        order_by_clause = "dba ASC"
    elif sort_by == 'name_desc':
        order_by_clause = "dba DESC"
    else: # Default 'relevance' sort
        order_by_clause = """
            CASE WHEN dba_normalized_search = %s THEN 0 WHEN dba_normalized_search ILIKE %s THEN 1 ELSE 2 END,
            similarity(dba_normalized_search, %s) DESC, length(dba_normalized_search), dba ASC
        """
        params.extend([normalized_search, f"{normalized_search}%", normalized_search])

    limit = per_page
    offset = (page - 1) * per_page
    
    # Construct the Full Query
    full_query = f"""
    WITH PaginatedRestaurants AS (
        SELECT camis
        FROM restaurants
        WHERE (dba_normalized_search ILIKE %s OR similarity(dba_normalized_search, %s) > 0.4)
        GROUP BY camis, dba, dba_normalized_search
        ORDER BY {order_by_clause}
        LIMIT %s OFFSET %s
    )
    SELECT
        pr.camis, r_full.dba, r_full.boro, r_full.building, r_full.street, r_full.zipcode, r_full.phone,
        r_full.latitude, r_full.longitude, r_full.inspection_date, r_full.critical_flag, r_full.grade,
        r_full.inspection_type, v.violation_code, v.violation_description, r_full.cuisine_description
    FROM PaginatedRestaurants pr
    JOIN restaurants r_full ON pr.camis = r_full.camis
    LEFT JOIN violations v ON r_full.camis = v.camis AND r_full.inspection_date = v.inspection_date
    WHERE {where_string}
    ORDER BY {order_by_clause}, r_full.inspection_date DESC;
    """
    
    # Adjust params for the new query structure
    relevance_params = []
    if sort_by == 'relevance':
        relevance_params = [normalized_search, f"{normalized_search}%", normalized_search]

    final_params = [f"%{normalized_search}%", normalized_search] + relevance_params + [limit, offset] + [f"%{normalized_search}%", normalized_search]
    if grade_filter and grade_filter in ['A', 'B', 'C']:
        final_params.append(grade_filter)
    if boro_filter:
        final_params.append(boro_filter.upper())
    final_params.extend(relevance_params)

    # Execute Query and Process Results
    try:
        with DatabaseConnection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(full_query, tuple(final_params))
            db_results_raw = cursor.fetchall()
    except Exception as e:
        logger.error(f"DB error for search '{normalized_search}': {e}", exc_info=True)
        return jsonify({"error": "Database query failed"}), 500
    
    if not db_results_raw: return jsonify([])

    restaurant_dict = {}
    for row in db_results_raw:
        camis = row['camis']
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
    logger.info("Received request for /recent")
    # ... (Your existing logic for this endpoint)
    return jsonify([]) # Placeholder

@app.route('/trigger-update', methods=['POST'])
def trigger_update():
    logger.info("Received request for /trigger-update")
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
    debug_mode = "true" in os.environ.get("DEBUG", "").lower()
    app.run(host=host, port=port, debug=debug_mode)
