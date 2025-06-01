# app_search.py - With original /search and new /search_fts_test endpoint

# Standard library imports
import os
import re
import logging
import json
import threading
import secrets
import sys

# Third-party imports
import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration
from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import psycopg2.extras # For DictCursor
import redis

# Local application imports
try:
    from db_manager import DatabaseConnection, get_redis_client
    logging.info("Imported db_manager successfully.")
except ImportError:
    logging.critical("FAILED to import db_manager. Using placeholder classes.")
    class DatabaseConnection:
        def __enter__(self): return None
        def __exit__(self, type, value, traceback): pass
    def get_redis_client(): return None

try:
    from config import APIConfig, SentryConfig
    logging.info("Imported config successfully.")
except ImportError:
    logging.critical("FAILED to import config. Using placeholder classes.")
    class APIConfig: DEBUG = False; UPDATE_SECRET_KEY = "key"; HOST = "0.0.0.0"; PORT = 8080
    class SentryConfig: SENTRY_DSN = None

try:
    from update_database import run_database_update
    update_logic_imported = True
    logging.info("Imported run_database_update successfully.")
except ImportError:
    logging.error("FAILED to import run_database_update. /trigger-update will not work.")
    update_logic_imported = False
    def run_database_update(days_back=5):
         logging.error("DUMMY run_database_update called - real function failed to import.")

# --- Sentry Initialization ---
if SentryConfig.SENTRY_DSN:
    try:
        sentry_sdk.init(
            dsn=SentryConfig.SENTRY_DSN, integrations=[FlaskIntegration()],
            traces_sample_rate=1.0, environment="production" # Assuming production
        )
        logging.info("Sentry initialized.")
    except Exception as e:
         logging.error(f"Failed to initialize Sentry: {e}")
else:
    logging.warning("SENTRY_DSN not set, Sentry not initialized.")

# --- Logging Setup ---
if not logging.getLogger().hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)
logger.info("Logging configured.")

# --- Flask App Initialization ---
app = Flask(__name__)
CORS(app)
logger.info("Flask app created.")

# --- AGGRESSIVE NORMALIZATION FUNCTION (Python-side, for FTS input) ---
def normalize_text(text): # For user input to FTS query
    if not isinstance(text, str): return ''
    text = text.lower()
    text = text.replace("'", "").replace(".", "").replace('&', 'and')
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', '', text).strip()
    return text

# --- ORIGINAL SANITIZE INPUT (For the old /search endpoint) ---
def original_sanitize_input(input_str):
    if not input_str: return "", ""
    input_str = input_str.replace("’", "'").replace("‘", "'")
    no_periods_version = input_str.replace(".", "")
    sanitized_input = re.sub(r"[^\w\s']", "", input_str)
    no_periods_sanitized = re.sub(r"[^\w\s']", "", no_periods_version)
    return sanitized_input, no_periods_sanitized

# --- API Routes ---
@app.route('/', methods=['GET'])
def root():
    return jsonify({"status": "ok", "message": "API is running"})

# --- ORIGINAL /search ENDPOINT (for live app stability) ---
@app.route('/search', methods=['GET'])
def search_original(): # Renamed function to avoid conflict
    logger.info("Received request for ORIGINAL /search endpoint")
    name = request.args.get('name', '').strip()
    if not name:
        return jsonify({"error": "Search term is empty"}), 400

    normalized_name_for_key = name.replace("’", "'").replace("‘", "'").lower().strip()
    cache_key = f"search_orig:{normalized_name_for_key}" # Different cache prefix
    CACHE_TTL_SECONDS = 3600 * 4

    redis_conn = get_redis_client()
    if redis_conn:
        try:
            cached_result_str = redis_conn.get(cache_key)
            if cached_result_str:
                return jsonify(json.loads(cached_result_str))
        except Exception as e:
             logger.error(f"Original Search Redis GET error: {e}"); sentry_sdk.capture_exception(e)
    
    name_with_periods, name_without_periods = original_sanitize_input(name)
    if '.' not in name and len(name_without_periods) >= 2: name_with_added_periods = '.'.join(list(name_without_periods))
    else: name_with_added_periods = name_with_periods
    transformed_name = name_with_periods.replace("s", "'s"); transformed_name_no_periods = name_without_periods.replace("s", "'s"); transformed_with_added_periods = name_with_added_periods.replace("s", "'s")
    
    query = """
        SELECT r.camis, r.dba, r.boro, r.building, r.street, r.zipcode, r.phone, 
               r.latitude, r.longitude, r.inspection_date, r.critical_flag, r.grade, 
               r.inspection_type, v.violation_code, v.violation_description, r.cuisine_description 
        FROM restaurants r 
        LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date 
        WHERE r.dba ILIKE %s OR r.dba ILIKE %s OR r.dba ILIKE %s OR r.dba ILIKE %s OR r.dba ILIKE %s 
        ORDER BY 
            CASE WHEN UPPER(r.dba) = UPPER(%s) THEN 0 WHEN UPPER(r.dba) = UPPER(%s) THEN 1 
                 WHEN UPPER(r.dba) LIKE UPPER(%s) THEN 2 WHEN UPPER(r.dba) LIKE UPPER(%s) THEN 3 
                 WHEN UPPER(r.dba) LIKE UPPER(%s) THEN 4 ELSE 5 END, 
            r.dba, r.inspection_date DESC
    """
    where_params = [ f"%{p}%" for p in [name_with_periods, transformed_name, name_without_periods, transformed_name_no_periods, transformed_with_added_periods] ]
    order_params = [ name_with_periods, name_without_periods, f"{name_with_periods}%", f"{name_without_periods}%", f"{transformed_with_added_periods}%" ]
    params = where_params + order_params
    
    db_results_raw = None
    columns = []
    try:
        with DatabaseConnection() as conn, conn.cursor() as cursor:
            cursor.execute(query, params)
            db_results_raw = cursor.fetchall()
            if db_results_raw is not None:
                columns = [desc[0] for desc in cursor.description]
    except Exception as e:
        logger.error(f"Original Search DB error: {e}", exc_info=True); sentry_sdk.capture_exception(e)
        return jsonify({"error": "Database query failed"}), 500

    if not db_results_raw: return jsonify([])

    db_results = [dict(zip(columns, row)) for row in db_results_raw]
    restaurant_dict = {}
    # ... (Your original result processing logic from the reverted app_search.py)
    for row_dict in db_results:
        camis = row_dict.get('camis')
        if not camis: continue
        if camis not in restaurant_dict:
            restaurant_dict[camis] = {k: v for k, v in row_dict.items() if k not in ['violation_code', 'violation_description', 'inspection_date', 'critical_flag', 'grade', 'inspection_type']}
            restaurant_dict[camis]['inspections'] = {}
        inspection_date_obj = row_dict.get('inspection_date')
        if inspection_date_obj:
            inspection_date_str = inspection_date_obj.isoformat() if hasattr(inspection_date_obj, 'isoformat') else str(inspection_date_obj)
            if inspection_date_str not in restaurant_dict[camis]['inspections']:
                restaurant_dict[camis]['inspections'][inspection_date_str] = {'inspection_date': inspection_date_str, 'critical_flag': row_dict.get('critical_flag'), 'grade': row_dict.get('grade'), 'inspection_type': row_dict.get('inspection_type'), 'violations': []}
            if row_dict.get('violation_code'):
                violation = {'violation_code': row_dict.get('violation_code'), 'violation_description': row_dict.get('violation_description')}
                if violation not in restaurant_dict[camis]['inspections'][inspection_date_str]['violations']:
                    restaurant_dict[camis]['inspections'][inspection_date_str]['violations'].append(violation)
    formatted_results = [dict(data, inspections=list(data['inspections'].values())) for data in restaurant_dict.values()]

    if redis_conn:
        try: redis_conn.setex(cache_key, CACHE_TTL_SECONDS, json.dumps(formatted_results, default=str))
        except Exception as e: logger.error(f"Original Search Redis SETEX error: {e}"); sentry_sdk.capture_exception(e)
    return jsonify(formatted_results)

# --- NEW /search_fts_test ENDPOINT (for testing FTS) ---
@app.route('/search_fts_test', methods=['GET'])
def search_fts_test():
    logger.info("Received request for TEST /search_fts_test endpoint")
    search_term_from_user = request.args.get('name', '').strip()
    if not search_term_from_user:
        return jsonify({"error": "Search term is empty"}), 400

    normalized_user_input_for_query = normalize_text(search_term_from_user) # Aggressive normalize
    if not normalized_user_input_for_query:
        return jsonify([])

    cache_key = f"search_v5_agg_test:{normalized_user_input_for_query}" # Different cache key for testing
    CACHE_TTL_SECONDS = 3600 * 1 # Shorter cache for testing, e.g., 1 hour

    redis_conn = get_redis_client()
    if redis_conn:
        try:
            cached_result_str = redis_conn.get(cache_key)
            if cached_result_str:
                return jsonify(json.loads(cached_result_str))
        except Exception as e:
             logger.error(f"Test FTS Search Redis GET error: {e}"); sentry_sdk.capture_exception(e)

    query = """
        SELECT
            r.camis, r.dba, r.boro, r.building, r.street, r.zipcode, r.phone,
            r.latitude, r.longitude, r.inspection_date, r.critical_flag, r.grade,
            r.inspection_type, v.violation_code, v.violation_description, r.cuisine_description,
            ts_rank_cd(r.dba_tsv, websearch_to_tsquery('english', %s)) AS rank
        FROM restaurants r
        LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date
        WHERE r.dba_tsv @@ websearch_to_tsquery('english', %s)
        ORDER BY rank DESC, r.dba ASC, r.inspection_date DESC
        LIMIT 100;
    """
    params = (normalized_user_input_for_query, normalized_user_input_for_query)
    db_results_raw = None
    columns = [] # Initialize columns
    try:
        with DatabaseConnection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(query, params)
            db_results_raw = cursor.fetchall()
            if db_results_raw is not None: # Ensure columns is set only if results are fetched
                columns = [desc[0] for desc in cursor.description]
    except Exception as e:
        logger.error(f"Test FTS Search DB error: {e}", exc_info=True); sentry_sdk.capture_exception(e)
        return jsonify({"error": "Database query failed"}), 500

    if not db_results_raw: return jsonify([])
    
    db_results = [dict(row) for row in db_results_raw]
    restaurant_dict = {}
    # ... (Result processing logic - same as the FTS v3 search function I sent before)
    for row_dict in db_results:
        camis = row_dict.get('camis')
        if not camis: continue
        if camis not in restaurant_dict:
            restaurant_dict[camis] = {k: v for k, v in row_dict.items() if k not in ['rank', 'violation_code', 'violation_description', 'inspection_date', 'critical_flag', 'grade', 'inspection_type']}
            restaurant_dict[camis]['inspections'] = {}
        inspection_date_obj = row_dict.get('inspection_date')
        if inspection_date_obj:
            inspection_date_str = inspection_date_obj.isoformat()
            if inspection_date_str not in restaurant_dict[camis]['inspections']:
                restaurant_dict[camis]['inspections'][inspection_date_str] = {'inspection_date': inspection_date_str, 'critical_flag': row_dict.get('critical_flag'), 'grade': row_dict.get('grade'), 'inspection_type': row_dict.get('inspection_type'), 'violations': []}
            if row_dict.get('violation_code'):
                violation = {'violation_code': row_dict.get('violation_code'), 'violation_description': row_dict.get('violation_description')}
                if violation not in restaurant_dict[camis]['inspections'][inspection_date_str]['violations']:
                    restaurant_dict[camis]['inspections'][inspection_date_str]['violations'].append(violation)
    formatted_results = [dict(data, inspections=list(data['inspections'].values())) for data in restaurant_dict.values()]


    if redis_conn:
        try: redis_conn.setex(cache_key, CACHE_TTL_SECONDS, json.dumps(formatted_results, default=str))
        except Exception as e: logger.error(f"Test FTS Search Redis SETEX error: {e}"); sentry_sdk.capture_exception(e)
    return jsonify(formatted_results)

# --- Other Routes (/recent, /test-db-connection, /trigger-update) ---
# These should be the latest correct versions.
# For brevity, I am not repeating them fully here, but ensure they are in your file.
# They should match the versions from the "app_search_py_fts_aggressive_norm_v3_full" immersive.

@app.route('/recent', methods=['GET'])
def recent_restaurants():
    # ... (Keep your existing /recent logic from the last full file I sent) ...
    logger.info("Original /recent endpoint called for stability.")
    return jsonify([]) # Placeholder - use your actual recent logic

@app.route('/test-db-connection', methods=['GET'])
def test_db_connection():
    # ... (Keep your existing /test-db-connection logic) ...
    return jsonify({"status":"ok from original test-db"})

@app.route('/trigger-update', methods=['POST'])
def trigger_update():
    # ... (Keep your existing /trigger-update logic) ...
    logger.info("Original /trigger-update endpoint called for stability.")
    # This MUST call the run_database_update from the FTS v3 version of update_database.py
    # to ensure dba_tsv is populated correctly for new/updated records.
    if not update_logic_imported:
        return jsonify({"status": "error", "message": "Update logic unavailable."}), 500
    provided_key = request.headers.get('X-Update-Secret')
    expected_key = APIConfig.UPDATE_SECRET_KEY
    if not expected_key or not secrets.compare_digest(provided_key or '', expected_key):
        return jsonify({"status": "error", "message": "Unauthorized."}), 403
    threading.Thread(target=run_database_update, daemon=True).start()
    return jsonify({"status": "success", "message": "Database update triggered."}), 202


# --- Error Handlers ---
@app.errorhandler(404)
def not_found_error_handler(error):
    return jsonify({"error": "Not found"}), 404

@app.errorhandler(500)
def internal_server_error_handler(error):
    logger.error(f"500 Error: {error}", exc_info=True) # Add exc_info
    sentry_sdk.capture_exception(error)
    return jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    host = os.environ.get("HOST", getattr(APIConfig, 'HOST', "0.0.0.0"))
    port = int(os.environ.get("PORT", getattr(APIConfig, 'PORT', 8080)))
    debug_mode = getattr(APIConfig, 'DEBUG', False)
    app.run(host=host, port=port, debug=debug_mode)
