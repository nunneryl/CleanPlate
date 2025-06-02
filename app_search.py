# app_search.py - With original /search and new /search_fts_test endpoint (explicit prefix logic)

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
    from config import APIConfig, SentryConfig # Using only these as per the uploaded file
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
    def run_database_update(days_back=5): # Matching definition from uploaded file
         logging.error("DUMMY run_database_update called - real function failed to import.")

# --- Sentry Initialization ---
if hasattr(SentryConfig, 'SENTRY_DSN') and SentryConfig.SENTRY_DSN: # Check attribute exists
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
if not logging.getLogger().hasHandlers(): # Check if handlers are already configured
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)
logger.info("Logging configured.")

# --- Flask App Initialization ---
app = Flask(__name__)
CORS(app)
logger.info("Flask app created.")

# --- AGGRESSIVE NORMALIZATION FUNCTION (Python-side, for FTS input) ---
# This is the aggressive version we decided on.
def normalize_text(text):
    if not isinstance(text, str): return ''
    text = text.lower()
    text = text.replace("'", "").replace(".", "").replace('&', 'and')
    text = re.sub(r'[^\w\s]', '', text) # Remove non-alphanumeric but keep spaces temporarily
    text = re.sub(r'\s+', '', text).strip() # Remove all spaces
    return text

# --- ORIGINAL SANITIZE INPUT (For the old /search endpoint) ---
def original_sanitize_input(input_str): # Copied from user's uploaded file
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
# This function is named search_original in the user's uploaded file.
@app.route('/search', methods=['GET'])
def search_original():
    logger.info("Received request for ORIGINAL /search endpoint")
    name = request.args.get('name', '').strip()
    if not name:
        return jsonify({"error": "Search term is empty"}), 400

    normalized_name_for_key = name.replace("’", "'").replace("‘", "'").lower().strip()
    cache_key = f"search_orig:{normalized_name_for_key}"
    CACHE_TTL_SECONDS = 3600 * 4

    redis_conn = get_redis_client()
    if redis_conn:
        try:
            cached_result_str = redis_conn.get(cache_key)
            if cached_result_str:
                logger.info(f"Cache hit for original search: '{name}'")
                return jsonify(json.loads(cached_result_str))
        except Exception as e:
             logger.error(f"Original Search Redis GET error for key {cache_key}: {e}"); sentry_sdk.capture_exception(e)
    
    name_with_periods, name_without_periods = original_sanitize_input(name)
    # Logic for name_with_added_periods from user's file
    if '.' not in name and len(name_without_periods) >= 2:
        name_with_added_periods = '.'.join(list(name_without_periods))
    else:
        name_with_added_periods = name_with_periods
    
    transformed_name = name_with_periods.replace("s", "'s")
    transformed_name_no_periods = name_without_periods.replace("s", "'s")
    transformed_with_added_periods = name_with_added_periods.replace("s", "'s")
    
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
    # Parameters as per user's uploaded file
    where_params = [ f"%{p}%" for p in [name_with_periods, transformed_name, name_without_periods, transformed_name_no_periods, transformed_with_added_periods] ]
    order_params = [ name_with_periods, name_without_periods, f"{name_with_periods}%", f"{name_without_periods}%", f"{transformed_with_added_periods}%" ]
    params = where_params + order_params
    
    db_results_raw = None
    columns = [] # Initialize columns
    try:
        with DatabaseConnection() as conn, conn.cursor() as cursor: # Default cursor
            logger.debug(f"Original Search: Executing query for '{name}'")
            cursor.execute(query, params)
            db_results_raw = cursor.fetchall()
            if db_results_raw is not None:
                columns = [desc[0] for desc in cursor.description] # Get column names
                logger.debug(f"Original Search: DB query OK, {len(db_results_raw)} rows fetched.")
    except Exception as e: # Catch broader exceptions
        logger.error(f"Original Search DB error for '{name}': {e}", exc_info=True)
        sentry_sdk.capture_exception(e)
        return jsonify({"error": "Database query failed"}), 500

    if not db_results_raw:
        logger.info(f"Original Search: No DB results for '{name}'")
        return jsonify([]) # Return empty list if no results

    db_results = [dict(zip(columns, row)) for row in db_results_raw]
    restaurant_dict = {}
    # Result processing logic from user's file
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
        try:
            redis_conn.setex(cache_key, CACHE_TTL_SECONDS, json.dumps(formatted_results, default=str))
            logger.info(f"Original Search: Stored result in cache for key: {cache_key}")
        except Exception as e:
            logger.error(f"Original Search Redis SETEX error for key {cache_key}: {e}"); sentry_sdk.capture_exception(e)
    return jsonify(formatted_results)

# --- NEW /search_fts_test ENDPOINT (for testing FTS with explicit prefix for single terms) ---
@app.route('/search_fts_test', methods=['GET'])
def search_fts_test():
    logger.info("Received request for TEST /search_fts_test endpoint")
    search_term_from_user = request.args.get('name', '').strip()
    if not search_term_from_user:
        return jsonify({"error": "Search term is empty"}), 400

    normalized_user_input_for_query = normalize_text(search_term_from_user) # Aggressive normalize
    if not normalized_user_input_for_query:
        logger.info(f"FTS Test: Search term '{search_term_from_user}' empty after normalization.")
        return jsonify([])

    # <<<< START OF MODIFIED LOGIC FOR EXPLICIT PREFIX >>>>
    query_terms = normalized_user_input_for_query.split() # Should be empty if input was all spaces
                                                        # but normalize_text also handles this.
                                                        # If normalized_user_input_for_query is "xian", query_terms = ["xian"]
                                                        # If normalized_user_input_for_query is "eatsplace", query_terms = ["eatsplace"]
    
    if len(query_terms) == 1 and query_terms[0]: # Check if there's exactly one term and it's not empty
        final_query_string_for_fts = query_terms[0] + ":*"
    elif len(query_terms) > 1: # For multiple terms, websearch_to_tsquery handles ANDing.
        # websearch_to_tsquery typically applies prefix to the last term.
        # We can pass the Python-normalized multi-word string directly.
        final_query_string_for_fts = normalized_user_input_for_query
    else: # Should not happen if normalized_user_input_for_query was checked for emptiness
        logger.warning(f"FTS Test: normalized_user_input_for_query ('{normalized_user_input_for_query}') resulted in no query_terms.")
        return jsonify([])


    logger.info(f"FTS Test: DB query using websearch_to_tsquery with FTS query string: '{final_query_string_for_fts}' (from normalized input '{normalized_user_input_for_query}')")
    # <<<< END OF MODIFIED LOGIC FOR EXPLICIT PREFIX >>>>

    cache_key = f"search_v6_fts_test:{final_query_string_for_fts}" # Updated cache key with new logic
    CACHE_TTL_SECONDS = 3600 * 1

    redis_conn = get_redis_client()
    if redis_conn:
        try:
            cached_result_str = redis_conn.get(cache_key)
            if cached_result_str:
                logger.info(f"FTS Test: Cache hit for key: {cache_key}")
                return jsonify(json.loads(cached_result_str))
        except Exception as e:
             logger.error(f"FTS Test: Redis GET error for key {cache_key}: {e}"); sentry_sdk.capture_exception(e)

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
    params = (final_query_string_for_fts, final_query_string_for_fts) # Use the potentially modified query string
    
    db_results_raw = None
    columns_fts = [] # Use a different variable name for columns to avoid scope issues
    try:
        with DatabaseConnection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            logger.debug(f"FTS Test: Executing query with params: {params}")
            cursor.execute(query, params)
            db_results_raw = cursor.fetchall()
            if db_results_raw is not None:
                columns_fts = [desc[0] for desc in cursor.description]
                logger.debug(f"FTS Test: DB query OK, {len(db_results_raw)} rows fetched.")
    except Exception as e:
        logger.error(f"FTS Test: DB error for query string '{final_query_string_for_fts}': {e}", exc_info=True)
        sentry_sdk.capture_exception(e)
        return jsonify({"error": "Database query failed"}), 500

    if not db_results_raw:
        logger.info(f"FTS Test: No DB results for query string '{final_query_string_for_fts}'")
        return jsonify([])
    
    db_results = [dict(row) for row in db_results_raw]
    restaurant_dict_fts = {} # Use a different variable name
    # Result processing logic - same as before
    for row_dict in db_results:
        camis = row_dict.get('camis')
        if not camis: continue
        if camis not in restaurant_dict_fts:
            restaurant_dict_fts[camis] = {k: v for k, v in row_dict.items() if k not in ['rank', 'violation_code', 'violation_description', 'inspection_date', 'critical_flag', 'grade', 'inspection_type']}
            restaurant_dict_fts[camis]['inspections'] = {}
        inspection_date_obj = row_dict.get('inspection_date')
        if inspection_date_obj:
            inspection_date_str = inspection_date_obj.isoformat()
            if inspection_date_str not in restaurant_dict_fts[camis]['inspections']:
                restaurant_dict_fts[camis]['inspections'][inspection_date_str] = {'inspection_date': inspection_date_str, 'critical_flag': row_dict.get('critical_flag'), 'grade': row_dict.get('grade'), 'inspection_type': row_dict.get('inspection_type'), 'violations': []}
            if row_dict.get('violation_code'):
                violation = {'violation_code': row_dict.get('violation_code'), 'violation_description': row_dict.get('violation_description')}
                if violation not in restaurant_dict_fts[camis]['inspections'][inspection_date_str]['violations']:
                    restaurant_dict_fts[camis]['inspections'][inspection_date_str]['violations'].append(violation)
    formatted_results = [dict(data, inspections=list(data['inspections'].values())) for data in restaurant_dict_fts.values()]

    if redis_conn:
        try:
            redis_conn.setex(cache_key, CACHE_TTL_SECONDS, json.dumps(formatted_results, default=str))
            logger.info(f"FTS Test: Stored result in cache for key: {cache_key}")
        except Exception as e:
            logger.error(f"FTS Test: Redis SETEX error for key {cache_key}: {e}"); sentry_sdk.capture_exception(e)
    return jsonify(formatted_results)

# --- Other Routes (/recent, /test-db-connection, /trigger-update) ---
# These are copied from the version the user uploaded, assuming they are stable/correct.
@app.route('/recent', methods=['GET'])
def recent_restaurants():
    logger.info("Received request for /recent")
    days = request.args.get('days', '7');
    try: days = int(days); days = 7 if days <= 0 else days
    except ValueError: days = 7
    logger.info(f"Fetching recent restaurants (graded A/B/C) from past {days} days.")
    query = """ SELECT DISTINCT ON (r.camis) r.camis, r.dba, r.boro, r.building, r.street, r.zipcode, r.phone, r.latitude, r.longitude, r.grade, r.inspection_date, r.cuisine_description FROM restaurants r WHERE r.grade IN ('A', 'B', 'C') AND r.inspection_date >= (CURRENT_DATE - INTERVAL '%s days') ORDER BY r.camis, r.inspection_date DESC LIMIT 50 """
    try:
        with DatabaseConnection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(query, (days,)); results_raw = cursor.fetchall()
                results = [dict(row) for row in results_raw]
                logger.info(f"Recent restaurants fetch returned {len(results)} results.")
                return jsonify(results)
    except psycopg2.Error as db_err: logger.error(f"Error fetching recent restaurants: {db_err}"); sentry_sdk.capture_exception(db_err); return jsonify({"error": "DB error"}), 500 # Propagate error
    except Exception as e: logger.error(f"Unexpected error fetching recent restaurants: {e}", exc_info=True); sentry_sdk.capture_exception(e); return jsonify({"error": "Unexpected error"}), 500


@app.route('/test-db-connection', methods=['GET'])
def test_db_connection():
    logger.info("Received request for /test-db-connection")
    try:
        with DatabaseConnection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1"); result = cursor.fetchone()
                if result and result[0] == 1: logger.info("Database connection test successful."); return jsonify({"status": "success", "message": "Database connection successful"})
                else: logger.error("DB connection test failed: Query returned unexpected result."); return jsonify({"status": "error", "message": "DB query failed"}), 500
    except Exception as e: logger.error(f"Database connection test failed: {e}"); return jsonify({"status": "error", "message": f"DB connection error: {str(e)}"}), 500


@app.route('/trigger-update', methods=['POST'])
def trigger_update():
    logger.info("Received request for /trigger-update")
    try:
        if not update_logic_imported:
            logger.error("Update logic unavailable."); return jsonify({"status": "error", "message": "Update logic unavailable."}), 500
        provided_key = request.headers.get('X-Update-Secret')
        expected_key = APIConfig.UPDATE_SECRET_KEY
        if not expected_key:
            logger.error("UPDATE_SECRET_KEY not configured."); return jsonify({"status": "error", "message": "Update trigger not configured."}), 500
        if not provided_key or not secrets.compare_digest(provided_key, expected_key):
            logger.warning("Invalid/missing secret key for /trigger-update."); return jsonify({"status": "error", "message": "Unauthorized."}), 403
        logger.info("Secret key validated. Triggering update in background.")
        try:
            update_thread = threading.Thread(target=run_database_update, daemon=True); update_thread.start()
        except Exception as e:
            logger.error(f"Failed to start update thread: {e}", exc_info=True); sentry_sdk.capture_exception(e); return jsonify({"status": "error", "message": "Failed to start update process."}), 500
        logger.info("Successfully launched background update thread.")
        return jsonify({"status": "success", "message": "Database update triggered in background."}), 202
    except Exception as e:
        logger.error(f"Unexpected error in /trigger-update handler: {e}", exc_info=True)
        sentry_sdk.capture_exception(e)
        return jsonify({"status": "error", "message": "Internal server error in trigger endpoint."}), 500

# --- Error Handlers ---
@app.errorhandler(404)
def not_found_error_handler(error): # Renamed parameter from 'e'
    logger.warning(f"404 Not Found error for URL: {request.url}")
    return jsonify({"error": "The requested resource was not found", "status": "error"}), 404

@app.errorhandler(500)
def internal_server_error_handler(error): # Renamed parameter from 'e'
    logger.error(f"500 Internal Server Error handling request for {request.url}: {error}", exc_info=True)
    sentry_sdk.capture_exception(error)
    return jsonify({"error": "An internal server error occurred", "status": "error"}), 500

# --- Main Execution Block ---
if __name__ == "__main__":
    # Use APIConfig for host and port, with fallback to os.environ.get for Railway/containerized environments
    host = os.environ.get("HOST", getattr(APIConfig, 'HOST', "0.0.0.0"))
    port = int(os.environ.get("PORT", getattr(APIConfig, 'PORT', 8080)))
    debug_mode = getattr(APIConfig, 'DEBUG', False)
    
    logger.info(f"Starting Flask app locally via app.run() on {host}:{port} with DEBUG={debug_mode}")
    app.run(host=host, port=port, debug=debug_mode)

logger.info("app_search.py: Module loaded completely.")
