# app_search.py - Hybrid FTS + Trigram for /search_fts_test

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
    from config import APIConfig, SentryConfig # As per user's last uploaded app_search.py
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
if hasattr(SentryConfig, 'SENTRY_DSN') and SentryConfig.SENTRY_DSN:
    try:
        sentry_sdk.init(
            dsn=SentryConfig.SENTRY_DSN, integrations=[FlaskIntegration()],
            traces_sample_rate=1.0, environment="production"
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

# --- PYTHON NORMALIZATION FUNCTION (to mirror SQL normalize_dba) ---
def normalize_search_term_for_hybrid(text):
    if not isinstance(text, str):
        return ''
    
    normalized_text = text.lower()
    
    # Basic accent normalization (consider unidecode library for production if more comprehensive handling is needed)
    # This is a simplified version. The database side uses unaccent() which is more robust.
    accent_map = {
        'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
        'á': 'a', 'à': 'a', 'â': 'a', 'ä': 'a',
        'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i',
        'ó': 'o', 'ò': 'o', 'ô': 'o', 'ö': 'o',
        'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u',
        'ç': 'c', 'ñ': 'n'
    }
    for accented, unaccented in accent_map.items():
        normalized_text = normalized_text.replace(accented, unaccented)
    
    # Mimic SQL: regexp_replace(normalized_text, E'\'s\\b|s\'\\b', '', 'g');
    normalized_text = re.sub(r"'s\b|s'\b", "", normalized_text, flags=re.IGNORECASE)
    
    # Mimic SQL: regexp_replace(normalized_text, E'[-/]', ' ', 'g');
    normalized_text = re.sub(r"[-/]", " ", normalized_text)
    
    # Mimic SQL: regexp_replace(normalized_text, E'[^a-z0-9\\s]', '', 'g');
    normalized_text = re.sub(r"[^a-z0-9\s]", "", normalized_text) # Already lowercased
    
    # Mimic SQL: regexp_replace(normalized_text, E'\\s+', ' ', 'g');
    normalized_text = re.sub(r"\s+", " ", normalized_text)
    
    normalized_text = normalized_text.strip()
    
    return normalized_text

# --- ORIGINAL SANITIZE INPUT (For the old /search endpoint, from user's file) ---
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
def search_original():
    logger.info("Received request for ORIGINAL /search endpoint")
    name = request.args.get('name', '').strip()
    if not name:
        return jsonify({"error": "Search term is empty"}), 400

    normalized_name_for_key = name.replace("’", "'").replace("‘", "'").lower().strip()
    cache_key = f"search_orig:{normalized_name_for_key}"
    CACHE_TTL_SECONDS = 3600 * 4

    #redis_conn = get_redis_client()
    #if redis_conn:
    #    try:
     #       cached_result_str = redis_conn.get(cache_key)
      #      if cached_result_str:
       #         return jsonify(json.loads(cached_result_str))
        #except Exception as e:
         #    logger.error(f"Original Search Redis GET error for key {cache_key}: {e}")
          #   sentry_sdk.capture_exception(e) if SentryConfig.SENTRY_DSN else None
    
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
    
    db_results_raw = None; columns = []
    try:
        with DatabaseConnection() as conn, conn.cursor() as cursor:
            cursor.execute(query, params)
            db_results_raw = cursor.fetchall()
            if db_results_raw is not None: columns = [desc[0] for desc in cursor.description]
    except Exception as e:
        logger.error(f"Original Search DB error: {e}", exc_info=True)
        sentry_sdk.capture_exception(e) if SentryConfig.SENTRY_DSN else None
        return jsonify({"error": "Database query failed"}), 500

    if not db_results_raw: return jsonify([])
    db_results = [dict(zip(columns, row)) for row in db_results_raw]
    restaurant_dict = {}
    for row_dict in db_results: # Result processing from user's file
        camis = row_dict.get('camis'); inspection_date_obj = row_dict.get('inspection_date')
        if not camis: continue
        inspection_date_str = inspection_date_obj.isoformat() if hasattr(inspection_date_obj, 'isoformat') else str(inspection_date_obj)
        if camis not in restaurant_dict: restaurant_dict[camis] = {k: v for k, v in row_dict.items() if k not in ['violation_code', 'violation_description', 'inspection_date', 'critical_flag', 'grade', 'inspection_type']}; restaurant_dict[camis]['inspections'] = {}
        inspections = restaurant_dict[camis]["inspections"]
        if inspection_date_str and inspection_date_str not in inspections: inspections[inspection_date_str] = {'inspection_date': inspection_date_str, 'critical_flag': row_dict.get('critical_flag'), 'grade': row_dict.get('grade'), 'inspection_type': row_dict.get('inspection_type'), 'violations': []}
        if inspection_date_str and row_dict.get('violation_code'):
            violation = {'violation_code': row_dict.get('violation_code'), 'violation_description': row_dict.get('violation_description')}
            if violation not in inspections[inspection_date_str]["violations"]: inspections[inspection_date_str]["violations"].append(violation)
    formatted_results = [dict(data, inspections=list(data['inspections'].values())) for data in restaurant_dict.values()]
    #if redis_conn:
     #   try: redis_conn.setex(cache_key, CACHE_TTL_SECONDS, json.dumps(formatted_results, default=str))
      #  except Exception as e: logger.error(f"Original Search Redis SETEX error: {e}"); sentry_sdk.capture_exception(e) if SentryConfig.SENTRY_DSN else None
    return jsonify(formatted_results)

# --- NEW /search_fts_test ENDPOINT (Hybrid FTS + Trigram) ---
@app.route('/search_fts_test', methods=['GET'])
def search_fts_test():
    logger.info("---- /search_fts_test: Request received ----")
    search_term_from_user = request.args.get('name', '').strip()
    logger.info(f"/search_fts_test: Raw search term from user: '{search_term_from_user}'")

    if not search_term_from_user:
        logger.warning("/search_fts_test: Search term is empty, returning 400.")
        return jsonify({"error": "Search term is empty"}), 400

   normalized_user_input = normalize_search_term_for_hybrid(search_term_from_user)
    logger.info(f"/search_fts_test: Python normalized input: '{normalized_user_input}'")

    if not normalized_user_input:
        logger.info(f"/search_fts_test: Normalized input is empty, returning empty list.")
        return jsonify([])

    # Explicit prefixing for to_tsquery, as websearch_to_tsquery wasn't giving prefix match
    # on single terms like 'xian' against 'xianfamousfood'
    # This logic was from our successful "explicit prefix" attempt.
    query_terms = normalized_user_input.split() # Should be just one term after aggressive normalization
    if len(query_terms) == 1 and query_terms[0]:
        fts_query_string = query_terms[0] + ":*"
    elif len(query_terms) > 1: # Should not happen with current aggressive normalize_text
        fts_query_string = ' & '.join([term + ":*" for term in query_terms])
    else:
        logger.warning(f"/search_fts_test: normalized_user_input ('{normalized_user_input}') resulted in no query_terms for FTS string.")
        return jsonify([])
        
    logger.info(f"/search_fts_test: Final FTS query string for to_tsquery: '{fts_query_string}'")

    # Cache is still temporarily disabled for this endpoint for debugging from previous step
    # We will re-enable it once this is working.
    # cache_key = f"search_v_debug_fts_test:{fts_query_string}"
    # logger.info(f"/search_fts_test: Cache key: {cache_key}")
    # ... (Redis GET logic would be here) ...

    query = """
        SELECT
            r.camis, r.dba, r.boro, r.building, r.street, r.zipcode, r.phone,
            r.latitude, r.longitude, r.inspection_date, r.critical_flag, r.grade,
            r.inspection_type, v.violation_code, v.violation_description, r.cuisine_description,
            ts_rank_cd(r.dba_tsv, to_tsquery('english', %s)) AS rank
        FROM restaurants r
        LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date
        WHERE r.dba_tsv @@ to_tsquery('english', %s) -- Using to_tsquery with explicit prefix
        ORDER BY rank DESC, r.dba ASC, r.inspection_date DESC
        LIMIT 100;
    """
    params = (fts_query_string, fts_query_string)
    logger.info(f"/search_fts_test: SQL Query = {query}")
    logger.info(f"/search_fts_test: SQL Params = {params}")

    db_results_raw = None
    columns_fts = []
    try:
        with DatabaseConnection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(query, params)
            db_results_raw = cursor.fetchall()
            if db_results_raw is not None:
                columns_fts = [desc[0] for desc in cursor.description]
                logger.info(f"/search_fts_test: DB query executed. Number of raw rows fetched: {len(db_results_raw)}")
                if len(db_results_raw) > 0:
                    logger.debug(f"/search_fts_test: First raw row sample: {dict(db_results_raw[0]) if db_results_raw else 'No rows'}")
            else:
                logger.warning("/search_fts_test: db_results_raw is None after query execution.")
                db_results_raw = []
    except Exception as e:
        logger.error(f"/search_fts_test: DB error for FTS query string '{fts_query_string}': {e}", exc_info=True)
        sentry_sdk.capture_exception(e) if SentryConfig.SENTRY_DSN else None
        return jsonify({"error": "Database query failed"}), 500

    if not db_results_raw:
        logger.info(f"/search_fts_test: No DB results for FTS query string '{fts_query_string}', returning empty list.")
        # ... (Redis SETEX for empty result would be here if cache was active) ...
        return jsonify([])
    
    logger.info(f"/search_fts_test: Starting processing of {len(db_results_raw)} raw rows.")
    db_results = [dict(row) for row in db_results_raw]
    restaurant_dict_fts = {}
    for row_dict in db_results:
        camis = row_dict.get('camis')
        if not camis:
            logger.warning("/search_fts_test: Row found with no CAMIS in DB results, skipping.")
            continue
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
    logger.info(f"/search_fts_test: Processed results. Final count: {len(formatted_results)}")
    # ... (Redis SETEX for successful result would be here if cache was active) ...
    return jsonify(formatted_results)
    
# --- Other Routes (/recent, /test-db-connection, /trigger-update from user's uploaded file) ---
@app.route('/recent', methods=['GET'])
def recent_restaurants():
    logger.info("Received request for /recent")
    days = request.args.get('days', '7');
    try: days = int(days); days = 7 if days <= 0 else days # Simplified from user's file
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
    except psycopg2.Error as db_err: logger.error(f"Error fetching recent restaurants: {db_err}"); sentry_sdk.capture_exception(db_err) if SentryConfig.SENTRY_DSN else None; return jsonify({"error": "DB error"}), 500
    except Exception as e: logger.error(f"Unexpected error fetching recent restaurants: {e}", exc_info=True); sentry_sdk.capture_exception(e) if SentryConfig.SENTRY_DSN else None; return jsonify({"error": "Unexpected error"}), 500

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
            logger.error(f"Failed to start update thread: {e}", exc_info=True); sentry_sdk.capture_exception(e) if SentryConfig.SENTRY_DSN else None; return jsonify({"status": "error", "message": "Failed to start update process."}), 500
        logger.info("Successfully launched background update thread.")
        return jsonify({"status": "success", "message": "Database update triggered in background."}), 202
    except Exception as e:
        logger.error(f"Unexpected error in /trigger-update handler: {e}", exc_info=True)
        sentry_sdk.capture_exception(e) if SentryConfig.SENTRY_DSN else None
        return jsonify({"status": "error", "message": "Internal server error in trigger endpoint."}), 500

# --- Error Handlers ---
@app.errorhandler(404)
def not_found_error_handler(error):
    logger.warning(f"404 Not Found error for URL: {request.url}")
    return jsonify({"error": "The requested resource was not found", "status": "error"}), 404

@app.errorhandler(500)
def internal_server_error_handler(error):
    logger.error(f"500 Internal Server Error handling request for {request.url}: {error}", exc_info=True)
    sentry_sdk.capture_exception(error) if SentryConfig.SENTRY_DSN else None
    return jsonify({"error": "An internal server error occurred", "status": "error"}), 500

# --- Main Execution Block ---
if __name__ == "__main__":
    host = os.environ.get("HOST", getattr(APIConfig, 'HOST', "0.0.0.0"))
    port = int(os.environ.get("PORT", getattr(APIConfig, 'PORT', 8080)))
    debug_mode = getattr(APIConfig, 'DEBUG', False)
    
    logger.info(f"Starting Flask app locally via app.run() on {host}:{port} with DEBUG={debug_mode}")
    app.run(host=host, port=port, debug=debug_mode)

logger.info("app_search.py: Module loaded completely.")
