# app_search.py - Hybrid FTS + Trigram for /search_fts_test with Detailed Logging

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
    """
    Canonical normalization function to match the database's normalize_dba() behavior.
    This version keeps spaces for punctuation to separate terms like "Xi'an" into "xi an".
    """
    if not isinstance(text, str):
        return ''
    
    # Lowercase and handle common substitutions
    normalized_text = text.lower()
    accent_map = {
        'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e', 'á': 'a', 'à': 'a', 'â': 'a', 'ä': 'a',
        'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i', 'ó': 'o', 'ò': 'o', 'ô': 'o', 'ö': 'o',
        'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u', 'ç': 'c', 'ñ': 'n'
    }
    for accented, unaccented in accent_map.items():
        normalized_text = normalized_text.replace(accented, unaccented)
    
    # Replace key punctuation with spaces INSTEAD of removing them.
    # This correctly tokenizes terms like "Xi'an" -> "xi an" and "E.J.'s" -> "e j s"
    normalized_text = re.sub(r"['./-]", " ", normalized_text)
    
    # Remove any remaining characters that are not alphanumeric or whitespace
    normalized_text = re.sub(r"[^a-z0-9\s]", "", normalized_text)
    
    # Collapse multiple spaces into one and strip whitespace
    normalized_text = re.sub(r"\s+", " ", normalized_text).strip()
    
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

    redis_conn = get_redis_client()
    if redis_conn:
        try:
            cached_result_str = redis_conn.get(cache_key)
            if cached_result_str:
                logger.info(f"Cache hit for original search: '{name}'")
                return jsonify(json.loads(cached_result_str))
        except Exception as e:
             logger.error(f"Original Search Redis GET error for key {cache_key}: {e}")
             sentry_sdk.capture_exception(e) if SentryConfig.SENTRY_DSN else None
    
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
    for row_dict in db_results:
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
    if redis_conn:
        try: redis_conn.setex(cache_key, CACHE_TTL_SECONDS, json.dumps(formatted_results, default=str))
        except Exception as e: logger.error(f"Original Search Redis SETEX error: {e}"); sentry_sdk.capture_exception(e) if SentryConfig.SENTRY_DSN else None
    return jsonify(formatted_results)

# --- /search_fts_test ENDPOINT (Hybrid FTS + Trigram with Detailed Logging) ---
@app.route('/search_fts_test', methods=['GET'])
def search_fts_test():
    logger.info("---- /search_fts_test: Request received ----")
    search_term_from_user = request.args.get('name', '').strip()
    logger.info(f"/search_fts_test: Raw search term from user: '{search_term_from_user}'")

    if not search_term_from_user:
        logger.warning("/search_fts_test: Search term is empty, returning 400.")
        return jsonify({"error": "Search term is empty"}), 400

    # Use the NEW, CORRECTED Python function that mirrors SQL normalize_dba()
    normalized_for_pg = normalize_search_term_for_hybrid(search_term_from_user)
    logger.info(f"/search_fts_test: Python normalized input for PG: '{normalized_for_pg}'")
    
    if not normalized_for_pg:
        logger.info(f"/search_fts_test: Normalized query is empty, returning empty list.")
        return jsonify([])

    # --- START OF QUERY REFINEMENT ---
    # Modify the normalized term for better FTS prefix matching.
    # 'lunch' -> 'lunch:*', 'xi an' -> 'xi & an:*'
    query_terms = normalized_for_pg.split()
    if query_terms:
        # Add the prefix operator to the last term
        query_terms[-1] = query_terms[-1] + ':*'
    # Join terms with '&' for websearch_to_tsquery, which handles 'AND' logic
    fts_query_string = ' & '.join(query_terms)
    logger.info(f"/search_fts_test: FTS-ready query string: '{fts_query_string}'")
    # --- END OF QUERY REFINEMENT ---

    cache_key = f"search_hybrid_v2_dl:{normalized_for_pg}" # v2 reflects new logic
    CACHE_TTL_SECONDS = 3600 * 1

    # <<<< REDIS GET temporarily commented out for debugging >>>>
    # redis_conn = get_redis_client()
    # if redis_conn:
    #     try:
    #         cached_result_str = redis_conn.get(cache_key)
    #         if cached_result_str:
    #             logger.info(f"/search_fts_test: Cache hit for key: {cache_key}")
    #             return jsonify(json.loads(cached_result_str))
    #         else:
    #             logger.info(f"/search_fts_test: Cache miss for key: {cache_key}")
    #     except Exception as e:
    #          logger.error(f"/search_fts_test: Redis GET error for key {cache_key}: {e}")
    #          sentry_sdk.capture_exception(e) if SentryConfig.SENTRY_DSN else None
    
    logger.info(f"/search_fts_test: Proceeding to DB query with normalized input: '{normalized_for_pg}' and FTS query: '{fts_query_string}'")
    
    query = """
    WITH user_input AS (
        SELECT 
            %s AS normalized_query,
            %s AS fts_query_string
    )
    SELECT
        r.camis, r.dba, r.boro, r.building, r.street, r.zipcode, r.phone,
        r.latitude, r.longitude, r.inspection_date, r.critical_flag, r.grade,
        r.inspection_type, v.violation_code, v.violation_description, r.cuisine_description,
        ts_rank_cd(r.dba_tsv, websearch_to_tsquery('public.restaurant_search_config', ui.fts_query_string), 32) AS fts_score,
        word_similarity(ui.normalized_query, r.dba_normalized_search) AS trgm_word_similarity
    FROM
        restaurants r
        JOIN user_input ui ON TRUE 
        LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date
    WHERE
        (r.dba_tsv @@ websearch_to_tsquery('public.restaurant_search_config', ui.fts_query_string))
        OR
        (word_similarity(ui.normalized_query, r.dba_normalized_search) > 0.3) -- Lowered threshold slightly
    ORDER BY
        (COALESCE(ts_rank_cd(r.dba_tsv, websearch_to_tsquery('public.restaurant_search_config', ui.fts_query_string), 32), 0) * 1.2) + -- Increased FTS weight
        (COALESCE(word_similarity(ui.normalized_query, r.dba_normalized_search), 0) * 0.9) DESC,
        r.dba ASC, 
        r.inspection_date DESC
    LIMIT 75; -- Increased limit slightly
    """
    params = (normalized_query_for_pg,)
    logger.info(f"/search_fts_test: SQL Query = {query}") # Log the query structure
    logger.info(f"/search_fts_test: SQL Params = {params}") # Log the params

    db_results_raw = None; columns_hybrid = []
    try:
        with DatabaseConnection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(query, params)
            db_results_raw = cursor.fetchall()
            # <<<< DETAILED LOGGING FOR RAW DB RESULTS >>>>
            if db_results_raw is not None:
                logger.info(f"/search_fts_test: RAW DB RESULTS COUNT: {len(db_results_raw)}")
                if len(db_results_raw) > 0:
                    logger.info(f"/search_fts_test: First raw DB result sample: {dict(db_results_raw[0])}")
                columns_hybrid = [desc[0] for desc in cursor.description]
            else:
                logger.warning("/search_fts_test: db_results_raw is None after query execution.")
                db_results_raw = []
    except Exception as e:
        logger.error(f"/search_fts_test: DB error for normalized input '{normalized_query_for_pg}': {e}", exc_info=True)
        sentry_sdk.capture_exception(e) if SentryConfig.SENTRY_DSN else None
        return jsonify({"error": "Database query failed"}), 500

    if not db_results_raw:
        logger.info(f"/search_fts_test: No DB results for normalized input '{normalized_query_for_pg}', returning empty list.")
        return jsonify([])
    
    db_results = [dict(row) for row in db_results_raw]
    restaurant_dict_hybrid = {}
    for row_dict in db_results:
        camis = row_dict.get('camis')
        if not camis:
            logger.warning("/search_fts_test: Row found with no CAMIS in DB results, skipping.")
            continue
        if camis not in restaurant_dict_hybrid:
            restaurant_dict_hybrid[camis] = {k: v for k, v in row_dict.items() if k not in ['fts_score', 'trgm_word_similarity', 'trgm_direct_similarity', 'violation_code', 'violation_description', 'inspection_date', 'critical_flag', 'grade', 'inspection_type']}
            restaurant_dict_hybrid[camis]['inspections'] = {}
        inspection_date_obj = row_dict.get('inspection_date')
        if inspection_date_obj:
            inspection_date_str = inspection_date_obj.isoformat()
            if inspection_date_str not in restaurant_dict_hybrid[camis]['inspections']:
                restaurant_dict_hybrid[camis]['inspections'][inspection_date_str] = {'inspection_date': inspection_date_str, 'critical_flag': row_dict.get('critical_flag'), 'grade': row_dict.get('grade'), 'inspection_type': row_dict.get('inspection_type'), 'violations': []}
            if row_dict.get('violation_code'):
                violation = {'violation_code': row_dict.get('violation_code'), 'violation_description': row_dict.get('violation_description')}
                if violation not in restaurant_dict_hybrid[camis]['inspections'][inspection_date_str]['violations']:
                    restaurant_dict_hybrid[camis]['inspections'][inspection_date_str]['violations'].append(violation)
    
    formatted_results = [dict(data, inspections=list(data['inspections'].values())) for data in restaurant_dict_hybrid.values()]
    # <<<< DETAILED LOGGING FOR FINAL FORMATTED RESULTS >>>>
    logger.info(f"/search_fts_test: FINAL FORMATTED RESULTS COUNT: {len(formatted_results)}")
    if formatted_results and len(formatted_results) > 0:
        logger.info(f"/search_fts_test: First formatted result sample: {formatted_results[0]}")
    
    # <<<< REDIS SETEX temporarily commented out for debugging >>>>
    # if redis_conn:
    #     try:
    #         redis_conn.setex(cache_key, CACHE_TTL_SECONDS, json.dumps(formatted_results, default=str))
    #         logger.info(f"/search_fts_test: Stored result in cache for key: {cache_key}")
    #     except Exception as e:
    #         logger.error(f"/search_fts_test: Redis SETEX error for key {cache_key}: {e}")
    #         sentry_sdk.capture_exception(e) if SentryConfig.SENTRY_DSN else None
    return jsonify(formatted_results)

# --- Other Routes (/recent, /test-db-connection, /trigger-update from user's uploaded file) ---
@app.route('/recent', methods=['GET'])
def recent_restaurants():
    logger.info("Received request for /recent")
    days = request.args.get('days', '7');
    try: days = int(days); days = 7 if days <= 0 else days
    except ValueError: days = 7
    logger.info(f"Fetching recent restaurants (graded A/B/C) from past {days} days.")
    query = """ SELECT DISTINCT ON (r.camis) r.camis, r.dba, r.boro, r.building, r.street, r.zipcode, r.phone, r.latitude, r.longitude, r.grade, r.inspection_date, r.cuisine_description FROM restaurants r WHERE r.grade IN ('A', 'B', 'C') AND r.inspection_date >= (CURRENT_DATE - INTERVAL '%s days') ORDER BY r.camis, r.inspection_date DESC LIMIT 50 """
    try:
        with DatabaseConnection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(query, (days,)); results_raw = cursor.fetchall()
            results = [dict(row) for row in results_raw]
            return jsonify(results)
    except psycopg2.Error as db_err: logger.error(f"Error fetching recent restaurants: {db_err}"); sentry_sdk.capture_exception(db_err) if SentryConfig.SENTRY_DSN else None; return jsonify({"error": "DB error"}), 500
    except Exception as e: logger.error(f"Unexpected error fetching recent restaurants: {e}", exc_info=True); sentry_sdk.capture_exception(e) if SentryConfig.SENTRY_DSN else None; return jsonify({"error": "Unexpected error"}), 500

@app.route('/test-db-connection', methods=['GET'])
def test_db_connection():
    logger.info("Received request for /test-db-connection")
    try:
        with DatabaseConnection() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT 1"); result = cursor.fetchone()
            if result and result[0] == 1: return jsonify({"status": "success", "message": "Database connection successful"})
            else: return jsonify({"status": "error", "message": "DB query failed"}), 500
    except Exception as e: return jsonify({"status": "error", "message": f"DB connection error: {str(e)}"}), 500

@app.route('/trigger-update', methods=['POST'])
def trigger_update():
    logger.info("Received request for /trigger-update")
    try:
        if not update_logic_imported: return jsonify({"status": "error", "message": "Update logic unavailable."}), 500
        provided_key = request.headers.get('X-Update-Secret'); expected_key = APIConfig.UPDATE_SECRET_KEY
        if not expected_key: return jsonify({"status": "error", "message": "Update trigger not configured."}), 500
        if not provided_key or not secrets.compare_digest(provided_key, expected_key): return jsonify({"status": "error", "message": "Unauthorized."}), 403
        threading.Thread(target=run_database_update, daemon=True).start()
        return jsonify({"status": "success", "message": "Database update triggered in background."}), 202
    except Exception as e: logger.error(f"Err in /trigger-update: {e}", exc_info=True); sentry_sdk.capture_exception(e) if SentryConfig.SENTRY_DSN else None; return jsonify({"status": "error"}), 500

# --- Error Handlers ---
@app.errorhandler(404)
def not_found_error_handler(error):
    return jsonify({"error": "Not found"}), 404

@app.errorhandler(500)
def internal_server_error_handler(error):
    logger.error(f"500 Error: {error}", exc_info=True)
    sentry_sdk.capture_exception(error) if SentryConfig.SENTRY_DSN else None
    return jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    host = os.environ.get("HOST", getattr(APIConfig, 'HOST', "0.0.0.0"))
    port = int(os.environ.get("PORT", getattr(APIConfig, 'PORT', 8080)))
    debug_mode = getattr(APIConfig, 'DEBUG', False)
    app.run(host=host, port=port, debug=debug_mode)

logger.info("app_search.py: Module loaded completely.")
