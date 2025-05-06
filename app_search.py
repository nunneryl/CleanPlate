# app_search.py - Full Version with Debug Logging

# --- ADD PRINT HERE ---
print("--- PRINT DEBUG: app_search.py: TOP LEVEL ---", flush=True)
# ----------------------

# Standard library imports
import os
import re
import logging
import json
import threading
import secrets
import sys # Import sys for explicit flushing

# --- ADD PRINT HERE ---
print("--- PRINT DEBUG: app_search.py: Imports done ---", flush=True)
# ----------------------


# Third-party imports
import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration
from flask import Flask, jsonify, request, make_response
from flask_cors import CORS
import psycopg2 # Import psycopg2 for specific DB error handling if needed
import redis # Import redis for specific Redis error handling

# --- ADD PRINT HERE ---
print("--- PRINT DEBUG: app_search.py: Third-party imports done ---", flush=True)
# ----------------------


# Local application imports
try:
    from db_manager import DatabaseConnection, redis_client
    print("--- PRINT DEBUG: app_search.py: Imported db_manager ---", flush=True)
except ImportError as e:
    print(f"--- PRINT DEBUG: app_search.py: FAILED to import db_manager: {e} ---", flush=True)
    # Set dummy values if import fails to prevent later NameErrors potentially
    DatabaseConnection = None
    redis_client = None

try:
    # Import all config classes
    from config import APIConfig, SentryConfig, DatabaseConfig, RedisConfig
    print("--- PRINT DEBUG: app_search.py: Imported config ---", flush=True)
except ImportError as e:
    print(f"--- PRINT DEBUG: app_search.py: FAILED to import config: {e} ---", flush=True)
    # Define dummy classes if import fails
    class APIConfig: DEBUG = False; UPDATE_SECRET_KEY = None
    class SentryConfig: SENTRY_DSN = None
    class DatabaseConfig: pass
    class RedisConfig: pass


try:
    # Import the update function
    from update_database import run_database_update
    update_logic_imported = True
    print("--- PRINT DEBUG: app_search.py: Imported run_database_update ---", flush=True)
except ImportError as e:
    print(f"--- PRINT DEBUG: app_search.py: FAILED to import run_database_update: {e} ---", flush=True)
    update_logic_imported = False
    def run_database_update():
         print("--- PRINT DEBUG: app_search.py: DUMMY run_database_update called ---", flush=True)


# --- Sentry Initialization ---
if SentryConfig.SENTRY_DSN:
    try:
        sentry_sdk.init(
            dsn=SentryConfig.SENTRY_DSN, integrations=[FlaskIntegration()],
            traces_sample_rate=1.0, environment="development" if APIConfig.DEBUG else "production",
        )
        print("--- PRINT DEBUG: Sentry initialized ---", flush=True)
    except Exception as e:
         print(f"--- PRINT DEBUG: Failed to initialize Sentry: {e} ---", flush=True)
else:
    print("--- PRINT DEBUG: SENTRY_DSN not set, Sentry not initialized. ---", flush=True)
# --- End Sentry Initialization ---

# --- Logging Setup ---
# Configure logging ONCE here for the entire application
logging.basicConfig(
    level=logging.INFO if not APIConfig.DEBUG else logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True # Reconfigure logging if already set
)
logger = logging.getLogger(__name__) # Get logger for this module
print("--- PRINT DEBUG: Logging configured ---", flush=True)
# --- End Logging Setup ---

# --- Flask App Initialization ---
app = Flask(__name__)
CORS(app) # Enable CORS for all origins
print("--- PRINT DEBUG: Flask app created ---", flush=True)
# --- End Flask App Initialization ---

# --- ADDED: before_request hook for debugging ---
@app.before_request
def log_request_info():
    # Log basic info for ALL incoming requests before they hit the route handler
    # Use logger first, then print as backup
    try:
        # Log headers as well for debugging the trigger request
        logger.info(f"--- before_request: Method={request.method}, Path={request.path}, Headers={request.headers} ---")
        print(f"--- PRINT DEBUG: before_request: Method={request.method}, Path={request.path} ---", flush=True)
    except Exception as e:
        # Log if there's an error just trying to log the request
        logger.error(f"--- before_request: Error logging request info: {e} ---")
        print(f"--- PRINT DEBUG: before_request: Error logging request info: {e} ---", flush=True)
# --- END ADDED: before_request hook ---


# --- Helper Functions ---
def sanitize_input(input_str):
    """ Sanitizes the input string for search. """
    if not input_str: return "", ""
    input_str = input_str.replace("’", "'").replace("‘", "'")
    no_periods_version = input_str.replace(".", "")
    sanitized_input = re.sub(r"[^\w\s']", "", input_str)
    no_periods_sanitized = re.sub(r"[^\w\s']", "", no_periods_version)
    return sanitized_input, no_periods_sanitized
# --- End Helper Functions ---


# --- API Routes ---

@app.route('/', methods=['GET'])
def root():
    print("--- PRINT DEBUG: Received request for / route ---", flush=True)
    logger.info("--- app_search.py: Received request for / route ---")
    return jsonify({"status": "ok", "message": "CleanPlate API is running"})

@app.route('/search', methods=['GET'])
def search():
    """ Searches restaurants, uses cache, retries DB on specific errors. """
    print("--- PRINT DEBUG: Received request for /search ---", flush=True)
    logger.info("--- app_search.py: Received request for /search ---")
    name = request.args.get('name', '').strip()
    if not name: logger.warning("Search request received with empty name parameter."); return jsonify({"error": "Search term is empty", "status": "error"}), 400
    normalized_name_for_key = name.replace("’", "'").replace("‘", "'").lower().strip()
    cache_key = f"search:{normalized_name_for_key}"; CACHE_TTL_SECONDS = 3600 * 4
    if redis_client:
        try:
            cached_result_str = redis_client.get(cache_key)
            if cached_result_str:
                logger.info(f"Cache hit for search: '{name}'"); return jsonify(json.loads(cached_result_str))
            else: logger.info(f"Cache miss for search: '{name}'")
        except redis.exceptions.TimeoutError: logger.warning(f"Redis timeout GET for {cache_key}.")
        except redis.exceptions.RedisError as redis_err: logger.error(f"Redis GET error for {cache_key}: {redis_err}"); sentry_sdk.capture_exception(redis_err)
        except Exception as e: logger.error(f"Unexpected error Redis GET {cache_key}: {e}"); sentry_sdk.capture_exception(e)
    else: logger.warning("Redis client unavailable, skipping cache check.")
    logger.info(f"DB query for search: '{name}'")
    name_with_periods, name_without_periods = sanitize_input(name)
    if '.' not in name and len(name_without_periods) >= 2: name_with_added_periods = '.'.join(list(name_without_periods))
    else: name_with_added_periods = name_with_periods
    transformed_name = name_with_periods.replace("s", "'s"); transformed_name_no_periods = name_without_periods.replace("s", "'s"); transformed_with_added_periods = name_with_added_periods.replace("s", "'s")
    query = """ SELECT r.camis, r.dba, r.boro, r.building, r.street, r.zipcode, r.phone, r.latitude, r.longitude, r.inspection_date, r.critical_flag, r.grade, r.inspection_type, v.violation_code, v.violation_description, r.cuisine_description FROM restaurants r LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date WHERE r.dba ILIKE %s OR r.dba ILIKE %s OR r.dba ILIKE %s OR r.dba ILIKE %s OR r.dba ILIKE %s ORDER BY CASE WHEN UPPER(r.dba) = UPPER(%s) THEN 0 WHEN UPPER(r.dba) = UPPER(%s) THEN 1 WHEN UPPER(r.dba) LIKE UPPER(%s) THEN 2 WHEN UPPER(r.dba) LIKE UPPER(%s) THEN 3 WHEN UPPER(r.dba) LIKE UPPER(%s) THEN 4 ELSE 5 END, r.dba, r.inspection_date DESC """
    where_params = [ f"%{p}%" for p in [name_with_periods, transformed_name, name_without_periods, transformed_name_no_periods, name_with_added_periods] ]; order_params = [ name_with_periods, name_without_periods, f"{name_with_periods}%", f"{name_without_periods}%", f"{name_with_added_periods}%" ]; params = where_params + order_params
    MAX_DB_RETRIES = 1; db_results = None; last_db_error = None
    for attempt in range(MAX_DB_RETRIES + 1):
        try:
            with DatabaseConnection() as conn:
                with conn.cursor() as cursor:
                    logger.debug(f"Attempt {attempt + 1}: Executing search query"); cursor.execute(query, params); db_results = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]; logger.debug(f"Attempt {attempt + 1}: DB query OK, {len(db_results) if db_results else 0} rows.")
                    last_db_error = None; break
        except psycopg2.OperationalError as op_err:
            last_db_error = op_err; logger.warning(f"Attempt {attempt + 1}: DB OperationalError search '{name}': {op_err}"); sentry_sdk.capture_exception(op_err)
            if attempt < MAX_DB_RETRIES: logger.info(f"Retrying DB query (attempt {attempt + 2})..."); continue
            else: logger.error(f"DB query failed after {MAX_DB_RETRIES + 1} attempts for '{name}'."); raise op_err
        except psycopg2.Error as db_err: last_db_error = db_err; logger.error(f"Attempt {attempt + 1}: Non-op DB error search '{name}': {db_err}"); sentry_sdk.capture_exception(db_err); raise db_err
        except Exception as e: last_db_error = e; logger.error(f"Attempt {attempt + 1}: Unexpected DB error search '{name}': {e}", exc_info=True); sentry_sdk.capture_exception(e); raise e
    if db_results is None and last_db_error is not None: logger.error(f"Exited DB loop due to error: {last_db_error}"); raise last_db_error
    if db_results is None: logger.error("db_results is None after loop without error."); raise Exception("Failed to retrieve results.")
    if not db_results:
        logger.info(f"No DB results for search: {name}")
        if redis_client:
            try: redis_client.setex(cache_key, 60 * 15, json.dumps([])); logger.info(f"Cached empty result for key: {cache_key}")
            except redis.exceptions.RedisError as redis_err: logger.error(f"Redis SETEX error empty key {cache_key}: {redis_err}"); sentry_sdk.capture_exception(redis_err)
        return jsonify([])
    logger.debug("Processing DB results..."); restaurant_dict = {}
    for row in db_results:
        restaurant_data = dict(zip(columns, row)); camis = restaurant_data.get('camis'); inspection_date_obj = restaurant_data.get('inspection_date')
        if not camis: continue
        inspection_date_str = inspection_date_obj.isoformat() if inspection_date_obj else None
        if camis not in restaurant_dict: restaurant_dict[camis] = { "camis": camis, "dba": restaurant_data.get('dba'), "boro": restaurant_data.get('boro'), "building": restaurant_data.get('building'), "street": restaurant_data.get('street'), "zipcode": restaurant_data.get('zipcode'), "phone": restaurant_data.get('phone'), "latitude": restaurant_data.get('latitude'), "longitude": restaurant_data.get('longitude'), "cuisine_description": restaurant_data.get('cuisine_description'), "inspections": {} }
        inspections = restaurant_dict[camis]["inspections"]
        if inspection_date_str and inspection_date_str not in inspections: inspections[inspection_date_str] = { "inspection_date": inspection_date_str, "critical_flag": restaurant_data.get('critical_flag'), "grade": restaurant_data.get('grade'), "inspection_type": restaurant_data.get('inspection_type'), "violations": [] }
        if inspection_date_str and restaurant_data.get('violation_code'):
            violation = { "violation_code": restaurant_data.get('violation_code'), "violation_description": restaurant_data.get('violation_description') }
            if violation not in inspections[inspection_date_str]["violations"]: inspections[inspection_date_str]["violations"].append(violation)
    formatted_results = []
    for restaurant in restaurant_dict.values(): restaurant["inspections"] = list(restaurant["inspections"].values()); formatted_results.append(restaurant)
    logger.debug("Finished processing DB results.")
    if redis_client:
        try:
            serialized_data = json.dumps(formatted_results); redis_client.setex(cache_key, CACHE_TTL_SECONDS, serialized_data); logger.info(f"Stored search result in cache: {cache_key}")
        except redis.exceptions.TimeoutError: logger.warning(f"Redis timeout SETEX for {cache_key}.")
        except redis.exceptions.RedisError as redis_err: logger.error(f"Redis SETEX error cache key {cache_key}: {redis_err}"); sentry_sdk.capture_exception(redis_err)
        except TypeError as json_err: logger.error(f"Error serializing results JSON {cache_key}: {json_err}"); sentry_sdk.capture_exception(json_err)
        except Exception as e: logger.error(f"Unexpected error Redis SETEX {cache_key}: {e}"); sentry_sdk.capture_exception(e)
    logger.info(f"DB search '{name}' OK, returning {len(formatted_results)} restaurants.")
    return jsonify(formatted_results)

@app.route('/recent', methods=['GET'])
def recent_restaurants():
    print("--- PRINT DEBUG: Received request for /recent ---", flush=True)
    logger.info("--- app_search.py: Received request for /recent ---")
    days = request.args.get('days', '7');
    try: days = int(days); days = 7 if days <= 0 else days
    except ValueError: days = 7
    logger.info(f"Fetching recent restaurants (graded A/B/C) from past {days} days.")
    query = """ SELECT DISTINCT ON (r.camis) r.camis, r.dba, r.boro, r.building, r.street, r.zipcode, r.phone, r.latitude, r.longitude, r.grade, r.inspection_date, r.cuisine_description FROM restaurants r WHERE r.grade IN ('A', 'B', 'C') AND r.inspection_date >= (CURRENT_DATE - INTERVAL '%s days') ORDER BY r.camis, r.inspection_date DESC LIMIT 50 """
    try:
        with DatabaseConnection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (days,)); results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                formatted_results = [dict(zip(columns, row)) for row in results]
                logger.info(f"Recent restaurants fetch returned {len(formatted_results)} results.")
                return jsonify(formatted_results)
    except psycopg2.Error as db_err: logger.error(f"Error fetching recent restaurants: {db_err}"); sentry_sdk.capture_exception(db_err); raise
    except Exception as e: logger.error(f"Unexpected error fetching recent restaurants: {e}", exc_info=True); sentry_sdk.capture_exception(e); raise

@app.route('/test-db-connection', methods=['GET'])
def test_db_connection():
    print("--- PRINT DEBUG: Received request for /test-db-connection ---", flush=True)
    logger.info("--- app_search.py: Received request for /test-db-connection ---")
    try:
        with DatabaseConnection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1"); result = cursor.fetchone()
                if result and result[0] == 1: logger.info("Database connection test successful."); return jsonify({"status": "success", "message": "Database connection successful"})
                else: logger.error("DB connection test failed: Query returned unexpected result."); return jsonify({"status": "error", "message": "DB query failed"}), 500
    except Exception as e: logger.error(f"Database connection test failed: {e}"); return jsonify({"status": "error", "message": f"DB connection error: {str(e)}"}), 500


@app.route('/trigger-update', methods=['POST'])
def trigger_update():
    # --- ADDED TOP-LEVEL TRY/EXCEPT ---
    try:
        print(f"--- PRINT DEBUG: /trigger-update: ENTERING TRY BLOCK ---", flush=True)
        logger.info("--- /trigger-update: Request received (Top of function) ---")

        if not update_logic_imported:
            print("--- PRINT DEBUG: /trigger-update: Update logic unavailable ---", flush=True)
            logger.error("Update logic unavailable."); return jsonify({"status": "error", "message": "Update logic unavailable."}), 500

        provided_key = request.headers.get('X-Update-Secret')
        expected_key = APIConfig.UPDATE_SECRET_KEY
        print(f"--- PRINT DEBUG: /trigger-update: Provided Key: {'Exists' if provided_key else 'Missing'}, Expected Key: {'Exists' if expected_key else 'Missing'} ---", flush=True)


        if not expected_key:
            print("--- PRINT DEBUG: /trigger-update: Expected key not configured ---", flush=True)
            logger.error("UPDATE_SECRET_KEY not configured."); return jsonify({"status": "error", "message": "Update trigger not configured."}), 500

        # Use a variable to store comparison result for logging
        keys_match = provided_key and secrets.compare_digest(provided_key, expected_key)
        print(f"--- PRINT DEBUG: /trigger-update: Keys Match: {keys_match} ---", flush=True)

        if not keys_match:
            print("--- PRINT DEBUG: /trigger-update: Unauthorized access attempt ---", flush=True)
            logger.warning("Invalid/missing secret key for /trigger-update."); return jsonify({"status": "error", "message": "Unauthorized."}), 403

        print("--- PRINT DEBUG: /trigger-update: Key validated, attempting to start thread ---", flush=True)
        logger.info("Secret key validated. Triggering update in background.")
        try:
            update_thread = threading.Thread(target=run_database_update, daemon=True); update_thread.start()
            print("--- PRINT DEBUG: /trigger-update: Thread started successfully ---", flush=True)
        except Exception as e:
            print(f"--- PRINT DEBUG: /trigger-update: FAILED to start thread: {e} ---", flush=True)
            logger.error(f"Failed to start update thread: {e}", exc_info=True); sentry_sdk.capture_exception(e); return jsonify({"status": "error", "message": "Failed to start update process."}), 500

        logger.info("Successfully launched background update thread.")
        print("--- PRINT DEBUG: /trigger-update: Returning 202 Accepted ---", flush=True)
        return jsonify({"status": "success", "message": "Database update triggered in background."}), 202

    except Exception as e:
        # --- CATCH ALL EXCEPTIONS WITHIN THE ROUTE ---
        print(f"--- PRINT DEBUG: /trigger-update: UNCAUGHT EXCEPTION in route handler: {e} ---", flush=True)
        logger.error(f"--- /trigger-update: UNCAUGHT EXCEPTION in route handler: {e} ---", exc_info=True)
        # Optionally send to Sentry here too if Flask's handler might miss it
        # sentry_sdk.capture_exception(e)
        # Return a generic 500 error
        return jsonify({"status": "error", "message": "Internal server error in trigger endpoint."}), 500
    # --- END TOP-LEVEL TRY/EXCEPT ---


# --- Keep Error Handlers ---
@app.errorhandler(404)
def not_found(e):
    """ Handles 404 Not Found errors. """
    print(f"--- PRINT DEBUG: 404 Error Handler triggered for {request.url} ---", flush=True)
    logger.warning(f"404 Not Found error for URL: {request.url}")
    return jsonify({"error": "The requested resource was not found", "status": "error"}), 404

@app.errorhandler(500)
def server_error(e):
    """ Handles 500 Internal Server errors. """
    print(f"--- PRINT DEBUG: 500 Error Handler triggered for {request.url} ---", flush=True)
    logger.error(f"500 Internal Server Error handling request for {request.url}: {e}", exc_info=True)
    return jsonify({"error": "An internal server error occurred", "status": "error"}), 500


# --- Keep Main Execution Block ---
if __name__ == "__main__":
    print("--- PRINT DEBUG: Running locally via __main__ ---", flush=True)
    logger.info(f"--- app_search.py: Running locally via __main__ ---")
    # Use Flask's development server (NOT for production)
    app.run( host=APIConfig.HOST, port=APIConfig.PORT, debug=APIConfig.DEBUG )

print("--- PRINT DEBUG: app_search.py: Module loaded completely ---", flush=True)
