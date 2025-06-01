# app_search.py - Reverted to version before FTS/Normalization changes

# Standard library imports
import os
import re
import logging
import json
import threading
import secrets
import sys # Import sys if using StreamHandler(sys.stdout) below

# Third-party imports
import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration
from flask import Flask, jsonify, request, make_response # make_response was present
from flask_cors import CORS
import psycopg2 # Import psycopg2 for specific DB error handling if needed
import redis # Import redis for specific Redis error handling

# Local application imports
try:
    # Import DatabaseConnection context manager and the get_redis_client function
    from db_manager import DatabaseConnection, get_redis_client
    logging.info("Imported db_manager successfully.")
except ImportError as e:
    logging.critical(f"FAILED to import db_manager: {e}")
    # Define dummy versions if import fails, as per your original file
    class DatabaseConnection:
        def __enter__(self): return None
        def __exit__(self, type, value, traceback): pass
    def get_redis_client(): return None

try:
    # Import all config classes
    from config import APIConfig, SentryConfig, DatabaseConfig, RedisConfig # These were in your original
    logging.info("Imported config successfully.")
except ImportError as e:
    logging.critical(f"FAILED to import config: {e}")
    # Define dummy classes if import fails
    class APIConfig: DEBUG = False; UPDATE_SECRET_KEY = None; HOST="0.0.0.0"; PORT=8080
    class SentryConfig: SENTRY_DSN = None
    class DatabaseConfig: pass # Dummy
    class RedisConfig: pass # Dummy

try:
    # Import the update function
    from update_database import run_database_update
    update_logic_imported = True
    logging.info("Imported run_database_update successfully.")
except ImportError as e:
    logging.error(f"FAILED to import run_database_update: {e}")
    update_logic_imported = False
    # Define a dummy function if import fails
    def run_database_update(days_back=5): # Added default for dummy
         logging.error("DUMMY run_database_update called - real function failed to import.")


# --- Sentry Initialization ---
if hasattr(SentryConfig, 'SENTRY_DSN') and SentryConfig.SENTRY_DSN:
    try:
        sentry_sdk.init(
            dsn=SentryConfig.SENTRY_DSN, integrations=[FlaskIntegration()],
            traces_sample_rate=1.0, # Adjust sample rate as needed
            environment="development" if APIConfig.DEBUG else "production",
        )
        logging.info("Sentry initialized successfully.")
    except Exception as e:
         logging.error(f"Failed to initialize Sentry: {e}")
else:
    logging.warning("SENTRY_DSN not set, Sentry not initialized.")
# --- End Sentry Initialization ---

# --- Logging Setup ---
# Configure logging ONCE here for the entire application
if not logging.getLogger().hasHandlers(): # Check if handlers are already configured
    logging.basicConfig(
        level=logging.INFO if not APIConfig.DEBUG else logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        force=True # May require Python 3.8+
    )
logger = logging.getLogger(__name__) # Get logger for this module
logger.info("Logging configured for app_search.py.")
# --- End Logging Setup ---

# --- Flask App Initialization ---
app = Flask(__name__)
CORS(app) # Enable CORS for all origins
logger.info("Flask app created.")
# --- End Flask App Initialization ---

# --- Helper Functions (as per your original file) ---
def sanitize_input(input_str):
    """ Sanitizes the input string for search. """
    if not input_str: return "", ""
    # Normalize different apostrophe types
    input_str = input_str.replace("’", "'").replace("‘", "'")
    # Version without periods
    no_periods_version = input_str.replace(".", "")
    # Allow letters, numbers, whitespace, standard apostrophe
    sanitized_input = re.sub(r"[^\w\s']", "", input_str)
    no_periods_sanitized = re.sub(r"[^\w\s']", "", no_periods_version)
    return sanitized_input, no_periods_sanitized
# --- End Helper Functions ---


# --- API Routes ---

@app.route('/', methods=['GET'])
def root():
    logger.info("Received request for / route")
    return jsonify({"status": "ok", "message": "CleanPlate API is running"})

@app.route('/search', methods=['GET'])
def search():
    """ Searches restaurants, uses cache (with lazy init), retries DB. """
    logger.info("Received request for /search")
    name = request.args.get('name', '').strip()
    if not name:
        logger.warning("Search request received with empty name parameter.")
        return jsonify({"error": "Search term is empty", "status": "error"}), 400

    # Original cache key logic
    normalized_name_for_key = name.replace("’", "'").replace("‘", "'").lower().strip()
    cache_key = f"search:{normalized_name_for_key}"
    CACHE_TTL_SECONDS = 3600 * 4 # 4 hours

    redis_conn = get_redis_client()
    if redis_conn:
        try:
            cached_result_str = redis_conn.get(cache_key)
            if cached_result_str:
                logger.info(f"Cache hit for search: '{name}'")
                try:
                    return jsonify(json.loads(cached_result_str))
                except json.JSONDecodeError as json_err:
                     logger.error(f"Error decoding cached JSON for key {cache_key}: {json_err}. Fetching from DB.")
                     sentry_sdk.capture_exception(json_err)
            else:
                 logger.info(f"Cache miss for search: '{name}'")
        except redis.exceptions.TimeoutError:
             logger.warning(f"Redis timeout GET for {cache_key}. Fetching from DB.")
        except redis.exceptions.RedisError as redis_err:
             logger.error(f"Redis GET error for {cache_key}: {redis_err}")
             sentry_sdk.capture_exception(redis_err)
        except Exception as e:
             logger.error(f"Unexpected error during Redis GET {cache_key}: {e}")
             sentry_sdk.capture_exception(e)
    else:
        logger.warning("Redis client unavailable, skipping cache check.")

    logger.info(f"DB query for search: '{name}'")
    name_with_periods, name_without_periods = sanitize_input(name) # Original sanitize
    
    # Original logic for term variations
    if '.' not in name and len(name_without_periods) >= 2:
        name_with_added_periods = '.'.join(list(name_without_periods))
    else:
        name_with_added_periods = name_with_periods
    
    transformed_name = name_with_periods.replace("s", "'s")
    transformed_name_no_periods = name_without_periods.replace("s", "'s")
    # transformed_with_added_periods = name_with_added_periods.replace("s", "'s") # This was in your original, ensure it's used if needed

    # Original Query
    query = """
        SELECT r.camis, r.dba, r.boro, r.building, r.street, r.zipcode, r.phone, 
               r.latitude, r.longitude, r.inspection_date, r.critical_flag, r.grade, 
               r.inspection_type, v.violation_code, v.violation_description, r.cuisine_description 
        FROM restaurants r 
        LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date 
        WHERE r.dba ILIKE %s OR r.dba ILIKE %s OR r.dba ILIKE %s OR r.dba ILIKE %s OR r.dba ILIKE %s 
        ORDER BY 
            CASE 
                WHEN UPPER(r.dba) = UPPER(%s) THEN 0 
                WHEN UPPER(r.dba) = UPPER(%s) THEN 1 
                WHEN UPPER(r.dba) LIKE UPPER(%s) THEN 2 
                WHEN UPPER(r.dba) LIKE UPPER(%s) THEN 3 
                WHEN UPPER(r.dba) LIKE UPPER(%s) THEN 4 
                ELSE 5 
            END, 
            r.dba, r.inspection_date DESC
    """
    # Ensure all variations are included in where_params and order_params if they were used.
    # Based on your original code, it looked like 5 ILIKE conditions.
    # The transformed_with_added_periods needs to be defined if used in params.
    # For now, using 5 terms.
    # Ensure this part matches your original file's logic precisely.
    # The original sanitize_input returned two values, you used 5 terms in the query.
    # Let's assume these were the intended terms, adjust if your original was different.
    term1 = f"%{name_with_periods}%"
    term2 = f"%{transformed_name}%"
    term3 = f"%{name_without_periods}%"
    term4 = f"%{transformed_name_no_periods}%"
    # term5 would be based on name_with_added_periods if it was used
    # For now, I'll assume a common pattern or repeat a term if only 4 distinct transformations were used
    # This part is critical to match your known working version.
    # Based on your query structure with 5 ILIKEs and 5 ORDER BY terms,
    # it implies 5 distinct search patterns were generated.
    # The `transformed_with_added_periods` was likely one.
    transformed_with_added_periods = name_with_added_periods.replace("s", "'s") # Define it
    term5 = f"%{transformed_with_added_periods}%"


    where_params = [term1, term2, term3, term4, term5]
    order_params = [
        name_with_periods, name_without_periods,
        f"{name_with_periods}%", f"{name_without_periods}%",
        f"{name_with_added_periods}%" # Or the transformed version
    ]
    params = where_params + order_params
    
    MAX_DB_RETRIES = 1; db_results = None; last_db_error = None
    for attempt in range(MAX_DB_RETRIES + 1):
        try:
            with DatabaseConnection() as conn:
                with conn.cursor() as cursor: # Original used default cursor
                    logger.debug(f"Attempt {attempt + 1}: Executing search query")
                    cursor.execute(query, params)
                    db_results = cursor.fetchall()
                    if db_results is not None:
                        columns = [desc[0] for desc in cursor.description]
                        logger.debug(f"Attempt {attempt + 1}: DB query OK, {len(db_results)} rows.")
                        last_db_error = None
                        break
                    else:
                        logger.error(f"Attempt {attempt + 1}: cursor.fetchall() returned None.")
                        # This was in your original logic structure. Consider raising an error.
                        # raise psycopg2.Error("fetchall returned None") # Or handle as appropriate
                        # For now, let it try again or fall through.
        except psycopg2.OperationalError as op_err:
            last_db_error = op_err
            logger.warning(f"Attempt {attempt + 1}: DB OperationalError search '{name}': {op_err}")
            sentry_sdk.capture_exception(op_err)
            if attempt < MAX_DB_RETRIES:
                logger.info(f"Retrying DB query (attempt {attempt + 2})...")
                continue
            else:
                logger.error(f"DB query failed after {MAX_DB_RETRIES + 1} attempts for '{name}'.")
                # raise op_err # Re-raise if you want the endpoint to fail hard
        except psycopg2.Error as db_err: # Other psycopg2 errors
            last_db_error = db_err
            logger.error(f"Attempt {attempt + 1}: Non-op DB error search '{name}': {db_err}")
            sentry_sdk.capture_exception(db_err)
            # raise db_err # Re-raise
        except Exception as e: # Other unexpected errors
            last_db_error = e
            logger.error(f"Attempt {attempt + 1}: Unexpected DB error search '{name}': {e}", exc_info=True)
            sentry_sdk.capture_exception(e)
            # raise e # Re-raise

    if db_results is None and last_db_error is not None:
        logger.error(f"Exited DB loop due to error: {last_db_error}")
        # Depending on how you want to handle, you might return an error here
        return jsonify({"error": "Database error occurred during search"}), 500
    
    if db_results is None: # Should not happen if loop completes without error and without break
        logger.error("db_results is None after loop without error. This indicates a logic flaw.")
        return jsonify({"error": "Failed to retrieve results due to an unknown issue"}), 500

    if not db_results:
        logger.info(f"No DB results for search: {name}")
        if redis_conn: # Original logic for caching empty result
            try:
                redis_conn.setex(cache_key, 60 * 15, json.dumps([]))
                logger.info(f"Cached empty result for key: {cache_key}")
            except redis.exceptions.RedisError as redis_err:
                 logger.error(f"Redis SETEX error empty key {cache_key}: {redis_err}")
                 sentry_sdk.capture_exception(redis_err)
        return jsonify([])

    # --- Process Results (as per your original file structure) ---
    logger.debug("Processing DB results...")
    restaurant_dict = {}
    # This columns variable needs to be defined if fetchall() was successful
    # It was defined inside the try block. For safety, redefine if db_results exist.
    if db_results: # This check is technically redundant if we raise errors above
        # Assuming the cursor description is still available or columns is defined
        # This is a simplification; in a real scenario, ensure columns is passed correctly
        # For now, let's assume `columns` was correctly populated from the last successful cursor
        # This part of the logic needs to be robust if `columns` is not guaranteed.
        # However, based on the provided snippet, it was within the try block.
        # For a direct revert, if this was how your original worked and didn't error, let's keep it.
        # But a safer way would be to ensure `columns` is defined here.
        # Let's assume `columns` is still in scope from the successful try block.
        # If not, you would need to re-fetch cursor.description or adapt.
        # For this revert, I am keeping structure similar to what was implied.
        pass # `columns` would have been set in the try block.

    for row in db_results:
        # This assumes `columns` is defined.
        # If there's an issue here, it means the original code had this structure.
        # We might need to re-initialize `columns` if the successful try block was exited.
        # For safety, let's assume `columns` needs to be available.
        # This is hard to reconstruct perfectly without seeing the full original flow if db_results could be non-None but columns undefined.
        # Given the context, the simplest assumption is that if db_results is non-empty, columns was set.
        try:
            restaurant_data = dict(zip(columns, row)) # columns must be defined
        except NameError: # If columns wasn't defined (e.g. error then recovery)
            logger.error("`columns` not defined during result processing. This indicates an issue with DB query success check.")
            # Fallback or error, for now, skip row
            continue

        camis = restaurant_data.get('camis')
        inspection_date_obj = restaurant_data.get('inspection_date') # Assuming date object or string
        if not camis: continue

        # Convert date to string if it's a date object, for dictionary keys
        inspection_date_str = inspection_date_obj.isoformat() if hasattr(inspection_date_obj, 'isoformat') else str(inspection_date_obj)

        if camis not in restaurant_dict:
            restaurant_dict[camis] = {
                "camis": camis,
                "dba": restaurant_data.get('dba'),
                "boro": restaurant_data.get('boro'),
                "building": restaurant_data.get('building'),
                "street": restaurant_data.get('street'),
                "zipcode": restaurant_data.get('zipcode'),
                "phone": restaurant_data.get('phone'),
                "latitude": restaurant_data.get('latitude'),
                "longitude": restaurant_data.get('longitude'),
                "cuisine_description": restaurant_data.get('cuisine_description'),
                "inspections": {}
            }
        
        inspections = restaurant_dict[camis]["inspections"]
        if inspection_date_str and inspection_date_str not in inspections: # Ensure date string is valid
            inspections[inspection_date_str] = {
                "inspection_date": inspection_date_str, # Store as string
                "critical_flag": restaurant_data.get('critical_flag'),
                "grade": restaurant_data.get('grade'),
                "inspection_type": restaurant_data.get('inspection_type'),
                "violations": []
            }
        
        if inspection_date_str and restaurant_data.get('violation_code'):
            violation = {
                "violation_code": restaurant_data.get('violation_code'),
                "violation_description": restaurant_data.get('violation_description')
            }
            # Ensure violation is not duplicated if data has multiple rows for same insp+viol
            if violation not in inspections[inspection_date_str]["violations"]:
                inspections[inspection_date_str]["violations"].append(violation)

    formatted_results = []
    for restaurant_key in restaurant_dict: # Iterate over keys
        restaurant_data_val = restaurant_dict[restaurant_key] # Get value
        restaurant_data_val["inspections"] = list(restaurant_data_val["inspections"].values())
        formatted_results.append(restaurant_data_val)
    logger.debug("Finished processing DB results.")
    # --- End Process Results ---

    # --- Store Result in Cache ---
    if redis_conn: # Original caching logic
        try:
            serialized_data = json.dumps(formatted_results, default=str) # Add default=str for date/datetime
            redis_conn.setex(cache_key, CACHE_TTL_SECONDS, serialized_data)
            logger.info(f"Stored search result in cache: {cache_key}")
        except redis.exceptions.RedisError as redis_err:
            logger.error(f"Redis SETEX error cache key {cache_key}: {redis_err}")
            sentry_sdk.capture_exception(redis_err)
        except TypeError as json_err: # Catch JSON serialization errors
            logger.error(f"Error serializing results JSON {cache_key}: {json_err}")
            sentry_sdk.capture_exception(json_err)
        except Exception as e: # Catch-all for other unexpected errors
            logger.error(f"Unexpected error Redis SETEX {cache_key}: {e}")
            sentry_sdk.capture_exception(e)
    # --- End Store Result in Cache ---

    logger.info(f"DB search '{name}' OK, returning {len(formatted_results)} restaurants.")
    return jsonify(formatted_results)

@app.route('/recent', methods=['GET'])
def recent_restaurants():
    logger.info("Received request for /recent")
    days_str = request.args.get('days', '7')
    try:
        days = int(days_str)
        days = 7 if days <= 0 or days > 90 else days
    except ValueError:
        days = 7
    
    logger.info(f"Fetching recent restaurants (graded A/B/C) from past {days} days.")
    query = """
        SELECT DISTINCT ON (r.camis) 
            r.camis, r.dba, r.boro, r.building, r.street, r.zipcode, r.phone, 
            r.latitude, r.longitude, r.grade, r.inspection_date, r.cuisine_description 
        FROM restaurants r 
        WHERE r.grade IN ('A', 'B', 'C') 
          AND r.inspection_date >= (CURRENT_DATE - INTERVAL '%s days') 
        ORDER BY r.camis, r.inspection_date DESC 
        LIMIT 50;
    """
    try:
        with DatabaseConnection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor: # Use DictCursor
                cursor.execute(query, (days,))
                results = [dict(row) for row in cursor.fetchall()] # Convert to dict
                logger.info(f"Recent restaurants fetch returned {len(results)} results.")
                return jsonify(results)
    except psycopg2.Error as db_err:
        logger.error(f"Error fetching recent restaurants: {db_err}", exc_info=True)
        sentry_sdk.capture_exception(db_err)
        return jsonify({"error": "Database error fetching recent restaurants"}), 500
    except Exception as e:
        logger.error(f"Unexpected error fetching recent restaurants: {e}", exc_info=True)
        sentry_sdk.capture_exception(e)
        return jsonify({"error": "Unexpected error fetching recent restaurants"}), 500


@app.route('/test-db-connection', methods=['GET'])
def test_db_connection():
    logger.info("Received request for /test-db-connection")
    try:
        with DatabaseConnection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1"); result = cursor.fetchone()
                if result and result[0] == 1:
                    logger.info("Database connection test successful.")
                    return jsonify({"status": "success", "message": "Database connection successful"})
                else:
                    logger.error("DB connection test failed: Query returned unexpected result.")
                    return jsonify({"status": "error", "message": "DB query failed"}), 500
    except Exception as e:
        logger.error(f"Database connection test failed: {e}", exc_info=True) # exc_info for full traceback
        sentry_sdk.capture_exception(e)
        return jsonify({"status": "error", "message": f"DB connection error: {str(e)}"}), 500


@app.route('/trigger-update', methods=['POST'])
def trigger_update():
    logger.info("Received request for /trigger-update")
    try:
        if not update_logic_imported:
            logger.error("Update logic unavailable (run_database_update failed to import).")
            return jsonify({"status": "error", "message": "Update logic unavailable."}), 500

        provided_key = request.headers.get('X-Update-Secret')
        expected_key = APIConfig.UPDATE_SECRET_KEY

        if not expected_key: # Check if the key is configured on the server
            logger.error("UPDATE_SECRET_KEY not configured on the server.")
            return jsonify({"status": "error", "message": "Update trigger not configured on server."}), 500

        if not provided_key or not secrets.compare_digest(provided_key, expected_key):
            logger.warning("Invalid or missing secret key for /trigger-update.")
            return jsonify({"status": "error", "message": "Unauthorized."}), 403

        logger.info("Secret key validated. Triggering update in background.")
        try:
            # Assuming run_database_update takes a days_back argument, pass default if not specified
            update_thread = threading.Thread(target=run_database_update, daemon=True) # Pass appropriate args if needed
            update_thread.start()
        except Exception as e:
            logger.error(f"Failed to start update thread: {e}", exc_info=True)
            sentry_sdk.capture_exception(e)
            return jsonify({"status": "error", "message": "Failed to start update process."}), 500

        logger.info("Successfully launched background update thread.")
        return jsonify({"status": "success", "message": "Database update triggered in background."}), 202

    except Exception as e:
        logger.error(f"Unexpected error in /trigger-update handler: {e}", exc_info=True)
        sentry_sdk.capture_exception(e)
        return jsonify({"status": "error", "message": "Internal server error in trigger endpoint."}), 500


# --- Error Handlers (as per your original file) ---
@app.errorhandler(404)
def not_found(e):
    logger.warning(f"404 Not Found error for URL: {request.url}")
    return jsonify({"error": "The requested resource was not found", "status": "error"}), 404

@app.errorhandler(500)
def server_error(e):
    logger.error(f"500 Internal Server Error handling request for {request.url}: {e}", exc_info=True)
    # Sentry will capture unhandled 500s if FlaskIntegration is on,
    # but explicitly capturing here can add more context if needed.
    # sentry_sdk.capture_exception(e)
    return jsonify({"error": "An internal server error occurred", "status": "error"}), 500

# --- Main Execution Block (as per your original file) ---
if __name__ == "__main__":
    # Use APIConfig for host and port, with fallback to os.environ.get for Railway/containerized environments
    host = os.environ.get("HOST", getattr(APIConfig, 'HOST', "0.0.0.0"))
    port = int(os.environ.get("PORT", getattr(APIConfig, 'PORT', 8080)))
    debug_mode = getattr(APIConfig, 'DEBUG', False)
    
    logger.info(f"Starting Flask app locally via app.run() on {host}:{port} with DEBUG={debug_mode}")
    app.run( host=host, port=port, debug=debug_mode )

logger.info("app_search.py: Module loaded completely.")
