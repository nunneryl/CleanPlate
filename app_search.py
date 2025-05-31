# app_search.py - Final v3 with Aggressive Normalization and websearch_to_tsquery

import os
import re # Ensure re is imported
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
    class DatabaseConnection: # Dummy
        def __enter__(self): return None
        def __exit__(self, type, value, traceback): pass
    def get_redis_client(): return None # Dummy

try:
    from config import APIConfig, SentryConfig
    logging.info("Imported config successfully.")
except ImportError:
    logging.critical("FAILED to import config. Using placeholder classes.")
    class APIConfig: # Dummy
        DEBUG = False
        UPDATE_SECRET_KEY = "default_secret_key_please_change" # Provide a default for safety
        HOST = "0.0.0.0"
        PORT = 8080 # Default port
    class SentryConfig: # Dummy
        SENTRY_DSN = None

try:
    # This import is for the /trigger-update endpoint
    from update_database import run_database_update
    update_logic_imported = True
    logging.info("Imported run_database_update successfully.")
except ImportError:
    logging.error("FAILED to import run_database_update. /trigger-update will not work.")
    update_logic_imported = False
    def run_database_update(days_back=5): # Dummy function
         logging.error("DUMMY run_database_update called - real function failed to import.")


# --- Sentry Initialization ---
if SentryConfig.SENTRY_DSN:
    try:
        sentry_sdk.init(
            dsn=SentryConfig.SENTRY_DSN,
            integrations=[FlaskIntegration()],
            traces_sample_rate=1.0,
            environment="development" if APIConfig.DEBUG else "production",
        )
        logging.info("Sentry initialized successfully.")
    except Exception as e:
         logging.error(f"Failed to initialize Sentry: {e}")
else:
    logging.warning("SENTRY_DSN not set in config, Sentry not initialized.")
# --- End Sentry Initialization ---

# --- Logging Setup ---
# Configure logging ONCE here for the entire application
# Ensure this runs before any loggers are retrieved if not already configured.
if not logging.getLogger().hasHandlers():
    logging.basicConfig(
        level=logging.INFO if not APIConfig.DEBUG else logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        force=True # May require Python 3.8+ to override other default configs
    )
logger = logging.getLogger(__name__) # Get logger for this module
logger.info("Logging configured for app_search.py.")
# --- End Logging Setup ---

# --- Flask App Initialization ---
app = Flask(__name__)
CORS(app) # Enable CORS for all origins by default
logger.info("Flask app created and CORS enabled.")
# --- End Flask App Initialization ---

# --- AGGRESSIVE NORMALIZATION FUNCTION (Must match update_database.py) ---
def normalize_text(text):
    """
    Aggressively normalizes text by lowercasing, removing specific punctuation
    and all spaces to create a single tokenizable string for FTS query input.
    """
    if not isinstance(text, str):
        return '' # Return empty string for non-string input
    text = text.lower()
    # Replace apostrophes, periods with nothing (remove them entirely)
    # Replace ampersand with 'and' (though spaces will be removed later)
    text = text.replace("'", "").replace(".", "").replace('&', 'and')
    # Remove all other non-alphanumeric characters (except spaces initially)
    text = re.sub(r'[^\w\s]', '', text)
    # NOW, remove all spaces to join words into a single string for FTS
    text = re.sub(r'\s+', '', text).strip()
    return text
# --- END AGGRESSIVE NORMALIZATION FUNCTION ---


# --- API Routes ---

@app.route('/', methods=['GET'])
def root():
    logger.info("Root endpoint / called")
    return jsonify({"status": "ok", "message": "CleanPlate API is running"})

@app.route('/search', methods=['GET'])
def search():
    search_term_from_user = request.args.get('name', '').strip()
    if not search_term_from_user:
        logger.warning("Search request: empty 'name' parameter.")
        return jsonify({"error": "Search term is empty", "status": "error"}), 400

    # Normalize the user's input using the aggressive normalization
    normalized_user_input_for_query = normalize_text(search_term_from_user)
    
    if not normalized_user_input_for_query:
        logger.info(f"Search term '{search_term_from_user}' became empty after normalization.")
        return jsonify([]) # Return empty list if normalization results in empty string

    # Use the aggressively normalized input for the cache key for consistency
    cache_key = f"search_v5_agg:{normalized_user_input_for_query}" # v5 for aggressive normalization
    CACHE_TTL_SECONDS = 3600 * 4 # 4 hours cache

    redis_conn = get_redis_client()
    if redis_conn:
        try:
            cached_result_str = redis_conn.get(cache_key)
            if cached_result_str:
                logger.info(f"Cache hit for aggressive normalized search: '{normalized_user_input_for_query}' (key: {cache_key})")
                return jsonify(json.loads(cached_result_str))
            else:
                 logger.info(f"Cache miss for aggressive normalized search: '{normalized_user_input_for_query}' (key: {cache_key})")
        except redis.exceptions.RedisError as redis_err:
             logger.error(f"Redis GET error for {cache_key}: {redis_err}")
             sentry_sdk.capture_exception(redis_err)
        except Exception as e:
             logger.error(f"Unexpected error during Redis GET or JSON decode for {cache_key}: {e}")
             sentry_sdk.capture_exception(e)
    else:
        logger.warning("Redis client unavailable, skipping cache check for search.")

    # --- Database Query Logic (Using websearch_to_tsquery with aggressively normalized input) ---
    logger.info(f"DB query using websearch_to_tsquery for Python-normalized input: '{normalized_user_input_for_query}'")

    # websearch_to_tsquery is good at interpreting user-like search strings.
    # It will handle 'AND'ing terms and can do prefix matching if the input implies it.
    # Example: "xi an famous" -> websearch_to_tsquery will create something like 'xi' & 'an' & 'famous':*
    # Our Python normalize_text turns "xi an famous" into "xianfamous".
    # So we pass "xianfamous" to websearch_to_tsquery.
    # websearch_to_tsquery('english', 'xianfamous') will look for documents containing 'xianfamous':*
    
    query = """
        SELECT
            r.camis, r.dba, r.boro, r.building, r.street, r.zipcode, r.phone,
            r.latitude, r.longitude, r.inspection_date, r.critical_flag, r.grade,
            r.inspection_type, v.violation_code, v.violation_description, r.cuisine_description,
            -- Ranking based on how well the document matches the query
            ts_rank_cd(r.dba_tsv, websearch_to_tsquery('english', %s)) AS rank
        FROM restaurants r
        LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date
        WHERE r.dba_tsv @@ websearch_to_tsquery('english', %s) -- Match against the tsvector column
        ORDER BY rank DESC, r.dba ASC, r.inspection_date DESC -- Order by relevance, then name, then date
        LIMIT 100; -- Limit results for performance and usability
    """
    # Pass the Python-normalized user input directly to websearch_to_tsquery
    params = (normalized_user_input_for_query, normalized_user_input_for_query)

    db_results_raw = None
    try:
        with DatabaseConnection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor: # Use DictCursor
                cursor.execute(query, params)
                db_results_raw = cursor.fetchall()
    except psycopg2.Error as db_err:
        logger.error(f"DB error during websearch FTS for input '{normalized_user_input_for_query}': {db_err}", exc_info=True)
        sentry_sdk.capture_exception(db_err)
        return jsonify({"error": "Database query failed"}), 500
    except Exception as e:
        logger.error(f"Unexpected DB error during websearch FTS for input '{normalized_user_input_for_query}': {e}", exc_info=True)
        sentry_sdk.capture_exception(e)
        return jsonify({"error": "An unexpected error occurred during search"}), 500

    if not db_results_raw:
        logger.info(f"No DB results for websearch FTS with input: '{normalized_user_input_for_query}'")
        if redis_conn:
            try:
                redis_conn.setex(cache_key, 60 * 15, json.dumps([])) # Cache empty result for 15 mins
            except redis.exceptions.RedisError as redis_err:
                 logger.error(f"Redis SETEX error for empty key {cache_key}: {redis_err}")
        return jsonify([])
    
    db_results = [dict(row) for row in db_results_raw] # Convert DictRow to dict

    # --- Process Results (Adapted from previous versions) ---
    restaurant_dict = {}
    for row_dict in db_results:
        camis = row_dict.get('camis')
        if not camis: continue

        if camis not in restaurant_dict:
            restaurant_dict[camis] = {
                k: v for k, v in row_dict.items() if k not in [
                    'rank', 'violation_code', 'violation_description',
                    'inspection_date', 'critical_flag', 'grade', 'inspection_type'
                ]
            }
            restaurant_dict[camis]['inspections'] = {}
        
        inspection_date_obj = row_dict.get('inspection_date')
        if inspection_date_obj:
            inspection_date_str = inspection_date_obj.isoformat()
            if inspection_date_str not in restaurant_dict[camis]['inspections']:
                restaurant_dict[camis]['inspections'][inspection_date_str] = {
                    'inspection_date': inspection_date_str,
                    'critical_flag': row_dict.get('critical_flag'),
                    'grade': row_dict.get('grade'),
                    'inspection_type': row_dict.get('inspection_type'),
                    'violations': []
                }
            if row_dict.get('violation_code'):
                violation = {
                    'violation_code': row_dict.get('violation_code'),
                    'violation_description': row_dict.get('violation_description')
                }
                if violation not in restaurant_dict[camis]['inspections'][inspection_date_str]['violations']:
                    restaurant_dict[camis]['inspections'][inspection_date_str]['violations'].append(violation)

    formatted_results = []
    for restaurant_data_val in restaurant_dict.values(): # Renamed to avoid conflict
        restaurant_data_val['inspections'] = list(restaurant_data_val['inspections'].values())
        formatted_results.append(restaurant_data_val)
    # --- End Process Results ---

    if redis_conn:
        try:
            serialized_data = json.dumps(formatted_results, default=str) # default=str for date/datetime
            redis_conn.setex(cache_key, CACHE_TTL_SECONDS, serialized_data)
            logger.info(f"Stored websearch result in cache: {cache_key}")
        except redis.exceptions.RedisError as redis_err:
            logger.error(f"Redis SETEX error cache key {cache_key}: {redis_err}")
            sentry_sdk.capture_exception(redis_err)
        except TypeError as json_err:
            logger.error(f"Error serializing results JSON for websearch {cache_key}: {json_err}")
            sentry_sdk.capture_exception(json_err)

    logger.info(f"Websearch FTS for user input '{search_term_from_user}' (normalized to '{normalized_user_input_for_query}') OK, returning {len(formatted_results)} restaurants.")
    return jsonify(formatted_results)


@app.route('/recent', methods=['GET'])
def recent_restaurants():
    """ Fetches recently graded (A/B/C) restaurants. """
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
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(query, (days,))
                results = [dict(row) for row in cursor.fetchall()]
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
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                if result and result[0] == 1:
                    return jsonify({"status": "success", "message": "Database connection successful"})
                else:
                    return jsonify({"status": "error", "message": "DB query failed"}), 500
    except Exception as e:
        sentry_sdk.capture_exception(e)
        return jsonify({"status": "error", "message": f"DB connection error: {str(e)}"}), 500


@app.route('/trigger-update', methods=['POST'])
def trigger_update():
    logger.info("Received request for /trigger-update")
    if not update_logic_imported:
        logger.error("Update logic (run_database_update) not available.")
        return jsonify({"status": "error", "message": "Update logic unavailable."}), 500

    provided_key = request.headers.get('X-Update-Secret')
    expected_key = APIConfig.UPDATE_SECRET_KEY

    if not expected_key:
        logger.error("UPDATE_SECRET_KEY not configured on the server.")
        return jsonify({"status": "error", "message": "Update trigger not configured."}), 500

    if not provided_key or not secrets.compare_digest(provided_key, expected_key):
        logger.warning("Invalid/missing secret key for /trigger-update.")
        return jsonify({"status": "error", "message": "Unauthorized."}), 403

    logger.info("Secret key validated. Triggering update in background.")
    try:
        update_thread = threading.Thread(target=run_database_update, daemon=True) # Pass default days_back
        update_thread.start()
    except Exception as e:
        logger.error(f"Failed to start update thread: {e}", exc_info=True)
        sentry_sdk.capture_exception(e)
        return jsonify({"status": "error", "message": "Failed to start update process."}), 500

    return jsonify({"status": "success", "message": "Database update triggered."}), 202


# --- Error Handlers ---
@app.errorhandler(404)
def not_found_error_handler(error): # Renamed parameter
    logger.warning(f"404 Not Found error for URL: {request.url}")
    return jsonify({"error": "The requested resource was not found", "status": "error"}), 404

@app.errorhandler(500)
def internal_server_error_handler(error): # Renamed parameter
    logger.error(f"500 Internal Server Error for {request.url}: {error}", exc_info=True)
    sentry_sdk.capture_exception(error)
    return jsonify({"error": "An internal server error occurred", "status": "error"}), 500

# --- Main Execution Block ---
if __name__ == "__main__":
    # Use APIConfig for host and port, with fallback to os.environ.get for Railway/containerized environments
    host = os.environ.get("HOST", getattr(APIConfig, 'HOST', "0.0.0.0"))
    port = int(os.environ.get("PORT", getattr(APIConfig, 'PORT', 8080)))
    debug_mode = getattr(APIConfig, 'DEBUG', False)
    
    logger.info(f"Starting Flask app via __main__ on {host}:{port} with DEBUG={debug_mode}")
    app.run(host=host, port=port, debug=debug_mode)

logger.info("app_search.py: Module loaded by Python interpreter.")
