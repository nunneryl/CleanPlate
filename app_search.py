# app_search.py - Final version with websearch_to_tsquery

# Standard library imports
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
from flask import Flask, jsonify, request # Removed make_response as it's not used
from flask_cors import CORS
import psycopg2
import psycopg2.extras # For DictCursor
import redis

# Local application imports
try:
    from db_manager import DatabaseConnection, get_redis_client
    logging.info("Imported db_manager successfully.")
except ImportError as e:
    logging.critical(f"FAILED to import db_manager: {e}")
    DatabaseConnection = None # Define a dummy if import fails
    def get_redis_client(): return None

try:
    from config import APIConfig, SentryConfig # Removed DatabaseConfig, RedisConfig as not directly used here
    logging.info("Imported config successfully.")
except ImportError as e:
    logging.critical(f"FAILED to import config: {e}")
    # Define dummy classes if import fails
    class APIConfig: DEBUG = False; UPDATE_SECRET_KEY = None; HOST = "0.0.0.0"; PORT = 8080
    class SentryConfig: SENTRY_DSN = None

try:
    from update_database import run_database_update # Assuming this is still needed for /trigger-update
    update_logic_imported = True
    logging.info("Imported run_database_update successfully.")
except ImportError as e:
    logging.error(f"FAILED to import run_database_update: {e}")
    update_logic_imported = False
    def run_database_update():
         logging.error("DUMMY run_database_update called - real function failed to import.")

# --- Sentry Initialization ---
if SentryConfig.SENTRY_DSN:
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
logging.basicConfig(
    level=logging.INFO if not APIConfig.DEBUG else logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True # May require Python 3.8+
)
logger = logging.getLogger(__name__) # Get logger for this module
logger.info("Logging configured.")
# --- End Logging Setup ---

# --- Flask App Initialization ---
app = Flask(__name__)
CORS(app) # Enable CORS for all origins
logger.info("Flask app created.")
# --- End Flask App Initialization ---

# --- Helper Functions ---
def normalize_text(text):
    """
    Normalizes text by lowercasing, removing specific punctuation,
    and preparing it for consistent searching or FTS input.
    This MUST match the logic in update_database.py if used for FTS input.
    """
    if not isinstance(text, str):
        return '' # Return empty string for non-string input
    text = text.lower()
    # Replace apostrophes and periods with spaces first
    text = text.replace("'", " ").replace(".", " ")
    # Replace common "and" variations
    text = text.replace('&', ' and ')
    # Remove all other characters that are not letters, numbers, or whitespace.
    text = re.sub(r'[^\w\s]', '', text)
    # Collapse multiple whitespace characters into a single space
    text = re.sub(r'\s+', ' ', text).strip()
    return text
# --- End Helper Functions ---

# --- API Routes ---

@app.route('/', methods=['GET'])
def root():
    logger.info("Received request for / route")
    return jsonify({"status": "ok", "message": "CleanPlate API is running"})

@app.route('/search', methods=['GET'])
def search():
    search_term_from_user = request.args.get('name', '').strip() # Get raw user input
    if not search_term_from_user:
        logger.warning("Search request received with empty name parameter.")
        return jsonify({"error": "Search term is empty", "status": "error"}), 400

    # Use Python normalize_text for initial cleaning of the user input
    # This helps create a more consistent input for websearch_to_tsquery
    normalized_user_input = normalize_text(search_term_from_user)
    if not normalized_user_input:
        logger.info(f"Search term '{search_term_from_user}' is empty after Python normalization.")
        return jsonify([]) # Return empty list if normalization results in empty string

    # Use the Python-normalized input for the cache key for consistency
    cache_key = f"search_v4:{normalized_user_input}" # v4 for websearch_to_tsquery version
    CACHE_TTL_SECONDS = 3600 * 4 # 4 hours

    redis_conn = get_redis_client()
    if redis_conn:
        try:
            cached_result_str = redis_conn.get(cache_key)
            if cached_result_str:
                logger.info(f"Cache hit for websearch: '{normalized_user_input}'")
                return jsonify(json.loads(cached_result_str))
            else:
                 logger.info(f"Cache miss for websearch: '{normalized_user_input}'")
        except redis.exceptions.RedisError as redis_err:
             logger.error(f"Redis GET error for {cache_key}: {redis_err}")
             sentry_sdk.capture_exception(redis_err) # Fall through to DB
        except Exception as e: # Catch potential JSONDecodeError or other errors
             logger.error(f"Unexpected error during Redis GET or JSON decode for {cache_key}: {e}")
             sentry_sdk.capture_exception(e) # Fall through to DB
    else:
        logger.warning("Redis client unavailable, skipping cache check.")

    # --- Database Query Logic (Using websearch_to_tsquery) ---
    logger.info(f"DB query using websearch_to_tsquery for input: '{normalized_user_input}'")

    query = """
        SELECT
            r.camis, r.dba, r.boro, r.building, r.street, r.zipcode, r.phone,
            r.latitude, r.longitude, r.inspection_date, r.critical_flag, r.grade,
            r.inspection_type, v.violation_code, v.violation_description, r.cuisine_description,
            ts_rank_cd(r.dba_tsv, websearch_to_tsquery('english', %s)) AS rank
        FROM restaurants r
        LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date
        WHERE r.dba_tsv @@ websearch_to_tsquery('english', %s)
        ORDER BY rank DESC, r.dba, r.inspection_date DESC
        LIMIT 100;
    """
    # Pass the Python-normalized user input directly to websearch_to_tsquery
    params = (normalized_user_input, normalized_user_input)

    db_results_raw = None
    try:
        with DatabaseConnection() as conn:
            # Use DictCursor to get results as dictionaries
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(query, params)
                db_results_raw = cursor.fetchall()
    except psycopg2.Error as db_err:
        logger.error(f"DB error during websearch FTS for '{normalized_user_input}': {db_err}")
        sentry_sdk.capture_exception(db_err)
        return jsonify({"error": "Database query failed"}), 500
    except Exception as e:
        logger.error(f"Unexpected DB error during websearch FTS for '{normalized_user_input}': {e}", exc_info=True)
        sentry_sdk.capture_exception(e)
        return jsonify({"error": "An unexpected error occurred during search"}), 500

    if not db_results_raw:
        logger.info(f"No DB results for websearch FTS: '{normalized_user_input}'")
        if redis_conn:
            try:
                redis_conn.setex(cache_key, 60 * 15, json.dumps([])) # Cache empty result for 15 mins
            except redis.exceptions.RedisError as redis_err:
                 logger.error(f"Redis SETEX error for empty key {cache_key}: {redis_err}")
        return jsonify([])
    
    # Convert rows from DictRow to plain dicts for JSON serialization compatibility
    # and further processing
    db_results = [dict(row) for row in db_results_raw]

    # --- Process Results (Adapted from previous versions) ---
    restaurant_dict = {}
    for row_dict in db_results: # db_results is now a list of dicts
        camis = row_dict.get('camis')
        if not camis: continue # Skip if no CAMIS ID

        # Initialize restaurant if not already in dict
        # Exclude inspection-specific and rank fields initially
        if camis not in restaurant_dict:
            restaurant_dict[camis] = {
                k: v for k, v in row_dict.items() if k not in [
                    'rank', 'violation_code', 'violation_description',
                    'inspection_date', 'critical_flag', 'grade', 'inspection_type'
                ]
            }
            restaurant_dict[camis]['inspections'] = {} # Initialize inspections dict
        
        inspection_date_obj = row_dict.get('inspection_date')
        if inspection_date_obj: # Check if there's an inspection date for this row
            inspection_date_str = inspection_date_obj.isoformat()
            # Initialize inspection if not already in dict for this restaurant
            if inspection_date_str not in restaurant_dict[camis]['inspections']:
                restaurant_dict[camis]['inspections'][inspection_date_str] = {
                    'inspection_date': inspection_date_str,
                    'critical_flag': row_dict.get('critical_flag'),
                    'grade': row_dict.get('grade'),
                    'inspection_type': row_dict.get('inspection_type'),
                    'violations': [] # Initialize violations list for this inspection
                }
            
            # Add violation if present in this row
            if row_dict.get('violation_code'):
                violation = {
                    'violation_code': row_dict.get('violation_code'),
                    'violation_description': row_dict.get('violation_description')
                }
                # Ensure violation is not duplicated if data has multiple rows for same insp+viol
                if violation not in restaurant_dict[camis]['inspections'][inspection_date_str]['violations']:
                    restaurant_dict[camis]['inspections'][inspection_date_str]['violations'].append(violation)

    # Convert inspections dict to list
    formatted_results = []
    for restaurant_data in restaurant_dict.values():
        restaurant_data['inspections'] = list(restaurant_data['inspections'].values())
        formatted_results.append(restaurant_data)
    # --- End Process Results ---

    # --- Store Result in Cache ---
    if redis_conn:
        try:
            # Use default=str for datetime/date objects if any are not properly converted during processing
            serialized_data = json.dumps(formatted_results, default=str)
            redis_conn.setex(cache_key, CACHE_TTL_SECONDS, serialized_data)
            logger.info(f"Stored websearch result in cache: {cache_key}")
        except redis.exceptions.RedisError as redis_err:
            logger.error(f"Redis SETEX error cache key {cache_key}: {redis_err}")
            sentry_sdk.capture_exception(redis_err)
        except TypeError as json_err: # Catch JSON serialization errors
            logger.error(f"Error serializing results JSON for websearch {cache_key}: {json_err}")
            sentry_sdk.capture_exception(json_err)
    # --- End Store Result in Cache ---

    logger.info(f"Websearch FTS for '{search_term_from_user}' OK, returning {len(formatted_results)} restaurants.")
    return jsonify(formatted_results)


@app.route('/recent', methods=['GET'])
def recent_restaurants():
    """ Fetches recently graded (A/B/C) restaurants. """
    logger.info("Received request for /recent")
    days_str = request.args.get('days', '7')
    try:
        days = int(days_str)
        days = 7 if days <= 0 or days > 90 else days # Max 90 days, default 7
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
    """ Simple endpoint to test database connectivity. """
    logger.info("Received request for /test-db-connection")
    try:
        with DatabaseConnection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                if result and result[0] == 1:
                    logger.info("Database connection test successful.")
                    return jsonify({"status": "success", "message": "Database connection successful"})
                else:
                    logger.error("DB connection test failed: Query returned unexpected result.")
                    return jsonify({"status": "error", "message": "DB query failed"}), 500
    except Exception as e:
        logger.error(f"Database connection test failed: {e}", exc_info=True)
        sentry_sdk.capture_exception(e)
        return jsonify({"status": "error", "message": f"DB connection error: {str(e)}"}), 500


@app.route('/trigger-update', methods=['POST'])
def trigger_update():
    """ Securely triggers the database update process in a background thread. """
    logger.info("Received request for /trigger-update")
    if not update_logic_imported:
        logger.error("Update logic (run_database_update) not available or failed to import.")
        return jsonify({"status": "error", "message": "Update logic unavailable."}), 500

    provided_key = request.headers.get('X-Update-Secret')
    expected_key = APIConfig.UPDATE_SECRET_KEY

    if not expected_key: # Ensure there IS a key configured
        logger.error("UPDATE_SECRET_KEY not configured on the server.")
        return jsonify({"status": "error", "message": "Update trigger not configured on server."}), 500

    if not provided_key or not secrets.compare_digest(provided_key, expected_key):
        logger.warning("Invalid or missing secret key for /trigger-update.")
        return jsonify({"status": "error", "message": "Unauthorized."}), 403

    logger.info("Secret key validated. Triggering update in background.")
    try:
        update_thread = threading.Thread(target=run_database_update, daemon=True)
        update_thread.start()
    except Exception as e:
        logger.error(f"Failed to start update thread: {e}", exc_info=True)
        sentry_sdk.capture_exception(e)
        return jsonify({"status": "error", "message": "Failed to start update process."}), 500

    logger.info("Successfully launched background update thread.")
    return jsonify({"status": "success", "message": "Database update triggered in background."}), 202


# --- Error Handlers ---
@app.errorhandler(404)
def not_found_error(error): # Parameter name changed to avoid conflict
    logger.warning(f"404 Not Found error for URL: {request.url}")
    return jsonify({"error": "The requested resource was not found", "status": "error"}), 404

@app.errorhandler(500)
def internal_server_error(error): # Parameter name changed
    logger.error(f"500 Internal Server Error handling request for {request.url}: {error}", exc_info=True)
    sentry_sdk.capture_exception(error) # Capture generic 500s
    return jsonify({"error": "An internal server error occurred", "status": "error"}), 500

# --- Main Execution Block ---
if __name__ == "__main__":
    # Use APIConfig for host and port, with fallback to os.environ.get for Railway
    host = os.environ.get("HOST", APIConfig.HOST)
    port = int(os.environ.get("PORT", APIConfig.PORT))
    debug_mode = APIConfig.DEBUG
    
    logger.info(f"Starting Flask app locally via app.run() on {host}:{port} with DEBUG={debug_mode}")
    app.run(host=host, port=port, debug=debug_mode)

logger.info("app_search.py: Module loaded completely.")
