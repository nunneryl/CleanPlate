# Standard library imports
import os
import re
import logging
import json # <-- Import json for serialization/deserialization

# Third-party imports
import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration
from flask import Flask, jsonify, request, make_response
from flask_cors import CORS
import psycopg2 # Import psycopg2 for specific DB error handling if needed
import redis # Import redis for specific Redis error handling

# Local application imports
# Assumes redis_client is initialized in db_manager and imported
from db_manager import DatabaseConnection, redis_client # <-- Import redis_client
# Import all config classes
from config import APIConfig, SentryConfig, DatabaseConfig, RedisConfig

# --- Sentry Initialization ---
if SentryConfig.SENTRY_DSN:
    try:
        sentry_sdk.init(
            dsn=SentryConfig.SENTRY_DSN,
            integrations=[FlaskIntegration()],
            traces_sample_rate=1.0,
            environment="development" if APIConfig.DEBUG else "production",
            # release="cleanplate@x.y.z" # Optional: Add release tracking
        )
        logging.info("Sentry initialized successfully.")
    except Exception as e:
         logging.error(f"Failed to initialize Sentry: {e}")
else:
    logging.warning("SENTRY_DSN environment variable not found, Sentry not initialized.")
# --- End Sentry Initialization ---

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
# --- End Logging Setup ---

# --- Flask App Initialization ---
app = Flask(__name__)
CORS(app) # Enable CORS for all origins
# --- End Flask App Initialization ---


# --- Helper Functions ---
def sanitize_input(input_str):
    """
    Sanitizes the input string for search.
    Removes potentially harmful characters and normalizes apostrophes.
    Creates versions with and without periods for abbreviation matching.
    """
    if not input_str:
        return "", ""
    input_str = input_str.replace("’", "'").replace("‘", "'")
    no_periods_version = input_str.replace(".", "")
    sanitized_input = re.sub(r"[^\w\s']", "", input_str)
    no_periods_sanitized = re.sub(r"[^\w\s']", "", no_periods_version)
    return sanitized_input, no_periods_sanitized
# --- End Helper Functions ---


# --- API Routes ---

@app.route('/search', methods=['GET'])
def search():
    """ Searches for restaurants by name (dba), checking cache first, with DB retry. """
    name = request.args.get('name', '').strip()
    if not name:
        logger.warning("Search request received with empty name parameter.")
        return jsonify({"error": "Search term is empty", "status": "error"}), 400

    # --- Cache Configuration ---
    normalized_name_for_key = name.replace("’", "'").replace("‘", "'").lower().strip()
    cache_key = f"search:{normalized_name_for_key}"
    CACHE_TTL_SECONDS = 3600 * 4 # 4 hours
    # --- End Cache Configuration ---

    # --- 1. Check Cache ---
    if redis_client:
        try:
            cached_result_str = redis_client.get(cache_key)
            if cached_result_str:
                logger.info(f"Cache hit for search term: '{name}' (key: {cache_key})")
                try:
                    restaurants_data = json.loads(cached_result_str)
                    return jsonify(restaurants_data)
                except json.JSONDecodeError as json_err:
                     logger.error(f"Error decoding cached JSON for key {cache_key}: {json_err}. Fetching from DB.")
                     sentry_sdk.capture_exception(json_err)
            else:
                 logger.info(f"Cache miss for search term: '{name}' (key: {cache_key})")
        except redis.exceptions.TimeoutError:
             logger.warning(f"Redis timeout during GET for key {cache_key}. Fetching from DB.")
        except redis.exceptions.RedisError as redis_err:
            logger.error(f"Redis GET error for key {cache_key}: {redis_err}. Fetching from DB.")
            sentry_sdk.capture_exception(redis_err)
        except Exception as e:
             logger.error(f"Unexpected error during Redis GET for key {cache_key}: {e}")
             sentry_sdk.capture_exception(e)
    else:
        logger.warning("Redis client not available, skipping cache check.")
    # --- End Check Cache ---


    # --- 2. If Cache Miss: Prepare for Database Query ---
    logger.info(f"Performing database query for search term: '{name}'")
    name_with_periods, name_without_periods = sanitize_input(name)
    if '.' not in name and len(name_without_periods) >= 2:
        name_with_added_periods = '.'.join(list(name_without_periods))
    else:
        name_with_added_periods = name_with_periods
    transformed_name = name_with_periods.replace("s", "'s")
    transformed_name_no_periods = name_without_periods.replace("s", "'s")
    transformed_with_added_periods = name_with_added_periods.replace("s", "'s")

    query = """
        SELECT r.camis, r.dba, r.boro, r.building, r.street,
               r.zipcode, r.phone, r.latitude, r.longitude,
               r.inspection_date, r.critical_flag, r.grade,
               r.inspection_type, v.violation_code, v.violation_description,
               r.cuisine_description
        FROM restaurants r
        LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date
        WHERE r.dba ILIKE %s OR
              r.dba ILIKE %s OR
              r.dba ILIKE %s OR
              r.dba ILIKE %s OR
              r.dba ILIKE %s
        ORDER BY
            CASE
                WHEN UPPER(r.dba) = UPPER(%s) THEN 0
                WHEN UPPER(r.dba) = UPPER(%s) THEN 1
                WHEN UPPER(r.dba) LIKE UPPER(%s) THEN 2
                WHEN UPPER(r.dba) LIKE UPPER(%s) THEN 3
                WHEN UPPER(r.dba) LIKE UPPER(%s) THEN 4
                ELSE 5
            END,
            r.dba,
            r.inspection_date DESC
    """
    where_params = [ f"%{p}%" for p in [name_with_periods, transformed_name, name_without_periods, transformed_name_no_periods, name_with_added_periods] ]
    order_params = [ name_with_periods, name_without_periods, f"{name_with_periods}%", f"{name_without_periods}%", f"{name_with_added_periods}%" ]
    params = where_params + order_params

    # --- 3. Execute Database Query with Retry Logic ---
    MAX_DB_RETRIES = 1 # Try original + 1 retry
    db_results = None
    last_db_error = None

    for attempt in range(MAX_DB_RETRIES + 1):
        try:
            # Use the DatabaseConnection context manager for each attempt
            # This ensures a connection is properly acquired and released/rolled back
            with DatabaseConnection() as conn:
                with conn.cursor() as cursor:
                    logger.debug(f"Attempt {attempt + 1}: Executing search query with params: {params}")
                    cursor.execute(query, params)
                    db_results = cursor.fetchall() # Fetch results if execute succeeds
                    columns = [desc[0] for desc in cursor.description]
                    logger.debug(f"Attempt {attempt + 1}: Database query successful, returned {len(db_results) if db_results else 0} rows.")
                    last_db_error = None # Clear last error on success
                    break # Exit retry loop on success

        except psycopg2.OperationalError as op_err:
            last_db_error = op_err # Store the error
            logger.warning(f"Attempt {attempt + 1}: Database OperationalError during search for '{name}': {op_err}")
            sentry_sdk.capture_exception(op_err) # Report the error to Sentry
            if attempt < MAX_DB_RETRIES:
                logger.info(f"Retrying database query (attempt {attempt + 2})...")
                # Optional: add a small delay before retrying? time.sleep(0.1)
                continue # Go to the next iteration to retry
            else:
                logger.error(f"Database query failed after {MAX_DB_RETRIES + 1} attempts for search term '{name}'.")
                raise op_err # Re-raise the last error if all retries fail

        except psycopg2.Error as db_err: # Catch other specific DB errors if needed
            last_db_error = db_err
            logger.error(f"Attempt {attempt + 1}: Non-operational Database error during search for '{name}': {db_err}")
            sentry_sdk.capture_exception(db_err)
            raise db_err # Re-raise immediately if it's not an OperationalError we want to retry

        except Exception as e: # Catch any other unexpected errors
            last_db_error = e
            logger.error(f"Attempt {attempt + 1}: Unexpected error during database interaction for '{name}': {e}", exc_info=True)
            sentry_sdk.capture_exception(e)
            raise e # Re-raise unexpected errors immediately

    # --- End DB Query Loop ---

    # Check if we exited the loop due to an error after retries
    if db_results is None and last_db_error is not None:
         # This case should technically be covered by the re-raise in the loop,
         # but adding for clarity. Let the 500 handler deal with it.
         logger.error(f"Exited DB query loop due to persistent error: {last_db_error}")
         raise last_db_error

    # --- Process results (only if db_results is not None) ---
    if db_results is None:
         # Should not happen if logic above is correct, but as a safeguard:
         logger.error("db_results is None after query loop without error being raised.")
         raise Exception("Failed to retrieve database results.")


    if not db_results:
        logger.info(f"No database results found for search term: {name}")
        # Cache empty result
        if redis_client:
            try:
                redis_client.setex(cache_key, 60 * 15, json.dumps([])) # Cache empty for 15 mins
                logger.info(f"Cached empty result for key: {cache_key}")
            except redis.exceptions.RedisError as redis_err:
                 logger.error(f"Redis SETEX error for empty result key {cache_key}: {redis_err}")
                 sentry_sdk.capture_exception(redis_err)
        return jsonify([])

    # --- 4. Process Database Results ---
    logger.debug("Processing database results...")
    restaurant_dict = {}
    # (Aggregation logic remains the same)
    for row in db_results: # Use db_results fetched earlier
        restaurant_data = dict(zip(columns, row))
        camis = restaurant_data.get('camis')
        inspection_date_obj = restaurant_data.get('inspection_date')
        if not camis: continue
        inspection_date_str = inspection_date_obj.isoformat() if inspection_date_obj else None

        if camis not in restaurant_dict:
            restaurant_dict[camis] = {
                "camis": camis, "dba": restaurant_data.get('dba'),
                "boro": restaurant_data.get('boro'), "building": restaurant_data.get('building'),
                "street": restaurant_data.get('street'), "zipcode": restaurant_data.get('zipcode'),
                "phone": restaurant_data.get('phone'), "latitude": restaurant_data.get('latitude'),
                "longitude": restaurant_data.get('longitude'),
                "cuisine_description": restaurant_data.get('cuisine_description'),
                "inspections": {}
            }
        inspections = restaurant_dict[camis]["inspections"]
        if inspection_date_str and inspection_date_str not in inspections:
            inspections[inspection_date_str] = {
                "inspection_date": inspection_date_str,
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
            if violation not in inspections[inspection_date_str]["violations"]:
                inspections[inspection_date_str]["violations"].append(violation)

    formatted_results = []
    for restaurant in restaurant_dict.values():
        restaurant["inspections"] = list(restaurant["inspections"].values())
        formatted_results.append(restaurant)
    logger.debug("Finished processing database results.")

    # --- 5. Store Result in Cache ---
    if redis_client:
        try:
            serialized_data = json.dumps(formatted_results)
            redis_client.setex(cache_key, CACHE_TTL_SECONDS, serialized_data)
            logger.info(f"Stored search result in cache for key: {cache_key} with TTL: {CACHE_TTL_SECONDS}s")
        except redis.exceptions.TimeoutError:
             logger.warning(f"Redis timeout during SETEX for key {cache_key}.")
        except redis.exceptions.RedisError as redis_err:
             logger.error(f"Redis SETEX error caching key {cache_key}: {redis_err}")
             sentry_sdk.capture_exception(redis_err)
        except TypeError as json_err:
             logger.error(f"Error serializing results to JSON for cache key {cache_key}: {json_err}")
             sentry_sdk.capture_exception(json_err)
        except Exception as e:
             logger.error(f"Unexpected error during Redis SETEX for key {cache_key}: {e}")
             sentry_sdk.capture_exception(e)
    # --- End Store Result in Cache ---

    logger.info(f"Database search for '{name}' successful, returning {len(formatted_results)} restaurants.")
    # --- 6. Return Database Result ---
    return jsonify(formatted_results)


# --- Keep other routes: /recent, /test-db-connection ---
@app.route('/recent', methods=['GET'])
def recent_restaurants():
    # ... (existing /recent code) ...
    # Consider adding retry logic here too if needed
    days = request.args.get('days', '7')
    try:
        days = int(days)
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
        LIMIT 50
    """
    try:
        with DatabaseConnection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (days,))
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                formatted_results = [dict(zip(columns, row)) for row in results]
                logger.info(f"Recent restaurants fetch returned {len(formatted_results)} results.")
                return jsonify(formatted_results)
    except psycopg2.Error as db_err:
        logger.error(f"Error fetching recent restaurants: {db_err}")
        sentry_sdk.capture_exception(db_err)
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching recent restaurants: {e}", exc_info=True)
        sentry_sdk.capture_exception(e)
        raise

@app.route('/test-db-connection', methods=['GET'])
def test_db_connection():
    # ... (existing /test-db-connection code) ...
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
                     logger.error("Database connection test failed: Query returned unexpected result.")
                     return jsonify({"status": "error", "message": "DB query failed"}), 500
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return jsonify({"status": "error", "message": f"DB connection error: {str(e)}"}), 500


# --- Keep Error Handlers ---
@app.errorhandler(404)
def not_found(e):
    # ... (existing 404 handler) ...
    logger.warning(f"404 Not Found error for URL: {request.url}")
    return jsonify({"error": "The requested resource was not found", "status": "error"}), 404

@app.errorhandler(500)
def server_error(e):
    # ... (existing 500 handler) ...
    # Sentry captures the exception before this runs
    logger.error(f"500 Internal Server Error handling request for {request.url}: {e}", exc_info=True)
    return jsonify({"error": "An internal server error occurred", "status": "error"}), 500

# --- Keep Main Execution Block ---
if __name__ == "__main__":
    # ... (existing main block) ...
    logger.info(f"Starting Flask app locally via app.run() on {APIConfig.HOST}:{APIConfig.PORT} with DEBUG={APIConfig.DEBUG}")
    app.run(
        host=APIConfig.HOST,
        port=APIConfig.PORT,
        debug=APIConfig.DEBUG
    )
