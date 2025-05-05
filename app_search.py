# app_search.py - Updated with External Scheduler Trigger

# Standard library imports
import os
import re
import logging
import json
import threading # --- ADDED: For background tasks ---
import secrets   # --- ADDED: For secure key comparison ---

# Third-party imports
import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration
from flask import Flask, jsonify, request, make_response
from flask_cors import CORS
import psycopg2 # Import psycopg2 for specific DB error handling if needed
import redis # Import redis for specific Redis error handling

# Local application imports
# Assumes redis_client is initialized in db_manager and imported
from db_manager import DatabaseConnection, redis_client
# Import all config classes
from config import APIConfig, SentryConfig, DatabaseConfig, RedisConfig

# --- ADDED: Import the update function ---
try:
    from update_database import run_database_update
    update_logic_imported = True
    logging.info("Successfully imported run_database_update function.")
except ImportError:
    logging.error("Failed to import run_database_update function. Update endpoint will not work.")
    update_logic_imported = False
    # Define a dummy function if import fails to prevent NameError later
    def run_database_update():
         logging.error("Update logic could not be imported.")
# --- END ADDED: Import the update function ---


# --- Sentry Initialization ---
if SentryConfig.SENTRY_DSN:
    try:
        sentry_sdk.init(
            dsn=SentryConfig.SENTRY_DSN,
            integrations=[FlaskIntegration()],
            traces_sample_rate=1.0, # Adjust sample rate as needed for production
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
# Configure logging (ensure it captures necessary info)
logging.basicConfig(
    level=logging.INFO if not APIConfig.DEBUG else logging.DEBUG, # Set level based on DEBUG
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
    # Normalize different apostrophe types
    input_str = input_str.replace("’", "'").replace("‘", "'")
    # Version without periods
    no_periods_version = input_str.replace(".", "")
    # Sanitize both versions (allow letters, numbers, whitespace, standard apostrophe)
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
                    # Attempt to load JSON from cache
                    restaurants_data = json.loads(cached_result_str)
                    return jsonify(restaurants_data)
                except json.JSONDecodeError as json_err:
                     # Log error if cached data is invalid, proceed to DB fetch
                     logger.error(f"Error decoding cached JSON for key {cache_key}: {json_err}. Fetching from DB.")
                     sentry_sdk.capture_exception(json_err)
            else:
                 # Log cache miss
                 logger.info(f"Cache miss for search term: '{name}' (key: {cache_key})")
        except redis.exceptions.TimeoutError:
             # Log Redis timeout, proceed to DB fetch
             logger.warning(f"Redis timeout during GET for key {cache_key}. Fetching from DB.")
        except redis.exceptions.RedisError as redis_err:
            # Log other Redis errors, proceed to DB fetch
            logger.error(f"Redis GET error for key {cache_key}: {redis_err}. Fetching from DB.")
            sentry_sdk.capture_exception(redis_err)
        except Exception as e:
             # Log unexpected errors during cache check, proceed to DB fetch
             logger.error(f"Unexpected error during Redis GET for key {cache_key}: {e}")
             sentry_sdk.capture_exception(e)
    else:
        # Log if Redis client isn't configured/available
        logger.warning("Redis client not available, skipping cache check.")
    # --- End Check Cache ---


    # --- 2. If Cache Miss: Prepare for Database Query ---
    logger.info(f"Performing database query for search term: '{name}'")
    # Sanitize and prepare variations of the search term
    name_with_periods, name_without_periods = sanitize_input(name)
    # Handle potential abbreviation expansion (e.g., "abc" -> "a.b.c")
    if '.' not in name and len(name_without_periods) >= 2:
        name_with_added_periods = '.'.join(list(name_without_periods))
    else:
        name_with_added_periods = name_with_periods
    # Handle potential possessive 's' variations
    transformed_name = name_with_periods.replace("s", "'s")
    transformed_name_no_periods = name_without_periods.replace("s", "'s")
    transformed_with_added_periods = name_with_added_periods.replace("s", "'s")

    # SQL query to search across different variations and order results by relevance
    query = """
        SELECT r.camis, r.dba, r.boro, r.building, r.street,
               r.zipcode, r.phone, r.latitude, r.longitude,
               r.inspection_date, r.critical_flag, r.grade,
               r.inspection_type, v.violation_code, v.violation_description,
               r.cuisine_description
        FROM restaurants r
        LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date
        WHERE r.dba ILIKE %s OR  -- Match name with periods
              r.dba ILIKE %s OR  -- Match transformed name with periods
              r.dba ILIKE %s OR  -- Match name without periods
              r.dba ILIKE %s OR  -- Match transformed name without periods
              r.dba ILIKE %s     -- Match name with added periods
        ORDER BY
            -- Prioritize exact matches, then prefix matches
            CASE
                WHEN UPPER(r.dba) = UPPER(%s) THEN 0 -- Exact match (with periods)
                WHEN UPPER(r.dba) = UPPER(%s) THEN 1 -- Exact match (without periods)
                WHEN UPPER(r.dba) LIKE UPPER(%s) THEN 2 -- Prefix match (with periods)
                WHEN UPPER(r.dba) LIKE UPPER(%s) THEN 3 -- Prefix match (without periods)
                WHEN UPPER(r.dba) LIKE UPPER(%s) THEN 4 -- Prefix match (added periods)
                ELSE 5 -- Other partial matches
            END,
            r.dba, -- Alphabetical within relevance group
            r.inspection_date DESC -- Most recent inspection first for the same restaurant
    """
    # Parameters for the WHERE clause (using LIKE for partial matches)
    where_params = [ f"%{p}%" for p in [name_with_periods, transformed_name, name_without_periods, transformed_name_no_periods, name_with_added_periods] ]
    # Parameters for the ORDER BY clause (exact matches and prefix matches)
    order_params = [ name_with_periods, name_without_periods, f"{name_with_periods}%", f"{name_without_periods}%", f"{name_with_added_periods}%" ]
    # Combine parameters for the query execution
    params = where_params + order_params

    # --- 3. Execute Database Query with Retry Logic ---
    MAX_DB_RETRIES = 1 # Try original + 1 retry
    db_results = None
    last_db_error = None

    for attempt in range(MAX_DB_RETRIES + 1):
        try:
            # Use the DatabaseConnection context manager for each attempt
            with DatabaseConnection() as conn:
                with conn.cursor() as cursor:
                    logger.debug(f"Attempt {attempt + 1}: Executing search query with params: {params}")
                    cursor.execute(query, params)
                    db_results = cursor.fetchall() # Fetch results if execute succeeds
                    columns = [desc[0] for desc in cursor.description] # Get column names
                    logger.debug(f"Attempt {attempt + 1}: Database query successful, returned {len(db_results) if db_results else 0} rows.")
                    last_db_error = None # Clear last error on success
                    break # Exit retry loop on success

        except psycopg2.OperationalError as op_err:
            # Handle potentially transient errors (connection issues, etc.)
            last_db_error = op_err # Store the error
            logger.warning(f"Attempt {attempt + 1}: Database OperationalError during search for '{name}': {op_err}")
            sentry_sdk.capture_exception(op_err) # Report the error to Sentry
            if attempt < MAX_DB_RETRIES:
                logger.info(f"Retrying database query (attempt {attempt + 2})...")
                # Optional: add a small delay before retrying? import time; time.sleep(0.1)
                continue # Go to the next iteration to retry
            else:
                # Log final failure after retries
                logger.error(f"Database query failed after {MAX_DB_RETRIES + 1} attempts for search term '{name}'.")
                raise op_err # Re-raise the last error if all retries fail

        except psycopg2.Error as db_err:
            # Handle other specific, non-retryable DB errors
            last_db_error = db_err
            logger.error(f"Attempt {attempt + 1}: Non-operational Database error during search for '{name}': {db_err}")
            sentry_sdk.capture_exception(db_err)
            raise db_err # Re-raise immediately

        except Exception as e:
            # Catch any other unexpected errors during DB interaction
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
        # Handle case where search term yields no results
        logger.info(f"No database results found for search term: {name}")
        # Cache empty result to avoid repeated DB queries for non-existent terms
        if redis_client:
            try:
                # Cache empty list for 15 minutes
                redis_client.setex(cache_key, 60 * 15, json.dumps([]))
                logger.info(f"Cached empty result for key: {cache_key}")
            except redis.exceptions.RedisError as redis_err:
                 logger.error(f"Redis SETEX error for empty result key {cache_key}: {redis_err}")
                 sentry_sdk.capture_exception(redis_err)
        return jsonify([]) # Return empty list

    # --- 4. Process Database Results ---
    logger.debug("Processing database results...")
    restaurant_dict = {}
    # Aggregate results by restaurant (camis), collecting inspections and violations
    for row in db_results:
        restaurant_data = dict(zip(columns, row)) # Convert row tuple to dictionary
        camis = restaurant_data.get('camis')
        inspection_date_obj = restaurant_data.get('inspection_date')
        if not camis: continue # Skip if CAMIS ID is missing
        # Format date as ISO string for consistency
        inspection_date_str = inspection_date_obj.isoformat() if inspection_date_obj else None

        # Initialize restaurant entry if not seen before
        if camis not in restaurant_dict:
            restaurant_dict[camis] = {
                "camis": camis, "dba": restaurant_data.get('dba'),
                "boro": restaurant_data.get('boro'), "building": restaurant_data.get('building'),
                "street": restaurant_data.get('street'), "zipcode": restaurant_data.get('zipcode'),
                "phone": restaurant_data.get('phone'), "latitude": restaurant_data.get('latitude'),
                "longitude": restaurant_data.get('longitude'),
                "cuisine_description": restaurant_data.get('cuisine_description'),
                "inspections": {} # Use dict for inspections temporarily for easy lookup
            }

        inspections = restaurant_dict[camis]["inspections"]
        # Initialize inspection entry if not seen before for this restaurant
        if inspection_date_str and inspection_date_str not in inspections:
            inspections[inspection_date_str] = {
                "inspection_date": inspection_date_str,
                "critical_flag": restaurant_data.get('critical_flag'),
                "grade": restaurant_data.get('grade'),
                "inspection_type": restaurant_data.get('inspection_type'),
                "violations": [] # List to hold violations for this inspection
            }

        # Add violation if it exists for this row and inspection date
        if inspection_date_str and restaurant_data.get('violation_code'):
            violation = {
                "violation_code": restaurant_data.get('violation_code'),
                "violation_description": restaurant_data.get('violation_description')
            }
            # Avoid adding duplicate violations if JOIN produced multiple rows for same violation
            if violation not in inspections[inspection_date_str]["violations"]:
                inspections[inspection_date_str]["violations"].append(violation)

    # Convert inspections dictionary back to a list for final output
    formatted_results = []
    for restaurant in restaurant_dict.values():
        restaurant["inspections"] = list(restaurant["inspections"].values())
        formatted_results.append(restaurant)
    logger.debug("Finished processing database results.")

    # --- 5. Store Result in Cache ---
    if redis_client:
        try:
            # Serialize the processed results to JSON string
            serialized_data = json.dumps(formatted_results)
            # Store in Redis with the defined TTL
            redis_client.setex(cache_key, CACHE_TTL_SECONDS, serialized_data)
            logger.info(f"Stored search result in cache for key: {cache_key} with TTL: {CACHE_TTL_SECONDS}s")
        except redis.exceptions.TimeoutError:
             logger.warning(f"Redis timeout during SETEX for key {cache_key}.")
        except redis.exceptions.RedisError as redis_err:
             # Log Redis errors during caching
             logger.error(f"Redis SETEX error caching key {cache_key}: {redis_err}")
             sentry_sdk.capture_exception(redis_err)
        except TypeError as json_err:
             # Log errors if data cannot be serialized to JSON
             logger.error(f"Error serializing results to JSON for cache key {cache_key}: {json_err}")
             sentry_sdk.capture_exception(json_err)
        except Exception as e:
             # Log any other unexpected errors during caching
             logger.error(f"Unexpected error during Redis SETEX for key {cache_key}: {e}")
             sentry_sdk.capture_exception(e)
    # --- End Store Result in Cache ---

    logger.info(f"Database search for '{name}' successful, returning {len(formatted_results)} restaurants.")
    # --- 6. Return Database Result ---
    return jsonify(formatted_results)


# --- Keep other routes: /recent, /test-db-connection ---
@app.route('/recent', methods=['GET'])
def recent_restaurants():
    """ Fetches recently graded (A/B/C) restaurants. """
    days = request.args.get('days', '7') # Default to 7 days
    try:
        days = int(days)
        if days <= 0: days = 7 # Ensure positive number of days
    except ValueError:
        days = 7 # Default if conversion fails
    logger.info(f"Fetching recent restaurants (graded A/B/C) from past {days} days.")

    # Query to get the latest inspection for each restaurant graded A/B/C within the timeframe
    query = """
        SELECT DISTINCT ON (r.camis) -- Get only the latest record per restaurant
               r.camis, r.dba, r.boro, r.building, r.street, r.zipcode, r.phone,
               r.latitude, r.longitude, r.grade, r.inspection_date, r.cuisine_description
        FROM restaurants r
        WHERE r.grade IN ('A', 'B', 'C') -- Only include graded restaurants
          AND r.inspection_date >= (CURRENT_DATE - INTERVAL '%s days') -- Filter by date range
        ORDER BY r.camis, r.inspection_date DESC -- Order to get the latest inspection first for DISTINCT ON
        LIMIT 50 -- Limit the number of results
    """
    try:
        # Use context manager for database connection
        with DatabaseConnection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (days,)) # Pass days as parameter
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] # Get column names
                # Format results as list of dictionaries
                formatted_results = [dict(zip(columns, row)) for row in results]
                logger.info(f"Recent restaurants fetch returned {len(formatted_results)} results.")
                return jsonify(formatted_results)
    except psycopg2.Error as db_err:
        # Log specific database errors
        logger.error(f"Error fetching recent restaurants: {db_err}")
        sentry_sdk.capture_exception(db_err)
        raise # Re-raise to trigger 500 handler
    except Exception as e:
        # Log unexpected errors
        logger.error(f"Unexpected error fetching recent restaurants: {e}", exc_info=True)
        sentry_sdk.capture_exception(e)
        raise # Re-raise to trigger 500 handler


@app.route('/test-db-connection', methods=['GET'])
def test_db_connection():
    """ Simple endpoint to test database connectivity. """
    logger.info("Received request for /test-db-connection")
    try:
        # Use context manager for database connection
        with DatabaseConnection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1") # Simple query
                result = cursor.fetchone()
                if result and result[0] == 1:
                    # Success case
                    logger.info("Database connection test successful.")
                    return jsonify({"status": "success", "message": "Database connection successful"})
                else:
                     # Query executed but returned unexpected result
                     logger.error("Database connection test failed: Query returned unexpected result.")
                     return jsonify({"status": "error", "message": "DB query failed"}), 500
    except Exception as e:
        # Handle connection errors or other exceptions
        logger.error(f"Database connection test failed: {e}")
        # Optionally send to Sentry here as well if not caught by global handler
        # sentry_sdk.capture_exception(e)
        return jsonify({"status": "error", "message": f"DB connection error: {str(e)}"}), 500


# --- ADDED: Route for triggering background update ---
@app.route('/trigger-update', methods=['POST'])
def trigger_update():
    """
    Securely triggers the database update process in a background thread.
    Requires a secret key passed in the 'X-Update-Secret' header.
    """
    logger.info("Received request for /trigger-update")

    # 1. Check if update logic function was imported successfully
    if not update_logic_imported:
         logger.error("Update logic was not imported. Cannot trigger update.")
         return jsonify({"status": "error", "message": "Update logic unavailable."}), 500

    # 2. Get the secret key provided in the request header
    provided_key = request.headers.get('X-Update-Secret')

    # 3. Get the expected secret key from configuration
    expected_key = APIConfig.UPDATE_SECRET_KEY # Loaded from env var via config.py

    # 4. Validate that the expected key is configured on the server
    if not expected_key:
        logger.error("UPDATE_SECRET_KEY is not configured on the server.")
        return jsonify({"status": "error", "message": "Update trigger not configured."}), 500

    # 5. Securely compare the provided key with the expected key
    # Use secrets.compare_digest to prevent timing attacks
    if not provided_key or not secrets.compare_digest(provided_key, expected_key):
        logger.warning("Invalid or missing secret key provided for /trigger-update.")
        # Return a generic error to avoid revealing whether key was missing or just wrong
        return jsonify({"status": "error", "message": "Unauthorized."}), 403 # Forbidden

    # 6. If keys match, trigger the update function in a separate thread
    logger.info("Secret key validated. Triggering database update in background thread.")
    try:
        # Create and start a background thread to run the update function
        # daemon=True ensures thread exits if main app process terminates
        update_thread = threading.Thread(target=run_database_update, daemon=True)
        update_thread.start()
    except Exception as e:
        # Log error if thread fails to start
        logger.error(f"Failed to start update thread: {e}", exc_info=True)
        sentry_sdk.capture_exception(e) # Optionally report to Sentry
        return jsonify({"status": "error", "message": "Failed to start update process."}), 500

    # 7. Return an immediate success response (202 Accepted)
    # Indicates the request was accepted and processing started in background
    logger.info("Successfully launched background update thread.")
    return jsonify({"status": "success", "message": "Database update triggered in background."}), 202
# --- END ADDED: Route for triggering background update ---


# --- Keep Error Handlers ---
@app.errorhandler(404)
def not_found(e):
    """ Handles 404 Not Found errors. """
    logger.warning(f"404 Not Found error for URL: {request.url}")
    # Return JSON response for API clients
    return jsonify({"error": "The requested resource was not found", "status": "error"}), 404

@app.errorhandler(500)
def server_error(e):
    """ Handles 500 Internal Server errors. """
    # Sentry integration automatically captures exceptions leading to 500 errors
    logger.error(f"500 Internal Server Error handling request for {request.url}: {e}", exc_info=True)
    # Return JSON response for API clients
    return jsonify({"error": "An internal server error occurred", "status": "error"}), 500

# --- Keep Main Execution Block ---
# This block runs only when the script is executed directly (e.g., `python app_search.py`)
# It's typically used for local development testing. Gunicorn runs the app differently.
if __name__ == "__main__":
    logger.info(f"Starting Flask app locally via app.run() on {APIConfig.HOST}:{APIConfig.PORT} with DEBUG={APIConfig.DEBUG}")
    # Use Flask's built-in development server
    # Note: This is NOT recommended for production. Gunicorn is used via Procfile.
    app.run(
        host=APIConfig.HOST,
        port=APIConfig.PORT,
        debug=APIConfig.DEBUG # Enable/disable debug mode based on config
    )
