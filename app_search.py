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
# Configure logging (consider moving to a dedicated logging config function/file if complex)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    # Add file handler if needed, like in update_database.py
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

    # Normalize curly apostrophes to straight ones
    input_str = input_str.replace("’", "'").replace("‘", "'")

    # Create a version with periods removed for abbreviations (E.J. -> EJ)
    no_periods_version = input_str.replace(".", "")

    # Keep the standard sanitization for both versions (allow letters, numbers, spaces, apostrophes)
    # Removed '.' from allowed characters here for consistency, handle periods separately
    sanitized_input = re.sub(r"[^\w\s']", "", input_str)
    no_periods_sanitized = re.sub(r"[^\w\s']", "", no_periods_version)

    # Return both versions for use in search
    return sanitized_input, no_periods_sanitized
# --- End Helper Functions ---


# --- API Routes ---

@app.route('/search', methods=['GET'])
def search():
    """
    Searches for restaurants by name (dba), checking cache first.
    """
    name = request.args.get('name', '').strip()
    if not name:
        logger.warning("Search request received with empty name parameter.")
        return jsonify({"error": "Search term is empty", "status": "error"}), 400

    # --- Cache Configuration ---
    # Use a consistent cache key, normalizing the input
    # Using the raw sanitized input might be better than just name_with_periods
    # Let's create a key based on the original search term after basic cleaning
    normalized_name_for_key = name.replace("’", "'").replace("‘", "'").lower().strip()
    cache_key = f"search:{normalized_name_for_key}"
    # Cache expiration time in seconds (e.g., 4 hours)
    CACHE_TTL_SECONDS = 3600 * 4
    # --- End Cache Configuration ---

    # --- 1. Check Cache ---
    if redis_client: # Only check cache if Redis client is available
        try:
            cached_result_str = redis_client.get(cache_key)
            if cached_result_str:
                logger.info(f"Cache hit for search term: '{name}' (key: {cache_key})")
                # Deserialize the JSON string from Redis
                try:
                    restaurants_data = json.loads(cached_result_str)
                    # Return cached data directly
                    return jsonify(restaurants_data)
                except json.JSONDecodeError as json_err:
                     logger.error(f"Error decoding cached JSON for key {cache_key}: {json_err}. Fetching from DB.")
                     # Proceed to database query if cache data is corrupted
            else:
                 logger.info(f"Cache miss for search term: '{name}' (key: {cache_key})")

        except redis.exceptions.RedisError as redis_err:
            # Log Redis errors but proceed to database query as fallback
            logger.error(f"Redis GET error for key {cache_key}: {redis_err}")
            sentry_sdk.capture_exception(redis_err) # Report Redis errors to Sentry
        except Exception as e:
             logger.error(f"Unexpected error during Redis GET for key {cache_key}: {e}")
             sentry_sdk.capture_exception(e)
    else:
        logger.warning("Redis client not available, skipping cache check.")
    # --- End Check Cache ---


    # --- 2. If Cache Miss: Prepare for Database Query ---
    logger.info(f"Performing database query for search term: '{name}'")

    # Get sanitized versions for DB query
    name_with_periods, name_without_periods = sanitize_input(name)

    # Handle potential abbreviation matching (e.g., EJ -> E.J)
    if '.' not in name and len(name_without_periods) >= 2:
        name_with_added_periods = '.'.join(list(name_without_periods))
    else:
        name_with_added_periods = name_with_periods # Use original if it had periods or too short

    # Create transformed versions with 's (handle possessives)
    transformed_name = name_with_periods.replace("s", "'s")
    transformed_name_no_periods = name_without_periods.replace("s", "'s")
    transformed_with_added_periods = name_with_added_periods.replace("s", "'s")

    # SQL Query (using GIN index on dba)
    # LEFT JOIN ensures we get restaurant info even if no violations exist for an inspection
    query = """
        SELECT r.camis, r.dba, r.boro, r.building, r.street,
               r.zipcode, r.phone, r.latitude, r.longitude,
               r.inspection_date, r.critical_flag, r.grade,
               r.inspection_type, v.violation_code, v.violation_description,
               r.cuisine_description -- Include cuisine description
        FROM restaurants r
        LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date
        WHERE r.dba ILIKE %s OR
              r.dba ILIKE %s OR
              r.dba ILIKE %s OR
              r.dba ILIKE %s OR
              r.dba ILIKE %s
        ORDER BY
            CASE
                WHEN UPPER(r.dba) = UPPER(%s) THEN 0  -- Exact match (with periods)
                WHEN UPPER(r.dba) = UPPER(%s) THEN 1  -- Exact match (without periods)
                WHEN UPPER(r.dba) LIKE UPPER(%s) THEN 2  -- Starts with (with periods)
                WHEN UPPER(r.dba) LIKE UPPER(%s) THEN 3  -- Starts with (without periods)
                WHEN UPPER(r.dba) LIKE UPPER(%s) THEN 4  -- Starts with (added periods)
                ELSE 5                                  -- Contains match (handled by ILIKE)
            END,
            r.dba,  -- Then sort alphabetically by name
            r.inspection_date DESC -- Then by most recent inspection
    """

    # Parameters for WHERE clause (handle variations)
    where_params = [
        f"%{name_with_periods}%",
        f"%{transformed_name}%",
        f"%{name_without_periods}%",
        f"%{transformed_name_no_periods}%",
        f"%{name_with_added_periods}%"
    ]

    # Parameters for ORDER BY clause (prioritize exact/prefix matches)
    order_params = [
        name_with_periods,
        name_without_periods,
        f"{name_with_periods}%",
        f"{name_without_periods}%",
        f"{name_with_added_periods}%"
    ]

    params = where_params + order_params

    # --- 3. Execute Database Query ---
    try:
        with DatabaseConnection() as conn:
            with conn.cursor() as cursor:
                logger.debug(f"Executing search query with params: {params}")
                cursor.execute(query, params)
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                logger.debug(f"Database query returned {len(results)} rows.")

                if not results:
                    logger.info(f"No database results found for search term: {name}")
                    # Optional: Cache empty result for a short time
                    if redis_client:
                        try:
                            redis_client.setex(cache_key, 60 * 15, json.dumps([])) # Cache empty for 15 mins
                            logger.info(f"Cached empty result for key: {cache_key}")
                        except redis.exceptions.RedisError as redis_err:
                             logger.error(f"Redis SETEX error for empty result key {cache_key}: {redis_err}")
                             sentry_sdk.capture_exception(redis_err)
                    return jsonify([]) # Return empty list

                # --- 4. Process Database Results ---
                # Aggregate results: Group inspections and violations by restaurant (camis)
                restaurant_dict = {}
                for row in results:
                    restaurant_data = dict(zip(columns, row))
                    camis = restaurant_data.get('camis')
                    inspection_date_obj = restaurant_data.get('inspection_date') # Date object from DB

                    # Ensure camis exists
                    if not camis:
                        logger.warning("Skipping row with missing CAMIS.")
                        continue

                    # Convert date object to string if needed for consistency, or handle None
                    inspection_date_str = inspection_date_obj.isoformat() if inspection_date_obj else None

                    # Initialize restaurant entry if first time seeing this camis
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
                            "cuisine_description": restaurant_data.get('cuisine_description'), # Added cuisine
                            "inspections": {} # Use dict for inspections first for easy lookup
                        }

                    # Process inspection if date exists
                    inspections = restaurant_dict[camis]["inspections"]
                    if inspection_date_str and inspection_date_str not in inspections:
                        inspections[inspection_date_str] = {
                            # Store date as string matching expected format if needed
                            "inspection_date": inspection_date_str,
                            "critical_flag": restaurant_data.get('critical_flag'),
                            "grade": restaurant_data.get('grade'),
                            "inspection_type": restaurant_data.get('inspection_type'),
                            "violations": [] # Initialize violations list for this inspection
                        }

                    # Add violation if it exists for this inspection
                    if inspection_date_str and restaurant_data.get('violation_code'):
                        violation = {
                            "violation_code": restaurant_data.get('violation_code'),
                            "violation_description": restaurant_data.get('violation_description')
                        }
                        # Avoid adding duplicate violations if query returns multiple rows for same violation
                        if violation not in inspections[inspection_date_str]["violations"]:
                            inspections[inspection_date_str]["violations"].append(violation)

                # Convert inspections dict to list and finalize results list
                formatted_results = []
                for restaurant in restaurant_dict.values():
                    # Convert the dict of inspections into a list
                    restaurant["inspections"] = list(restaurant["inspections"].values())
                    formatted_results.append(restaurant)

                # --- 5. Store Result in Cache ---
                if redis_client:
                    try:
                        # Serialize the results list to a JSON string
                        serialized_data = json.dumps(formatted_results)
                        # Store in Redis with the defined TTL
                        redis_client.setex(cache_key, CACHE_TTL_SECONDS, serialized_data)
                        logger.info(f"Stored search result in cache for key: {cache_key} with TTL: {CACHE_TTL_SECONDS}s")
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

    except psycopg2.Error as db_err:
        logger.error(f"Database error during search for '{name}': {db_err}")
        sentry_sdk.capture_exception(db_err) # Report DB errors to Sentry
        # Let the 500 handler manage the response
        raise # Re-raise to trigger the 500 handler
    except Exception as e:
        logger.error(f"Unexpected error during search processing for '{name}': {e}")
        sentry_sdk.capture_exception(e) # Report other errors to Sentry
        # Let the 500 handler manage the response
        raise # Re-raise to trigger the 500 handler


@app.route('/recent', methods=['GET'])
def recent_restaurants():
    """
    Fetches recently graded restaurants (A, B, C).
    TODO: Consider adding caching here as well.
    """
    days = request.args.get('days', '7')
    try:
        days = int(days)
    except ValueError:
        days = 7 # Default to 7 days if input is invalid

    logger.info(f"Fetching recent restaurants (graded A/B/C) from past {days} days.")

    # Query to get distinct restaurants with their most recent graded inspection within the timeframe
    # Uses a subquery or window function to find the latest inspection per restaurant first
    # This query assumes you want the restaurant details based on its *most recent* inspection overall,
    # but filtered to only include restaurants that had *any* A/B/C inspection recently.
    # A simpler approach might just list recent inspections directly.
    # Let's simplify: Get recent A/B/C inspections and associated restaurant info.
    query = """
        SELECT DISTINCT ON (r.camis) -- Get only one row per restaurant (the most recent)
               r.camis, r.dba, r.boro, r.building, r.street, r.zipcode, r.phone,
               r.latitude, r.longitude, r.grade, r.inspection_date, r.cuisine_description
        FROM restaurants r
        WHERE r.grade IN ('A', 'B', 'C')
          AND r.inspection_date >= (CURRENT_DATE - INTERVAL '%s days')
        ORDER BY r.camis, r.inspection_date DESC -- Order by camis, then latest inspection first for DISTINCT ON
        LIMIT 50 -- Limit the number of results
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
        raise # Trigger 500 handler
    except Exception as e:
        logger.error(f"Unexpected error fetching recent restaurants: {e}")
        sentry_sdk.capture_exception(e)
        raise # Trigger 500 handler


@app.route('/test-db-connection', methods=['GET'])
def test_db_connection():
    """Tests the database connection pool."""
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
        # Sentry likely captured this already if DatabaseConnection raises
        return jsonify({"status": "error", "message": f"DB connection error: {str(e)}"}), 500


# --- Error Handlers ---
@app.errorhandler(404)
def not_found(e):
    """Handles 404 Not Found errors."""
    logger.warning(f"404 Not Found error for URL: {request.url}")
    return jsonify({"error": "The requested resource was not found", "status": "error"}), 404

@app.errorhandler(500)
def server_error(e):
    """Handles 500 Internal Server errors."""
    # Note: Sentry's FlaskIntegration typically captures the exception *before*
    # this handler runs, so the error should already be in Sentry.
    # This handler just defines the JSON response format sent to the client.
    logger.error(f"500 Internal Server Error: {e}", exc_info=True) # Log the error details
    return jsonify({"error": "An internal server error occurred", "status": "error"}), 500
# --- End Error Handlers ---


# --- Main Execution ---
if __name__ == "__main__":
    # Use Gunicorn or another WSGI server in production instead of app.run()
    # Configuration for app.run is mainly for local development
    logger.info(f"Starting Flask app locally on {APIConfig.HOST}:{APIConfig.PORT} with DEBUG={APIConfig.DEBUG}")
    app.run(
        host=APIConfig.HOST,
        port=APIConfig.PORT,
        debug=APIConfig.DEBUG
    )
# --- End Main Execution ---

