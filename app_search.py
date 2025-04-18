# Near the top of app_search.py, after existing imports

import os
import re
import logging
import sentry_sdk # <-- Add Sentry SDK import
from sentry_sdk.integrations.flask import FlaskIntegration # <-- Add Flask Integration import
from flask import Flask, jsonify, request, make_response
from flask_cors import CORS
from db_manager import DatabaseConnection
# Make sure your config import includes the class/way you load SENTRY_DSN
from config import APIConfig, SentryConfig, DatabaseConfig # <-- Assuming SentryConfig loads SENTRY_DSN

# --- Sentry Initialization Block ---
# Check if the DSN environment variable was loaded successfully by config.py
if SentryConfig.SENTRY_DSN:
    try:
        sentry_sdk.init(
            # Read the DSN from your configuration
            dsn=SentryConfig.SENTRY_DSN,

            # Add Flask integration to automatically capture Flask errors
            integrations=[
                FlaskIntegration(),
            ],

            # Set traces_sample_rate to 1.0 to capture 100%
            # of transactions for performance monitoring.
            # Lower this value (e.g., 0.1 for 10%) in production
            # if performance monitoring adds too much overhead.
            traces_sample_rate=1.0,

            # Set the environment based on your DEBUG flag or another env var
            # This helps filter issues in Sentry (e.g., production vs development)
            environment="development" if APIConfig.DEBUG else "production",

            # Optional: Set a release version (e.g., from an env var or git commit)
            # release="cleanplate@1.0.1"
        )
        # Log success if initialization works
        logging.info("Sentry initialized successfully.")
    except Exception as e:
         # Log error if initialization fails
         logging.error(f"Failed to initialize Sentry: {e}")
else:
    # Log warning if the DSN wasn't found in the environment variables
    logging.warning("SENTRY_DSN environment variable not found, Sentry not initialized.")
# --- End Sentry Initialization Block ---

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)  # Enable CORS for all origins

def sanitize_input(input_str):
    if not input_str:
        return "", ""
    
    # Normalize curly apostrophes to straight ones
    input_str = input_str.replace("'", "'").replace("'", "'")
    
    # Create a version with periods removed for abbreviations (E.J. -> EJ)
    no_periods_version = input_str.replace(".", "")
    
    # Keep the standard sanitization for both versions
    sanitized_input = re.sub(r"[^\w\s'.]", "", input_str)
    no_periods_sanitized = re.sub(r"[^\w\s']", "", no_periods_version)
    
    # Return both versions for use in search
    return sanitized_input, no_periods_sanitized
    
# Add this temporary route for Sentry testing
@app.route('/sentry-test')
def sentry_test_route():
    try:
        # This line will intentionally cause a ZeroDivisionError
        result = 1 / 0
    except Exception as e:
        # Even though we catch it here, Sentry's FlaskIntegration
        # should have already captured the error before this point.
        # We still raise it to ensure a 500 error response.
         logger.error("Intentional Sentry test error occurred.", exc_info=True)
         raise e
    return "This should not be reached." # Should return 500 error
    
@app.route('/search', methods=['GET'])
def search():
    name = request.args.get('name', '').strip()
    if not name:
        return jsonify({"error": "Search term is empty", "status": "error"}), 400

    # Get both sanitized versions of the input
    name_with_periods, name_without_periods = sanitize_input(name)
    
    # Important: Also create a version WITH periods even if the input doesn't have them
    # This handles searching "EJ" and finding "E.J's"
    if '.' not in name and len(name) >= 2:
        # Try adding periods between characters for abbreviation matching
        # E.g., "EJ" -> "E.J"
        with_periods_guess = '.'.join(list(name_without_periods))
        name_with_added_periods = with_periods_guess
    else:
        name_with_added_periods = name_with_periods
    
    # Create transformed versions with 's
    transformed_name = name_with_periods.replace("s", "'s")
    transformed_name_no_periods = name_without_periods.replace("s", "'s")
    transformed_with_added_periods = name_with_added_periods.replace("s", "'s")
    
    logger.info(f"Search input: '{name}', Sanitized: '{name_with_periods}', No periods: '{name_without_periods}', With added periods: '{name_with_added_periods}'")

    query = """
        SELECT restaurants.camis, restaurants.dba, restaurants.boro, restaurants.building, restaurants.street,
               restaurants.zipcode, restaurants.phone, restaurants.latitude, restaurants.longitude,
               restaurants.inspection_date, restaurants.critical_flag, restaurants.grade,
               restaurants.inspection_type, violations.violation_code, violations.violation_description
        FROM restaurants
        LEFT JOIN violations ON restaurants.camis = violations.camis AND restaurants.inspection_date = violations.inspection_date
        WHERE restaurants.dba ILIKE %s OR 
              restaurants.dba ILIKE %s OR
              restaurants.dba ILIKE %s OR
              restaurants.dba ILIKE %s OR
              restaurants.dba ILIKE %s
        ORDER BY 
            CASE 
                WHEN UPPER(restaurants.dba) = UPPER(%s) THEN 0  -- Exact match with original
                WHEN UPPER(restaurants.dba) = UPPER(%s) THEN 0  -- Exact match without periods
                WHEN UPPER(restaurants.dba) LIKE UPPER(%s) THEN 1  -- Starts with original
                WHEN UPPER(restaurants.dba) LIKE UPPER(%s) THEN 1  -- Starts with no periods
                WHEN UPPER(restaurants.dba) LIKE UPPER(%s) THEN 1  -- Starts with added periods
                ELSE 3                                             -- Other matches
            END,
            restaurants.dba  -- Then sort alphabetically
    """
    
    # Parameters for WHERE clause
    where_params = [
        f"%{name_with_periods}%",           # Original term
        f"%{transformed_name}%",            # Original with 's
        f"%{name_without_periods}%",        # Without periods
        f"%{transformed_name_no_periods}%", # Without periods with 's
        f"%{name_with_added_periods}%"      # With added periods (for EJ -> E.J)
    ]
    
    # Parameters for ORDER BY clause
    order_params = [
        name_with_periods,                  # Exact match with original
        name_without_periods,               # Exact match without periods
        f"{name_with_periods}%",            # Starts with original
        f"{name_without_periods}%",         # Starts with no periods
        f"{name_with_added_periods}%"       # Starts with added periods
    ]
    
    params = where_params + order_params

    try:
        with DatabaseConnection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                if not results:
                    logger.info(f"No results for search term: {name}")
                    return jsonify([])

                # Process results into the expected format
                restaurant_dict = {}
                for row in results:
                    restaurant_data = dict(zip(columns, row))
                    camis = restaurant_data['camis']
                    inspection_date = restaurant_data['inspection_date']

                    if camis not in restaurant_dict:
                        restaurant_dict[camis] = {
                            "camis": camis,
                            "dba": restaurant_data['dba'],
                            "boro": restaurant_data['boro'],
                            "building": restaurant_data['building'],
                            "street": restaurant_data['street'],
                            "zipcode": restaurant_data['zipcode'],
                            "phone": restaurant_data['phone'],
                            "latitude": restaurant_data['latitude'],
                            "longitude": restaurant_data['longitude'],
                            "inspections": {}
                        }
                    
                    inspections = restaurant_dict[camis]["inspections"]
                    if inspection_date not in inspections:
                        inspections[inspection_date] = {
                            "inspection_date": inspection_date,
                            "critical_flag": restaurant_data['critical_flag'],
                            "grade": restaurant_data['grade'],
                            "inspection_type": restaurant_data['inspection_type'],
                            "violations": []
                        }
                    
                    if restaurant_data['violation_code']:
                        violation = {
                            "violation_code": restaurant_data['violation_code'],
                            "violation_description": restaurant_data['violation_description']
                        }
                        if violation not in inspections[inspection_date]["violations"]:
                            inspections[inspection_date]["violations"].append(violation)

                # Convert to list for the response
                formatted_results = []
                for restaurant in restaurant_dict.values():
                    restaurant["inspections"] = list(restaurant["inspections"].values())
                    formatted_results.append(restaurant)

                # Log search results summary
                logger.info(f"Search for '{name}' returned {len(formatted_results)} restaurants")
                return jsonify(formatted_results)
                
    except Exception as e:
        logger.error(f"Error during search: {e}")
        return jsonify({"error": str(e), "status": "error"}), 500

@app.route('/recent', methods=['GET'])
def recent_restaurants():
    days = request.args.get('days', '7')
    try:
        days = int(days)
    except ValueError:
        days = 7

    # This is the corrected PostgreSQL interval syntax
    query = """
        SELECT r.camis, r.dba, r.boro, r.building, r.street, r.zipcode, r.phone,
               r.latitude, r.longitude, r.grade, r.inspection_date
        FROM restaurants r
        WHERE r.grade IN ('A', 'B', 'C')
          AND r.inspection_date >= NOW() - INTERVAL '%s days'
        ORDER BY r.inspection_date DESC, r.camis
        LIMIT 50
    """
    
    try:
        with DatabaseConnection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (days,))
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                formatted_results = [dict(zip(columns, row)) for row in results]
                return jsonify(formatted_results)
    except Exception as e:
        logger.error(f"Error fetching recent restaurants: {e}")
        return jsonify({"error": str(e), "status": "error"}), 500

@app.route('/test-db-connection', methods=['GET'])
def test_db_connection():
    try:
        with DatabaseConnection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                return jsonify({"status": "success", "message": "Database connection successful"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "The requested resource was not found", "status": "error"}), 404

@app.errorhandler(500)
def server_error(e):
    return jsonify({"error": "An internal server error occurred", "status": "error"}), 500

if __name__ == "__main__":
    app.run(
        host=APIConfig.HOST,
        port=APIConfig.PORT,
        debug=APIConfig.DEBUG
    )
