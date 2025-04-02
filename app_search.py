import os
import re
import logging
from flask import Flask, jsonify, request, make_response
from flask_cors import CORS
from db_manager import DatabaseConnection
from config import APIConfig

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
        return ""
    # Normalize curly apostrophes to straight ones
    input_str = input_str.replace("'", "'").replace("'", "'")
    # Remove periods that might be in abbreviations (e.g., E.J's -> EJs)
    input_str = input_str.replace(".", "")
    # Keep apostrophes, spaces, alphanumeric characters
    return re.sub(r"[^\w\s']", "", input_str)

@app.route('/search', methods=['GET'])
def search():
    name = request.args.get('name', '').strip()
    if not name:
        return jsonify({"error": "Search term is empty", "status": "error"}), 400

    # Sanitize and prepare search term variations
    name = sanitize_input(name)
    
    # Create additional search patterns
    # 1. Original with 's added where applicable
    transformed_name_s = name.replace("s", "'s")
    
    # 2. Version with apostrophes removed
    no_apostrophe_name = name.replace("'", "")
    
    # 3. Version with spaces removed (for joined terms like EJ's -> EJs)
    compressed_name = name.replace(" ", "")
    
    # 4. Version that's just the initials (e.g., "E.J" from "E.J's Luncheonette")
    initials_match = re.match(r"([A-Za-z][\.]?[A-Za-z][\.]?)", name)
    initials = initials_match.group(1) if initials_match else ""

    logger.info(f"Search term: '{name}', Variations: '{transformed_name_s}', '{no_apostrophe_name}', '{compressed_name}', Initials: '{initials}'")

    query = """
        SELECT restaurants.camis, restaurants.dba, restaurants.boro, restaurants.building, restaurants.street,
               restaurants.zipcode, restaurants.phone, restaurants.latitude, restaurants.longitude,
               restaurants.inspection_date, restaurants.critical_flag, restaurants.grade,
               restaurants.inspection_type, violations.violation_code, violations.violation_description
        FROM restaurants
        LEFT JOIN violations ON restaurants.camis = violations.camis AND restaurants.inspection_date = violations.inspection_date
        WHERE 
            restaurants.dba ILIKE %s OR 
            restaurants.dba ILIKE %s OR 
            restaurants.dba ILIKE %s OR 
            restaurants.dba ILIKE %s OR
            restaurants.dba ILIKE %s
        ORDER BY 
            CASE 
                WHEN UPPER(restaurants.dba) = UPPER(%s) THEN 0  -- Exact match
                WHEN UPPER(restaurants.dba) LIKE UPPER(%s) THEN 1  -- Starts with search term
                WHEN UPPER(restaurants.dba) LIKE UPPER(%s) THEN 2  -- Contains search term as a whole word
                ELSE 3                                -- Contains search term as part of another word
            END,
            restaurants.dba  -- Then sort alphabetically
    """
    
    # Parameters for the WHERE clause
    search_term = f"%{name}%"  # Original term
    transformed_s_term = f"%{transformed_name_s}%"  # With 's
    no_apostrophe_term = f"%{no_apostrophe_name}%"  # Without apostrophes
    compressed_term = f"%{compressed_name}%"  # Without spaces
    initials_term = f"%{initials}%" if initials else "%_NONEXISTENT_%"  # Just initials
    
    # Parameters for the ORDER BY clause
    exact_match = name
    starts_with = f"{name}%"
    contains_word = f"% {name} %"
    
    params = [
        search_term, transformed_s_term, no_apostrophe_term, compressed_term, initials_term,  # WHERE params
        exact_match, starts_with, contains_word  # ORDER BY params
    ]

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
