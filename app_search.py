# app_search.py - The Definitive Final Version

import os
import re
import logging
import json
import threading
import secrets
from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import psycopg2.extras

# Local application imports
# Ensure db_manager.py and config.py are in the same directory.
try:
    from db_manager import DatabaseConnection, get_redis_client
    from config import APIConfig
    from update_database import run_database_update
    update_logic_imported = True
except ImportError:
    update_logic_imported = False
    # This allows the app to run even if update_database.py is missing,
    # though the /trigger-update endpoint will be disabled.
    def run_database_update(*args, **kwargs):
        logging.error("run_database_update is not available due to import error.")
        pass

# --- Basic Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)
app = Flask(__name__)
CORS(app)

# --- Normalization Function (Apostrophe-handling logic) ---
def normalize_search_term_for_hybrid(text):
    """
    Cleans a search term by making it lowercase, removing accents,
    and stripping out special characters like apostrophes, periods, and dashes.
    This logic is critical for matching user input to the database.
    """
    if not isinstance(text, str): return ''
    normalized_text = text.lower()
    normalized_text = normalized_text.replace('&', ' and ')
    
    # Efficiently handle common accent characters
    accent_map = { 'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e', 'á': 'a', 'à': 'a', 'â': 'a', 'ä': 'a', 'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i', 'ó': 'o', 'ò': 'o', 'ô': 'o', 'ö': 'o', 'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u', 'ç': 'c', 'ñ': 'n' }
    for accented, unaccented in accent_map.items():
        normalized_text = normalized_text.replace(accented, unaccented)
    
    # CRITICAL: Remove apostrophes entirely for matching (e.g., "joe's" -> "joes")
    normalized_text = re.sub(r"[']", "", normalized_text)
    # Replace other specific punctuation with a space to separate terms
    normalized_text = re.sub(r"[./-]", " ", normalized_text)
    # Clean up any other unwanted characters and extra spaces.
    normalized_text = re.sub(r"[^a-z0-9\s]", "", normalized_text)
    normalized_text = re.sub(r"\s+", " ", normalized_text)
    
    return normalized_text.strip()

# --- THE FINAL, WORKING SEARCH ENDPOINT ---
@app.route('/search', methods=['GET'])
def search():
    # 1. Get and Validate All Request Parameters
    search_term = request.args.get('name', '').strip()
    grade_filter = request.args.get('grade', type=str)   # Optional grade filter (e.g., 'A', 'B', 'C')
    boro_filter = request.args.get('boro', type=str)     # Optional boro filter (e.g., 'Manhattan')
    sort_by = request.args.get('sort', 'relevance', type=str) # Sort order
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 25))

    # A search term is required for this endpoint.
    if not search_term:
        return jsonify([])

    # Normalize the search term using the same logic as the database.
    normalized_search = normalize_search_term_for_hybrid(search_term)
    if not normalized_search:
        return jsonify([])

    # 2. Dynamically Build the Database Query
    params = []
    
    # Base search condition against the pre-normalized column for efficiency.
    where_conditions = ["(dba_normalized_search ILIKE %s OR similarity(dba_normalized_search, %s) > 0.4)"]
    params.extend([f"%{normalized_search}%", normalized_search])

    # Add new optional filters to the WHERE clause if they are provided in the request.
    if grade_filter:
        where_conditions.append("grade = %s")
        params.append(grade_filter)
    if boro_filter:
        # Using ILIKE for borough to be flexible with casing (e.g., 'manhattan', 'Manhattan')
        where_conditions.append("boro ILIKE %s")
        params.append(boro_filter)
    
    # Combine all conditions with "AND".
    where_clause = " AND ".join(where_conditions)

    # Determine the sorting order based on the 'sort' parameter.
    order_by_clause = ""
    order_params = []
    if sort_by == 'name_asc':
        order_by_clause = "ORDER BY dba ASC"
    elif sort_by == 'name_desc':
        order_by_clause = "ORDER BY dba DESC"
    else: # Default to relevance-based sorting.
        order_by_clause = """
        ORDER BY
            CASE
                WHEN dba_normalized_search = %s THEN 0 -- Exact match first
                WHEN dba_normalized_search LIKE %s THEN 1 -- Starts with match
                ELSE 2
            END,
            similarity(dba_normalized_search, %s) DESC, -- Similarity score
            length(dba_normalized_search) -- Shorter names that are similar are often better matches
        """
        # Note: LIKE is used here instead of ILIKE because the term is already lowercased.
        order_params.extend([normalized_search, f"{normalized_search}%", normalized_search])

    # Add pagination logic to the query.
    offset = (page - 1) * per_page
    pagination_params = [per_page, offset]

    # Combine all parameters in the correct order for the final query.
    final_params = params + order_params + pagination_params
    
    # This main query uses a Common Table Expression (CTE) for clarity and performance.
    # It first finds the latest inspection for each restaurant, then paginates those results,
    # and finally joins to get all necessary details.
    query = f"""
        WITH latest_restaurants AS (
            SELECT DISTINCT ON (camis) * FROM restaurants ORDER BY camis, inspection_date DESC
        ),
        paginated_camis AS (
            SELECT camis, dba, dba_normalized_search
            FROM latest_restaurants
            WHERE {where_clause}
            {order_by_clause}
            LIMIT %s OFFSET %s
        )
        SELECT 
            pc.camis, pc.dba, pc.dba_normalized_search,
            r.boro, r.building, r.street, r.zipcode, r.phone, r.latitude, r.longitude,
            r.inspection_date, r.critical_flag, r.grade, r.inspection_type,
            v.violation_code, v.violation_description, r.cuisine_description
        FROM paginated_camis pc
        JOIN restaurants r ON pc.camis = r.camis
        LEFT JOIN violations v ON pc.camis = v.camis AND r.inspection_date = v.inspection_date
        ORDER BY
            (CASE WHEN pc.dba_normalized_search = %s THEN 0 WHEN pc.dba_normalized_search LIKE %s THEN 1 ELSE 2 END),
            similarity(pc.dba_normalized_search, %s) DESC,
            length(pc.dba_normalized_search),
            pc.dba ASC,
            r.inspection_date DESC;
    """
    
    # Add the final sorting parameters, which are used again in the outer ORDER BY for consistency.
    final_params.extend([normalized_search, f"{normalized_search}%", normalized_search])

    # 3. Execute the Query and Process Results
    try:
        with DatabaseConnection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(query, tuple(final_params))
            results = cursor.fetchall()
    except Exception as e:
        logger.error(f"Database search failed for term '{search_term}': {e}", exc_info=True)
        return jsonify({"error": "Database query failed"}), 500

    if not results:
        return jsonify([])
        
    # 4. Format Results into a nested JSON structure for the client.
    # This groups all inspections and violations under a single restaurant object.
    restaurant_dict = {}
    for row in results:
        camis = str(row['camis']) # Use string for camis for JSON consistency
        if camis not in restaurant_dict:
            restaurant_dict[camis] = {
                # Select all restaurant-level fields, excluding those that belong to inspections/violations.
                k: v for k, v in row.items()
                if k not in ['violation_code', 'violation_description', 'inspection_date', 'critical_flag', 'grade', 'inspection_type']
            }
            # Initialize an empty dictionary to hold inspections.
            restaurant_dict[camis]['inspections'] = {}

        # Use the inspection date as a unique key for each inspection.
        insp_date_str = row['inspection_date'].isoformat()
        if insp_date_str not in restaurant_dict[camis]['inspections']:
            restaurant_dict[camis]['inspections'][insp_date_str] = {
                'inspection_date': insp_date_str,
                'grade': row['grade'],
                'critical_flag': row['critical_flag'],
                'inspection_type': row['inspection_type'],
                'violations': [] # Initialize empty list for violations for this inspection.
            }
        
        # If a violation exists in this row, add it to the correct inspection's violation list.
        if row['violation_code']:
            v_data = {'violation_code': row['violation_code'], 'violation_description': row['violation_description']}
            if v_data not in restaurant_dict[camis]['inspections'][insp_date_str]['violations']:
                restaurant_dict[camis]['inspections'][insp_date_str]['violations'].append(v_data)

    # Convert the dictionary of inspections into a sorted list.
    final_results = [
        {**data, 'inspections': sorted(list(data['inspections'].values()), key=lambda x: x['inspection_date'], reverse=True)}
        for data in restaurant_dict.values()
    ]
    
    return jsonify(final_results)

# --- Other Endpoints and Error Handlers ---

@app.route('/recent', methods=['GET'])
def recent_restaurants():
    """Placeholder for fetching recent restaurants. Not implemented in this feature."""
    return jsonify([])

@app.route('/trigger-update', methods=['POST'])
def trigger_update():
    """A secure endpoint to trigger a background database update."""
    if not update_logic_imported:
        return jsonify({"status": "error", "message": "Update logic is unavailable due to an import error."}), 500
    
    provided_key = request.headers.get('X-Update-Secret')
    expected_key = APIConfig.UPDATE_SECRET_KEY

    # Securely compare the provided key with the one in your environment variables.
    if not expected_key or not provided_key or not secrets.compare_digest(provided_key, expected_key):
        return jsonify({"status": "error", "message": "Unauthorized."}), 403
    
    # Run the database update in a separate thread to avoid blocking the API response.
    threading.Thread(target=run_database_update, args=(15,), daemon=True).start()
    return jsonify({"status": "success", "message": "Database update triggered in background."}), 202

@app.errorhandler(404)
def not_found_error_handler(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_server_error_handler(error):
    logger.error(f"Internal Server Error (500): {error}", exc_info=True)
    return jsonify({"error": "An internal server error occurred"}), 500

# This allows running the app directly for local testing (e.g., `python app_search.py`)
if __name__ == "__main__":
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", 8080)) # Railway provides the PORT variable
    app.run(host=host, port=port)
