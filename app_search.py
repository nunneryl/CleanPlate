# In file: PREVIEW_app_search.py (Corrected)

import logging
from datetime import datetime, date
import json
import redis # Keep Redis import if used elsewhere, though Flask-Caching handles connection now
from decimal import Decimal # Keep Decimal import for JSON encoding
from flask import Flask, jsonify, request, abort, Response
from flask_cors import CORS # Import CORS
from flask_caching import Cache # Import Cache
import psycopg # Keep psycopg import
from psycopg.rows import dict_row
from db_manager import DatabaseConnection, DatabaseManager # Keep db_manager import
from config import RedisConfig, APIConfig, SentryConfig # Keep config import
from utils import normalize_search_term_for_hybrid # Keep utils import
import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration
from werkzeug.exceptions import HTTPException
import os # Import os for environment variables
import secrets # Import secrets for security
import threading # Import threading if run_database_update is used


# --- Sentry Initialization (Keep as is) ---
if SentryConfig.SENTRY_DSN:
    sentry_sdk.init(
        dsn=SentryConfig.SENTRY_DSN,
        integrations=[FlaskIntegration()],
        traces_sample_rate=1.0, # Adjust in production
        profiles_sample_rate=1.0, # Adjust in production
        send_default_pii=True # Be mindful of PII
    )
    logging.info("Sentry initialized.")
else:
    logging.info("Sentry DSN not found, skipping initialization.")

# --- Logging Setup (Match Production) ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)

# --- Flask App Initialization (Match Production) ---
app = Flask(__name__)
CORS(app) # Enable CORS like production

# --- CORRECTED CACHE CONFIGURATION (Match Production) ---
# Use Redis URL directly from environment for Flask-Caching
cache_config = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_REDIS_URL": os.environ.get('REDIS_URL'), # Get URL from environment
    "CACHE_DEFAULT_TIMEOUT": 300 # Default timeout like production
}
app.config.from_mapping(cache_config)
cache = Cache(app) # Initialize Cache AFTER app config is set
# --- END CACHE CORRECTION ---

# --- Custom JSON Encoder (Keep as is) ---
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

app.json_encoder = CustomJSONEncoder

# --- Helper Functions ---

# _execute_query: Use simpler version WITHOUT manual caching, rely on decorator
def _execute_query(sql, params=None, fetch_one=False):
    """Executes a SQL query."""
    try:
        with DatabaseConnection() as conn, conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute(sql, params)
            if fetch_one:
                result = cursor.fetchone()
            else:
                result = cursor.fetchall()
            return result
    except psycopg.Error as db_err:
        logger.error(f"Database query error: {db_err}", exc_info=True)
        # Abort within the helper to ensure consistent error handling
        abort(500, description="Database error occurred.")
    except Exception as e:
        logger.error(f"General error during query execution: {e}", exc_info=True)
        abort(500, description="Database error occurred.")


# _group_and_shape_results: Ensure this matches PRODUCTION exactly,
# but ADD the new google fields.
def _group_and_shape_results(all_rows, ordered_camis):
    """Groups violations under their respective restaurant inspection."""
    if not all_rows:
        return []

    # Use dict to group rows by camis first
    restaurant_rows_map = {camis_str: [] for camis_str in ordered_camis}
    for row in all_rows:
        camis_str = str(row['camis'])
        if camis_str in restaurant_rows_map:
            restaurant_rows_map[camis_str].append(row)

    final_results = []
    for camis_str in ordered_camis:
        rows_for_camis = restaurant_rows_map.get(camis_str)
        if not rows_for_camis:
            continue # Should not happen if logic is correct, but safe check

        # Use the first row for base restaurant info (they should be the same)
        base_info_row = rows_for_camis[0]
        restaurant_obj = {
            'camis': base_info_row['camis'],
            'dba': base_info_row['dba'],
            'boro': base_info_row['boro'],
            'building': base_info_row['building'],
            'street': base_info_row['street'],
            'zipcode': base_info_row['zipcode'],
            'phone': base_info_row['phone'],
            'cuisine_description': base_info_row['cuisine_description'],
            'latitude': base_info_row['latitude'],
            'longitude': base_info_row['longitude'],
            'foursquare_fsq_id': base_info_row.get('foursquare_fsq_id'), # Include if selected
            'google_place_id': base_info_row.get('google_place_id'), # Include if selected
            # --- ADD NEW FIELDS HERE ---
            'google_rating': base_info_row['google_rating'],
            'google_review_count': base_info_row['google_review_count'],
            'website': base_info_row['website'],
            'hours': base_info_row['hours'],
            'price_level': base_info_row['price_level'],
            # --- END NEW FIELDS ---
            'inspections': []
        }

        # Group inspections and violations
        inspections_map = {}
        for row in rows_for_camis:
            insp_date_iso = row['inspection_date'].isoformat()
            if insp_date_iso not in inspections_map:
                inspections_map[insp_date_iso] = {
                    'inspection_date': row['inspection_date'],
                    'action': row['action'],
                    'critical_flag': row['critical_flag'],
                    'grade': row['grade'],
                    'grade_date': row['grade_date'],
                    'inspection_type': row['inspection_type'],
                    'score': row.get('score'), # Include score if selected
                    'violations': []
                }

            # Add violation if present for this inspection row
            if row.get('violation_code') or row.get('violation_description'):
                 # Avoid duplicate violations if JOIN produced multiple rows for same violation
                 violation_tuple = (row.get('violation_code'), row.get('violation_description'))
                 existing_violations = {
                     (v.get('violation_code'), v.get('violation_description'))
                     for v in inspections_map[insp_date_iso]['violations']
                 }
                 if violation_tuple not in existing_violations:
                     inspections_map[insp_date_iso]['violations'].append({
                        'violation_code': row.get('violation_code'),
                        'violation_description': row.get('violation_description')
                    })

        # Sort inspections by date descending and add to restaurant object
        sorted_inspections = sorted(inspections_map.values(), key=lambda x: x['inspection_date'], reverse=True)
        restaurant_obj['inspections'] = sorted_inspections
        final_results.append(restaurant_obj)

    return final_results


# --- API Routes ---

# CORRECTED search_restaurants to match PRODUCTION logic
@app.route('/search', methods=['GET'])
@cache.cached(timeout=300, query_string=True) # Use cache decorator
def search_restaurants():
    query = request.args.get('name', '').strip()
    limit = request.args.get('per_page', 25, type=int)
    page = request.args.get('page', 1, type=int)
    offset = (page - 1) * limit
    grade = request.args.get('grade')
    boro = request.args.get('boro')
    cuisine = request.args.get('cuisine')
    sort_param = request.args.get('sort', 'relevance')

    if not query and not grade and not boro and not cuisine:
        logger.info("Search request with no query or filters, returning empty.")
        return jsonify([])

    normalized_query = normalize_search_term_for_hybrid(query) if query else None

    where_clauses = ["TRUE"]
    params_list = []

    if normalized_query:
        where_clauses.append("(similarity(r.dba_normalized_search, %s) > 0.2 OR r.dba ILIKE %s)")
        params_list.extend([normalized_query, f"%{query}%"])
    elif query:
        where_clauses.append("r.dba ILIKE %s")
        params_list.append(f"%{query}%")

    if grade and grade.upper() in ['A', 'B', 'C', 'P', 'Z', 'N']:
         if grade.upper() in ['P', 'Z', 'N']:
             where_clauses.append("r.grade IN ('P', 'Z', 'N')")
         else:
             where_clauses.append("r.grade = %s")
             params_list.append(grade.upper())
    if boro and boro != 'Any':
        where_clauses.append("r.boro = %s")
        params_list.append(boro.title())
    if cuisine and cuisine != 'Any':
        where_clauses.append("r.cuisine_description = %s")
        params_list.append(cuisine)

    where_sql = " AND ".join(where_clauses)

    order_by_sql_parts = []
    base_params_count = len(params_list)

    if normalized_query and sort_param == 'relevance':
        params_list.insert(0, normalized_query)
        order_by_sql_parts.append("similarity(r.dba_normalized_search, %s) DESC")

    sort_field = "r.inspection_date"
    sort_direction = "DESC"
    if sort_param == 'date_asc':
        sort_direction = "ASC"
    elif sort_param == 'name_asc':
        sort_field = "r.dba"
        sort_direction = "ASC"
    elif sort_param == 'name_desc':
        sort_field = "r.dba"
        sort_direction = "DESC"

    order_by_sql_parts.append(f"{sort_field} {sort_direction}")
    order_by_sql_parts.append("r.camis")
    order_by_sql = ", ".join(order_by_sql_parts)

    # Use the PRODUCTION SQL structure
    sql = f"""
        SELECT r.*, v.violation_code, v.violation_description
        FROM (
            SELECT DISTINCT r_inner.camis
            FROM restaurants r_inner
            WHERE {where_sql} -- Apply filters to find relevant CAMIS
            ORDER BY {order_by_sql} -- Apply sort to determine which CAMIS are on this page
            LIMIT %s OFFSET %s
        ) AS paged_restaurants
        JOIN restaurants r ON paged_restaurants.camis = r.camis
        LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date
        WHERE {where_sql} -- Apply filters AGAIN to get all inspections/violations for the paged CAMIS
        ORDER BY {order_by_sql}, r.inspection_date DESC; -- Final sort for grouping logic
    """

    final_params_list = list(params_list) # Create a mutable list
    # Add WHERE params again (excluding potential similarity sort param if it was added)
    final_params_list += params_list[len(params_list)-base_params_count:]
    final_params_list += [limit, offset] # Add LIMIT and OFFSET

    params_tuple = tuple(final_params_list)

    try:
        all_rows = _execute_query(sql, params_tuple)
    except Exception as e:
         logger.error(f"Search query failed: {e}", exc_info=True)
         return jsonify({"error": "Search failed", "details": str(e)}), 500

    ordered_camis = []
    seen_camis = set()
    if all_rows:
        for row in all_rows:
            camis_str = str(row['camis'])
            if camis_str not in seen_camis:
                ordered_camis.append(camis_str)
                seen_camis.add(camis_str)
                # No early break needed here, let _group_and_shape_results handle filtering to ordered_camis
    else:
        all_rows = []

    # --- Use the production _group_and_shape_results ---
    # Ensure _group_and_shape_results includes google_rating etc.
    grouped_data = _group_and_shape_results(all_rows, ordered_camis)

    logger.info(f"Search for '{query}' found {len(ordered_camis)} unique restaurants, returning grouped data.")
    return jsonify(grouped_data)


# --- Keep /restaurant/<camis> endpoint (ensure _group_and_shape_results includes new fields) ---
@app.route('/restaurant/<string:camis>', methods=['GET'])
@cache.cached(timeout=86400, key_prefix='restaurant:%s') # Use decorator, key_prefix uses CAMIS
def get_restaurant_details(camis):
    if not camis.isdigit():
        abort(400, description="Invalid CAMIS format.")

    # --- Ensure ALL fields needed are selected, including NEW ones ---
    sql = """
        SELECT
            r.*, -- Select all columns from restaurants table
            v.violation_code, v.violation_description
        FROM restaurants r
        LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date
        WHERE r.camis = %s
        ORDER BY r.inspection_date DESC;
    """
    params = (camis,)
    rows = _execute_query(sql, params)

    if not rows:
        abort(404, description="Restaurant not found.")

    # Use _group_and_shape_results (ensure it handles new fields)
    # The function expects ordered_camis, pass just this one camis
    grouped_data = _group_and_shape_results(rows, [camis])

    # _group_and_shape_results returns a list, return the first (only) element
    return jsonify(grouped_data[0] if grouped_data else None)


# --- Keep /recently-graded endpoint (ensure SELECT includes google_rating) ---
@app.route('/recently-graded', methods=['GET'])
@cache.cached(timeout=3600, query_string=True) # Use decorator
def get_recently_graded():
    limit = request.args.get('limit', 50, type=int)
    offset = request.args.get('offset', 0, type=int)

    sql = """
        WITH RankedInspections AS (
            SELECT
                r.*, -- Select all from restaurants
                ROW_NUMBER() OVER(PARTITION BY r.camis ORDER BY r.inspection_date DESC) as rn
            FROM restaurants r
            WHERE r.grade IS NOT NULL AND r.grade NOT IN ('', 'N', 'Z', 'P') -- Select only A, B, C
            AND r.grade_date IS NOT NULL
        )
        SELECT * FROM RankedInspections -- Includes google_rating etc. because of r.*
        WHERE rn = 1
        ORDER BY grade_date DESC, inspection_date DESC
        LIMIT %s OFFSET %s;
    """
    params = (limit, offset)
    results = _execute_query(sql, params)

    # Production shaping seems different, let's use a simpler version
    # matching the original preview logic but keeping enriched fields if needed
    shaped_results = []
    if results:
        for row in results:
            shaped_results.append({
                'camis': row['camis'],
                'dba': row['dba'],
                'boro': row['boro'],
                'building': row['building'],
                'street': row['street'],
                'zipcode': row['zipcode'],
                'grade': row['grade'],
                'grade_date': row['grade_date'],
                'inspection_date': row['inspection_date'],
                'cuisine_description': row['cuisine_description'],
                'latitude': row['latitude'],
                'longitude': row['longitude'],
                'google_rating': row['google_rating'] # Include rating
                 # Add other enriched fields if needed for this view
            })

    return jsonify(shaped_results)


# --- Keep /lists/recent-actions & /grade-updates endpoint (ensure SELECT includes google_rating) ---
# Assuming production has both routes pointing to the same function
@app.route('/lists/recent-actions', methods=['GET'])
@app.route('/grade-updates', methods=['GET'])
@cache.cached(timeout=3600, query_string=True) # Use decorator
def get_grade_updates():
    limit = request.args.get('limit', 50, type=int)
    offset = request.args.get('offset', 0, type=int)
    update_type = request.args.get('type', 'finalized')

    if update_type not in ['finalized', 'closed', 'reopened']:
         abort(400, description="Invalid update type.")

    base_sql_with = """
        WITH LatestRestaurantInspection AS (
            SELECT
                r.*, -- Select all fields from restaurants
                ROW_NUMBER() OVER(PARTITION BY r.camis ORDER BY r.inspection_date DESC) as rn
            FROM restaurants r
        ),
        LatestRestaurantState AS (
            SELECT * FROM LatestRestaurantInspection WHERE rn = 1
        )
    """
    params = [limit, offset] # Start params list

    if update_type == 'finalized':
        sql = base_sql_with + """
            SELECT
                gu.restaurant_camis AS camis,
                lr.*, -- Select all fields from latest state
                gu.previous_grade, gu.new_grade, gu.update_date AS finalized_date
            FROM grade_updates gu
            JOIN LatestRestaurantState lr ON gu.restaurant_camis = lr.camis
                AND gu.inspection_date = lr.inspection_date -- Match specific inspection
            WHERE gu.update_type = 'finalized'
            ORDER BY gu.update_date DESC
            LIMIT %s OFFSET %s;
        """
    elif update_type == 'closed':
         sql = base_sql_with + """
             SELECT lr.* -- Select all fields from latest state
             FROM LatestRestaurantState lr
             WHERE lr.action = 'Establishment Closed by DOHMH.'
             ORDER BY lr.inspection_date DESC
             LIMIT %s OFFSET %s;
         """
    else: # reopened
        sql = base_sql_with + """
             WITH PreviousInspections AS (
                 SELECT
                     r.camis, r.action, r.inspection_date,
                     ROW_NUMBER() OVER(PARTITION BY r.camis ORDER BY r.inspection_date DESC) as rn
                 FROM restaurants r
             ),
             SecondLatestState AS ( SELECT * FROM PreviousInspections WHERE rn = 2 )
             SELECT lr.* -- Select all fields from latest state
             FROM LatestRestaurantState lr
             JOIN SecondLatestState sls ON lr.camis = sls.camis
             WHERE lr.action != 'Establishment Closed by DOHMH.'
             AND sls.action = 'Establishment Closed by DOHMH.'
             ORDER BY lr.inspection_date DESC
             LIMIT %s OFFSET %s;
         """

    results = _execute_query(sql, tuple(params))

    # Shape results: Convert rows to simple dicts, add update_type for clarity
    shaped_results = []
    if results:
         for row in results:
             row_dict = dict(row) # Convert psycopg dict_row to standard dict
             row_dict['update_type'] = update_type # Add the type requested
             # Rename new_grade/previous_grade for consistency if needed by iOS
             if update_type == 'finalized':
                 row_dict['grade'] = row_dict.pop('new_grade', None)
                 row_dict['grade_date'] = row_dict.pop('finalized_date', None) # Use finalized_date as grade_date
             shaped_results.append(row_dict)

    return jsonify(shaped_results)


# --- Keep /clear-cache endpoint (Use Flask-Caching method) ---
@app.route('/clear-cache', methods=['POST'])
def clear_cache():
    provided_key = request.headers.get('X-Update-Secret')
    expected_key = APIConfig.UPDATE_SECRET_KEY
    if not expected_key or not provided_key or not secrets.compare_digest(provided_key, expected_key):
        logger.warning("Unauthorized cache clear attempt.")
        abort(403, description="Unauthorized.")

    try:
        cleared = cache.clear() # Use Flask-Caching's clear method
        if cleared:
             logger.info("Cache cleared successfully via API endpoint.")
             return jsonify({"status": "success", "message": "Cache cleared."}), 200
        else:
             # cache.clear() might return False if cache type doesn't support clear
             logger.warning("Cache clear command executed, but cache might not support clearing or was already empty.")
             return jsonify({"status": "success", "message": "Cache clear attempted."}), 200
    except Exception as e:
        logger.error(f"Error during cache clear: {e}", exc_info=True)
        abort(500, description="Failed to clear cache.")


# --- Keep Error Handlers (HTTPException and General Exception) ---
@app.errorhandler(HTTPException)
def handle_http_exception(e):
    # (Keep existing implementation)
    response = e.get_response()
    response.data = json.dumps({
        "code": e.code, "name": e.name, "description": e.description,
    })
    response.content_type = "application/json"
    logging.error(f"{e.code} {e.name}: {e.description} for request {request.url}")
    return response

@app.errorhandler(Exception)
def handle_general_exception(e):
    # (Keep existing implementation)
    logging.error("An unexpected server error occurred.", exc_info=True)
    if SentryConfig.SENTRY_DSN: sentry_sdk.capture_exception(e)
    response = jsonify({
        "code": 500, "name": "Internal Server Error",
        "description": "An unexpected error occurred on the server.",
    })
    response.status_code = 500
    return response

# --- Keep Main block (DatabaseManager init, app.run) ---
if __name__ == '__main__':
    # Initialize DB pool when running directly (Gunicorn might handle this differently)
    try:
         DatabaseManager.initialize_pool()
    except Exception as e:
         logger.critical(f"Failed to initialize database pool on startup: {e}", exc_info=True)
         # Optionally exit if DB pool is critical for startup
         # exit(1)

    logger.info(f"Starting Flask app on {APIConfig.HOST}:{APIConfig.PORT} with DEBUG={APIConfig.DEBUG}")
    # Use waitress or another production server instead of app.run in production
    # For development:
    app.run(host=APIConfig.HOST, port=APIConfig.PORT, debug=APIConfig.DEBUG)
