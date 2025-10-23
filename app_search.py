# In file: PREVIEW_app_search.py (Corrected with View-Level Caching)

import logging
from datetime import datetime, date
import json
import redis # Keep Redis import
from decimal import Decimal # Keep Decimal import for JSON encoding
from flask import Flask, jsonify, request, abort, Response
from flask_cors import CORS # Import CORS from Production
from flask_caching import Cache # Import Cache from Production
import psycopg # Keep psycopg import
from psycopg.rows import dict_row
from db_manager import DatabaseConnection, DatabaseManager # Keep db_manager import
from config import RedisConfig, APIConfig, SentryConfig # Keep config import
from utils import normalize_search_term_for_hybrid # Keep utils import
import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration
from werkzeug.exceptions import HTTPException
import os # Import os like Production
import secrets # Import secrets like Production
import threading # Import threading like Production

# --- Sentry Initialization (Keep as is from Preview file) ---
if SentryConfig.SENTRY_DSN:
    sentry_sdk.init(
        dsn=SentryConfig.SENTRY_DSN, integrations=[FlaskIntegration()],
        traces_sample_rate=1.0, profiles_sample_rate=1.0, send_default_pii=True
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
cache_config = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_REDIS_URL": os.environ.get('REDIS_URL'), # Get URL from environment like Production
    "CACHE_DEFAULT_TIMEOUT": 300 # Default timeout like Production
}
app.config.from_mapping(cache_config)
cache = Cache(app) # Initialize Cache AFTER app config is set (like Production)
# --- END CACHE CORRECTION ---

# --- Custom JSON Encoder (Keep as is from Preview file) ---
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, date): return obj.isoformat()
        if isinstance(obj, Decimal): return float(obj)
        return super().default(obj)
app.json_encoder = CustomJSONEncoder

# --- Helper Functions ---

# _execute_query: Match Production structure (NO cache logic, NO use_cache param)
def _execute_query(sql, params=None, fetch_one=False):
    """Executes a SQL query."""
    try:
        with DatabaseConnection() as conn, conn.cursor(row_factory=dict_row) as cursor:
            # Production uses try-except around execute
            try:
                logger.debug(f"Executing SQL: {cursor.mogrify(sql, params).decode('utf-8') if params else sql}") # Log query
                cursor.execute(sql, params)
                if fetch_one: result = cursor.fetchone()
                else: result = cursor.fetchall()
                logger.debug(f"Query returned {len(result) if isinstance(result, list) else (1 if result else 0)} rows.")
                return result
            except psycopg.Error as db_err: # Catch specific DB errors like Production
                 # Log the specific error and query details
                 logger.error(f"Database query error during execution: {db_err} SQL: {sql} PARAMS: {params}", exc_info=True)
                 abort(500, description="Database query error occurred during execution.")
            except Exception as e: # Catch other potential errors during execution
                 logger.error(f"General error during query execution: {e} SQL: {sql} PARAMS: {params}", exc_info=True)
                 abort(500, description="General error during query execution.")
    except psycopg.Error as conn_err: # Catch connection errors like Production
        logger.error(f"Database connection error: {conn_err}", exc_info=True)
        abort(500, description="Database connection error occurred.")
    except Exception as e: # Catch other potential errors getting connection/cursor
        logger.error(f"Error getting DB connection or cursor: {e}", exc_info=True)
        abort(500, description="Error establishing database connection.")


# _group_and_shape_results: Ensure this matches PRODUCTION exactly,
# AND includes the NEW enriched fields.
def _group_and_shape_results(all_rows, ordered_camis):
    """Groups violations under their respective restaurant inspection."""
    if not all_rows: return []

    # Use dict to group rows by camis first (like Production)
    restaurant_rows_map = {camis_str: [] for camis_str in ordered_camis}
    for row in all_rows:
        camis_str = str(row['camis']) # Convert camis to string key like Production
        if camis_str in restaurant_rows_map:
            restaurant_rows_map[camis_str].append(row)
        # else: # Log if a row's camis wasn't in the expected ordered list
        #     logger.warning(f"Row found for CAMIS {camis_str} which was not in the ordered_camis list.")


    final_results = []
    for camis_str in ordered_camis:
        rows_for_camis = restaurant_rows_map.get(camis_str)
        if not rows_for_camis:
            logger.warning(f"No rows found for expected CAMIS {camis_str} during shaping.")
            continue # Skip if no rows were actually found for this camis

        base_info_row = rows_for_camis[0] # Base info from first row
        restaurant_obj = {
            # --- Fields exactly like PRODUCTION ---
            'camis': base_info_row['camis'], 'dba': base_info_row['dba'],
            'boro': base_info_row['boro'], 'building': base_info_row['building'],
            'street': base_info_row['street'], 'zipcode': base_info_row['zipcode'],
            'phone': base_info_row['phone'], 'cuisine_description': base_info_row['cuisine_description'],
            'latitude': base_info_row['latitude'], 'longitude': base_info_row['longitude'],
            'foursquare_fsq_id': base_info_row.get('foursquare_fsq_id'),
            'google_place_id': base_info_row.get('google_place_id'),
            # --- ADD/KEEP NEW FIELDS HERE ---
            'google_rating': base_info_row.get('google_rating'), # Use .get() for safety
            'google_review_count': base_info_row.get('google_review_count'),
            'website': base_info_row.get('website'),
            'hours': base_info_row.get('hours'),
            'price_level': base_info_row.get('price_level'),
            # --- END NEW FIELDS ---
            'inspections': [] # Initialize inspections list
        }

        # Group inspections and violations (like Production)
        inspections_map = {}
        processed_violation_keys = set() # Add set to prevent violation duplication like Production

        for row in rows_for_camis:
            try:
                 # Ensure inspection_date is valid before using as key
                 insp_date_obj = row.get('inspection_date') # Use .get() for safety
                 if not isinstance(insp_date_obj, date):
                      # Log or handle rows with missing/invalid inspection dates if necessary
                      # logger.warning(f"Skipping row for CAMIS {camis_str} due to invalid inspection_date: {insp_date_obj}")
                      continue
                 insp_date_iso = insp_date_obj.isoformat()

                 if insp_date_iso not in inspections_map:
                    inspections_map[insp_date_iso] = {
                        'inspection_date': row['inspection_date'],
                        'action': row.get('action'), # Use .get()
                        'critical_flag': row.get('critical_flag'),
                        'grade': row.get('grade'),
                        'grade_date': row.get('grade_date'),
                        'inspection_type': row.get('inspection_type'),
                        'score': row.get('score'),
                        'violations': []
                    }

                 # Add violation if present (like Production)
                 violation_code = row.get('violation_code')
                 violation_desc = row.get('violation_description')
                 if violation_code or violation_desc:
                      violation_key = (insp_date_iso, violation_code, violation_desc)
                      if violation_key not in processed_violation_keys:
                           inspections_map[insp_date_iso]['violations'].append({
                                'violation_code': violation_code,
                                'violation_description': violation_desc
                            })
                           processed_violation_keys.add(violation_key)
            except Exception as e:
                 # Log error processing a specific row but continue with others
                 logger.error(f"Error processing row during shaping for CAMIS {camis_str}, inspection {insp_date_iso}: {e}", exc_info=True)


        # Sort inspections by date descending (like Production)
        sorted_inspections = sorted(inspections_map.values(), key=lambda x: x['inspection_date'], reverse=True)
        restaurant_obj['inspections'] = sorted_inspections
        final_results.append(restaurant_obj)

    return final_results


# --- API Routes ---

# CORRECTED search_restaurants (Matches PRODUCTION logic, uses decorator)
@app.route('/search', methods=['GET'])
@cache.cached(timeout=300, query_string=True) # Cache decorator applied HERE
def search_restaurants():
    # --- Use parameters and logic exactly like PRODUCTION ---
    query = request.args.get('name', '').strip()
    limit = request.args.get('per_page', 25, type=int)
    page = request.args.get('page', 1, type=int)
    offset = (page - 1) * limit
    grade = request.args.get('grade')
    boro = request.args.get('boro')
    cuisine = request.args.get('cuisine')
    sort_param = request.args.get('sort', 'relevance') # Default like Production

    # --- (Keep WHERE and ORDER BY logic exactly like Production/Previous Correct Version) ---
    if not query and not grade and not boro and not cuisine:
        logger.info("Search request with no query or filters, returning empty.")
        return jsonify([])

    normalized_query = normalize_search_term_for_hybrid(query) if query else None
    where_clauses = ["TRUE"]
    params_list = []
    if normalized_query:
        where_clauses.append("(similarity(r_inner.dba_normalized_search, %s) > 0.2 OR r_inner.dba ILIKE %s)")
        params_list.extend([normalized_query, f"%{query}%"])
    elif query:
        where_clauses.append("r_inner.dba ILIKE %s")
        params_list.append(f"%{query}%")
    # (Add grade, boro, cuisine clauses like before, using r_inner alias)
    if grade and grade.upper() in ['A', 'B', 'C', 'P', 'Z', 'N']:
        where_clauses.append("r_inner.grade = %s" if grade.upper() in ['A','B','C'] else "r_inner.grade IN ('P', 'Z', 'N')")
        if grade.upper() in ['A','B','C']: params_list.append(grade.upper())
    if boro and boro != 'Any':
        where_clauses.append("r_inner.boro = %s")
        params_list.append(boro.title())
    if cuisine and cuisine != 'Any':
        where_clauses.append("r_inner.cuisine_description = %s")
        params_list.append(cuisine)

    where_sql_for_subquery = " AND ".join(where_clauses)
    order_by_sql_parts = []
    sort_params_list = []
    if normalized_query and sort_param == 'relevance':
        sort_params_list.append(normalized_query)
        order_by_sql_parts.append("similarity(r_inner.dba_normalized_search, %s) DESC")
    sort_field = "r_inner.inspection_date"; sort_direction = "DESC"
    if sort_param == 'date_asc': sort_direction = "ASC"
    elif sort_param == 'name_asc': sort_field = "r_inner.dba"; sort_direction = "ASC"
    elif sort_param == 'name_desc': sort_field = "r_inner.dba"; sort_direction = "DESC"
    order_by_sql_parts.append(f"{sort_field} {sort_direction}"); order_by_sql_parts.append("r_inner.camis")
    order_by_sql_for_subquery = ", ".join(order_by_sql_parts)
    # --- (End WHERE and ORDER BY logic) ---

    # --- Use the exact SQL query structure from PRODUCTION ---
    sql = f"""
        SELECT r.*, v.violation_code, v.violation_description
        FROM (
            SELECT DISTINCT r_inner.camis
            FROM restaurants r_inner
            WHERE {where_sql_for_subquery} -- Apply filters using r_inner alias
            ORDER BY {order_by_sql_for_subquery} -- Apply sort using r_inner alias
            LIMIT %s OFFSET %s
        ) AS paged_restaurants
        JOIN restaurants r ON paged_restaurants.camis = r.camis
        LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date
        -- Outer WHERE clause IS needed to re-apply filters (match Production)
        WHERE {where_sql_for_subquery.replace('r_inner.', 'r.')}
        ORDER BY {order_by_sql_for_subquery.replace('r_inner.', 'r.')}, r.inspection_date DESC;
    """

    # --- CORRECTED Parameter Construction (Match PRODUCTION Logic) ---
    final_params_list = []
    final_params_list.extend(params_list)       # Subquery WHERE params
    final_params_list.extend(sort_params_list)  # Subquery ORDER BY params
    final_params_list.append(limit)             # LIMIT param
    final_params_list.append(offset)            # OFFSET param
    final_params_list.extend(params_list)       # Outer WHERE params (duplicate required)
    params_tuple = tuple(final_params_list)
    # --- END Parameter Correction ---

    logger.debug(f"Executing search SQL (Preview): {sql}")
    logger.debug(f"With parameters (Preview count: {len(params_tuple)}): {params_tuple}")

    try:
        # Call _execute_query WITHOUT use_cache argument
        all_rows = _execute_query(sql, params_tuple)
    except Exception as e:
         logger.error(f"Search query failed.", exc_info=True) # Keep detailed log
         return jsonify({"error": "Search failed"}), 500 # Simple error to client

    # --- Determine ordered CAMIS list for grouping (like PRODUCTION) ---
    ordered_camis = []
    seen_camis = set()
    if all_rows:
        for row in all_rows:
            camis_str = str(row['camis'])
            if camis_str not in seen_camis:
                 ordered_camis.append(camis_str)
                 seen_camis.add(camis_str)
                 # Limit the number of unique restaurants based on 'limit' param
                 if len(ordered_camis) >= limit:
                      break
    else:
        all_rows = [] # Ensure it's a list for the next step

    # --- Use the production _group_and_shape_results ---
    # Ensure _group_and_shape_results includes google_rating etc.
    grouped_data = _group_and_shape_results(all_rows, ordered_camis)

    logger.info(f"Search for '{query}' found {len(ordered_camis)} unique restaurants, returning grouped data.")
    return jsonify(grouped_data) # Return the grouped structure from production logic


# --- Keep /restaurant/<camis> endpoint (ensure decorator is present) ---
@app.route('/restaurant/<string:camis>', methods=['GET'])
@cache.cached(timeout=86400) # Decorator handles cache based on path/camis
def get_restaurant_details(camis):
    if not camis.isdigit(): abort(400, description="Invalid CAMIS format.")

    # Select ALL columns from restaurants, plus violation info (like Production)
    sql = """
        SELECT r.*, v.violation_code, v.violation_description
        FROM restaurants r
        LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date
        WHERE r.camis = %s
        ORDER BY r.inspection_date DESC;
    """
    params = (camis,)
    # Call _execute_query WITHOUT use_cache argument
    rows = _execute_query(sql, params)

    if not rows: abort(404, description="Restaurant not found.")

    # Use _group_and_shape_results (ensure it handles new fields)
    grouped_data = _group_and_shape_results(rows, [camis])
    return jsonify(grouped_data[0] if grouped_data else None)


# --- Keep /recently-graded endpoint (ensure decorator is present) ---
@app.route('/recently-graded', methods=['GET'])
@cache.cached(timeout=3600, query_string=True) # Decorator handles cache
def get_recently_graded():
    limit = request.args.get('limit', 50, type=int)
    offset = request.args.get('offset', 0, type=int)

    # Use Production SQL structure
    sql = """
        WITH RankedInspections AS (
            SELECT r.*, ROW_NUMBER() OVER(PARTITION BY r.camis ORDER BY r.inspection_date DESC) as rn
            FROM restaurants r
            WHERE r.grade IS NOT NULL AND r.grade NOT IN ('', 'N', 'Z', 'P')
            AND r.grade_date IS NOT NULL
        )
        SELECT * FROM RankedInspections
        WHERE rn = 1
        ORDER BY grade_date DESC, inspection_date DESC
        LIMIT %s OFFSET %s;
    """
    params = (limit, offset)
    # Call _execute_query WITHOUT use_cache argument
    results = _execute_query(sql, params)

    # Use simple shaping like Production file
    shaped_results = [dict(row) for row in results] if results else []
    return jsonify(shaped_results)


# --- Keep /lists/recent-actions & /grade-updates endpoint (ensure decorator is present) ---
@app.route('/lists/recent-actions', methods=['GET'])
@app.route('/grade-updates', methods=['GET'])
@cache.cached(timeout=3600, query_string=True) # Decorator handles cache
def get_grade_updates():
    limit = request.args.get('limit', 50, type=int)
    offset = request.args.get('offset', 0, type=int)
    update_type = request.args.get('type', 'finalized')

    if update_type not in ['finalized', 'closed', 'reopened']:
         abort(400, description="Invalid update type.")

    # Use Production SQL structure
    base_sql_with = """
        WITH LatestRestaurantInspection AS (
            SELECT r.*, ROW_NUMBER() OVER(PARTITION BY r.camis ORDER BY r.inspection_date DESC) as rn
            FROM restaurants r
        ), LatestRestaurantState AS ( SELECT * FROM LatestRestaurantInspection WHERE rn = 1 )
    """
    params = [limit, offset]

    # (Keep SQL logic for finalized, closed, reopened exactly like Production/Previous)
    if update_type == 'finalized':
        sql = base_sql_with + """ SELECT gu.restaurant_camis AS camis, lr.*, gu.previous_grade, gu.new_grade, gu.update_date AS finalized_date FROM grade_updates gu JOIN LatestRestaurantState lr ON gu.restaurant_camis = lr.camis WHERE gu.update_type = 'finalized' AND gu.inspection_date = lr.inspection_date ORDER BY gu.update_date DESC LIMIT %s OFFSET %s; """
    elif update_type == 'closed':
        sql = base_sql_with + """ SELECT lr.* FROM LatestRestaurantState lr WHERE lr.action = 'Establishment Closed by DOHMH.' ORDER BY lr.inspection_date DESC LIMIT %s OFFSET %s; """
    else: # reopened
        sql = base_sql_with + """ WITH PreviousInspections AS ( SELECT r.camis, r.action, r.inspection_date, ROW_NUMBER() OVER(PARTITION BY r.camis ORDER BY r.inspection_date DESC) as rn FROM restaurants r ), SecondLatestState AS ( SELECT * FROM PreviousInspections WHERE rn = 2 ) SELECT lr.* FROM LatestRestaurantState lr JOIN SecondLatestState sls ON lr.camis = sls.camis WHERE lr.action != 'Establishment Closed by DOHMH.' AND sls.action = 'Establishment Closed by DOHMH.' ORDER BY lr.inspection_date DESC LIMIT %s OFFSET %s; """

    # Call _execute_query WITHOUT use_cache argument
    results = _execute_query(sql, tuple(params))

    # Use simple shaping like Production file
    shaped_results = [dict(row) for row in results] if results else []
    if shaped_results:
         for item in shaped_results:
             item['update_type'] = update_type
             if update_type == 'finalized':
                  item['grade'] = item.pop('new_grade', None)
                  item['grade_date'] = item.pop('finalized_date', None)

    return jsonify(shaped_results)


# --- Keep /clear-cache endpoint (Match Production) ---
@app.route('/clear-cache', methods=['POST'])
def clear_cache():
    provided_key = request.headers.get('X-Update-Secret')
    expected_key = APIConfig.UPDATE_SECRET_KEY
    if not expected_key or not provided_key or not secrets.compare_digest(provided_key, expected_key):
        logger.warning("Unauthorized cache clear attempt.")
        return jsonify({"status": "error", "message": "Unauthorized."}), 403

    try:
        cleared = cache.clear() # Use Flask-Caching's clear method like Production
        # Check return value like Production
        if cleared is not False:
             logger.info("Cache cleared successfully via API endpoint.")
             return jsonify({"status": "success", "message": "Cache cleared."}), 200
        else:
             logger.warning("Cache clear command executed, but cache might not support clearing or was already empty.")
             return jsonify({"status": "success", "message": "Cache clear attempted."}), 200
    except Exception as e:
        logger.error(f"Error during cache clear: {e}", exc_info=True)
        abort(500, description="Failed to clear cache.")


# --- Keep Error Handlers (Match Production) ---
@app.errorhandler(HTTPException)
def handle_http_exception(e):
    # Match Production response structure
    response = e.get_response()
    response.data = json.dumps({"code": e.code, "name": e.name, "description": e.description})
    response.content_type = "application/json"
    logger.error(f"{e.code} {e.name}: {e.description} for request {request.url}")
    return response

@app.errorhandler(Exception) # Catch generic Exception like Production
def handle_general_exception(e):
    logger.error("An unexpected server error occurred.", exc_info=True)
    if SentryConfig.SENTRY_DSN: sentry_sdk.capture_exception(e)
    # Match Production response structure
    response = jsonify({"code": 500, "name": "Internal Server Error", "description": "An unexpected error occurred on the server."})
    response.status_code = 500
    return response

# --- Keep Main block (Match Production/Preview) ---
if __name__ == '__main__':
    try:
         DatabaseManager.initialize_pool()
         logger.info("Database pool initialized successfully.")
    except Exception as e:
         logger.critical(f"Failed to initialize database pool on startup: {e}", exc_info=True)
         exit(1) # Exit if DB is required

    logger.info(f"Starting Flask app on {APIConfig.HOST}:{APIConfig.PORT} with DEBUG={APIConfig.DEBUG}")
    app.run(host=APIConfig.HOST, port=APIConfig.PORT, debug=APIConfig.DEBUG)
