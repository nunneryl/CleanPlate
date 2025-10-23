# In file: app_search.py (Updated for Enriched Data)

import logging
from datetime import datetime, date
import json
import redis
from flask import Flask, jsonify, request, abort, Response
from psycopg.rows import dict_row
from db_manager import DatabaseConnection, DatabaseManager
from config import RedisConfig, APIConfig, SentryConfig
from utils import normalize_search_term_for_hybrid
import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration
from werkzeug.exceptions import HTTPException

# --- Sentry Initialization ---
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


app = Flask(__name__)

# --- Redis Cache Setup ---
redis_client = None
try:
    redis_client = redis.Redis(
        host=RedisConfig.HOST,
        port=RedisConfig.PORT,
        password=RedisConfig.PASSWORD,
        username=RedisConfig.USER,
        decode_responses=True,
        socket_timeout=5 # Add a timeout
    )
    redis_client.ping()
    logging.info(f"Successfully connected to Redis at {RedisConfig.HOST}:{RedisConfig.PORT}")
except redis.exceptions.AuthenticationError as e:
    logging.error(f"Redis authentication failed: {e}")
    redis_client = None
except redis.exceptions.ConnectionError as e:
    logging.error(f"Could not connect to Redis: {e}")
    redis_client = None
except Exception as e:
    logging.error(f"An unexpected error occurred during Redis connection: {e}")
    redis_client = None

# --- Custom JSON Encoder ---
from decimal import Decimal

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

app.json_encoder = CustomJSONEncoder

# --- Helper Functions ---
def _execute_query(sql, params=None, fetch_one=False, use_cache=False, cache_key=None, cache_ttl=3600):
    """Executes a SQL query with optional caching."""
    if use_cache and redis_client and cache_key:
        try:
            cached_result = redis_client.get(cache_key)
            if cached_result:
                logging.info(f"Cache HIT for key: {cache_key}")
                return json.loads(cached_result)
            logging.info(f"Cache MISS for key: {cache_key}")
        except redis.RedisError as e:
            logging.error(f"Redis GET error for key {cache_key}: {e}")
            # Fall through to DB query if cache read fails

    try:
        with DatabaseConnection() as conn, conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute(sql, params)
            if fetch_one:
                result = cursor.fetchone()
            else:
                result = cursor.fetchall()

            if use_cache and redis_client and cache_key and result is not None:
                try:
                    # Use custom encoder for date objects before caching
                    redis_client.setex(cache_key, cache_ttl, json.dumps(result, cls=CustomJSONEncoder))
                    logging.info(f"Set cache for key: {cache_key} with TTL: {cache_ttl}")
                except redis.RedisError as e:
                    logging.error(f"Redis SETEX error for key {cache_key}: {e}")
                    # Continue even if caching fails

            return result
    except Exception as e:
        logging.error(f"Database query error: {e}", exc_info=True)
        abort(500, description="Database error occurred.")

def _group_and_shape_results(rows):
    """Groups violations under their respective restaurant inspection."""
    restaurants = {}
    for row in rows:
        # Use tuple (camis, inspection_date) as the unique key for an inspection
        inspection_key = (row['camis'], row['inspection_date'].isoformat())

        if inspection_key not in restaurants:
            restaurants[inspection_key] = {
                'camis': row['camis'],
                'dba': row['dba'],
                'boro': row['boro'],
                'building': row['building'],
                'street': row['street'],
                'zipcode': row['zipcode'],
                'phone': row['phone'],
                'cuisine_description': row['cuisine_description'],
                'inspection_date': row['inspection_date'], # Already date object
                'action': row['action'],
                'critical_flag': row['critical_flag'],
                'grade': row['grade'],
                'grade_date': row['grade_date'], # Already date object or None
                'inspection_type': row['inspection_type'],
                'latitude': row['latitude'],
                'longitude': row['longitude'],
                'violations': [],
                # --- NEW: Add enriched fields ---
                'google_rating': row['google_rating'],
                'google_review_count': row['google_review_count'],
                'website': row['website'],
                'hours': row['hours'], # Keep as JSONB object/None
                'price_level': row['price_level']
                # --- END NEW ---
            }

        if row.get('violation_code') or row.get('violation_description'):
            restaurants[inspection_key]['violations'].append({
                'violation_code': row.get('violation_code'),
                'violation_description': row.get('violation_description')
            })

    # Return as a list, sorted by inspection date descending if needed
    return sorted(list(restaurants.values()), key=lambda x: x['inspection_date'], reverse=True)

# --- API Routes ---

# In PREVIEW_app_search.py

# --- (Keep imports, app setup, cache setup, CustomJSONEncoder, _execute_query, _group_and_shape_results AS IS) ---
# Ensure _group_and_shape_results in PREVIEW is identical to PRODUCTION

@app.route('/search', methods=['GET'])
@cache.cached(timeout=300, query_string=True) # Use cache decorator like production
def search_restaurants():
    # --- Use parameters and logic exactly like PRODUCTION ---
    query = request.args.get('name', '').strip()
    limit = request.args.get('per_page', 25, type=int)
    page = request.args.get('page', 1, type=int)
    offset = (page - 1) * limit
    grade = request.args.get('grade')
    boro = request.args.get('boro')
    cuisine = request.args.get('cuisine')
    sort_param = request.args.get('sort', 'relevance') # Default to relevance like production

    if not query and not grade and not boro and not cuisine:
        logger.info("Search request with no query or filters, returning empty.")
        return jsonify([])

    normalized_query = normalize_search_term_for_hybrid(query) if query else None

    # --- Build WHERE clause exactly like PRODUCTION ---
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

    # --- Build ORDER BY exactly like PRODUCTION ---
    order_by_sql_parts = []
    # Store original params list length before potentially adding similarity param for sorting
    base_params_count = len(params_list)

    if normalized_query and sort_param == 'relevance':
        params_list.insert(0, normalized_query) # Add similarity param for ORDER BY
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
    order_by_sql_parts.append("r.camis") # Add camis for stable pagination sort
    order_by_sql = ", ".join(order_by_sql_parts)

    # --- Use the exact SQL query structure from PRODUCTION ---
    # This fetches all necessary rows for grouping later
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

    # --- Construct parameters exactly like PRODUCTION ---
    # Params for outer WHERE clause + sorting + LIMIT + OFFSET + inner WHERE clause
    # Need to duplicate the WHERE clause params
    final_params_list = params_list # Contains similarity param if needed for ORDER BY
    # Add WHERE params again (excluding potential similarity sort param)
    final_params_list += params_list[len(params_list)-base_params_count:]
    final_params_list += [limit, offset] # Add LIMIT and OFFSET

    params_tuple = tuple(final_params_list)

    # --- Execute query ---
    try:
        # Pass use_cache=False because decorator handles it
        all_rows = _execute_query(sql, params_tuple, use_cache=False)
    except Exception as e:
         logger.error(f"Search query failed: {e}", exc_info=True)
         # Match production error response more closely if needed
         return jsonify({"error": "Search failed", "details": str(e)}), 500

    # --- Determine ordered CAMIS list for grouping (like PRODUCTION) ---
    ordered_camis = []
    seen_camis = set()
    if all_rows: # Check if all_rows is not None
        # This relies on the final ORDER BY clause in the SQL
        for row in all_rows:
            camis_str = str(row['camis']) # Convert to string for consistency
            if camis_str not in seen_camis:
                ordered_camis.append(camis_str)
                seen_camis.add(camis_str)
                if len(ordered_camis) == limit: # Stop once we have enough unique CAMIS for the page
                     break
    else:
        all_rows = [] # Ensure all_rows is an empty list if None

    # Ensure _group_and_shape_results includes google_rating in its logic
    grouped_data = _group_and_shape_results(all_rows, ordered_camis)

    logger.info(f"Search for '{query}' found {len(ordered_camis)} unique restaurants, returning grouped data.")
    return jsonify(grouped_data) # Return the grouped structure from production logic

@app.route('/restaurant/<string:camis>', methods=['GET'])
def get_restaurant_details(camis):
    if not camis.isdigit():
        abort(400, description="Invalid CAMIS format.")

    # Cache key specific to this CAMIS
    cache_key = f"restaurant:{camis}"

    # --- UPDATED QUERY: Select all necessary columns including new enriched fields ---
    sql = """
        SELECT
            r.camis, r.dba, r.boro, r.building, r.street, r.zipcode, r.phone,
            r.cuisine_description, r.inspection_date, r.action, r.critical_flag,
            r.grade, r.grade_date, r.inspection_type, r.latitude, r.longitude,
            v.violation_code, v.violation_description,
            -- New enriched fields
            r.google_rating, r.google_review_count, r.website, r.hours, r.price_level
        FROM restaurants r
        LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date
        WHERE r.camis = %s
        ORDER BY r.inspection_date DESC;
    """
    params = (camis,)
    # Use cache for restaurant details, TTL 24 hours (86400 seconds)
    rows = _execute_query(sql, params, use_cache=True, cache_key=cache_key, cache_ttl=86400)

    if not rows:
        abort(404, description="Restaurant not found.")

    # The existing _group_and_shape_results function now handles the new fields
    grouped_data = _group_and_shape_results(rows)
    return jsonify(grouped_data)

@app.route('/recently-graded', methods=['GET'])
def get_recently_graded():
    limit = request.args.get('limit', 50, type=int)
    offset = request.args.get('offset', 0, type=int)
    # Cache key including limit and offset
    cache_key = f"recently_graded:limit:{limit}:offset:{offset}"

    sql = """
        WITH RankedInspections AS (
            SELECT
                r.camis, r.dba, r.boro, r.building, r.street, r.zipcode, r.grade, r.grade_date,
                r.inspection_date, r.cuisine_description, r.latitude, r.longitude,
                -- NEW: Include rating
                r.google_rating,
                -- END NEW
                ROW_NUMBER() OVER(PARTITION BY r.camis ORDER BY r.inspection_date DESC) as rn
            FROM restaurants r
            WHERE r.grade IS NOT NULL AND r.grade != '' AND r.grade_date IS NOT NULL
        )
        SELECT * FROM RankedInspections
        WHERE rn = 1
        ORDER BY grade_date DESC, inspection_date DESC
        LIMIT %s OFFSET %s;
    """
    params = (limit, offset)
    # Use cache, TTL 1 hour
    results = _execute_query(sql, params, use_cache=True, cache_key=cache_key, cache_ttl=3600)

    # Simplified shaping for this list view
    shaped_results = [{
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
         # --- NEW: Add rating to response ---
        'google_rating': row['google_rating']
        # --- END NEW ---
    } for row in results]

    return jsonify(shaped_results)

@app.route('/lists/recent-actions', methods=['GET'])
@app.route('/grade-updates', methods=['GET'])
def get_grade_updates():
    limit = request.args.get('limit', 50, type=int)
    offset = request.args.get('offset', 0, type=int)
    update_type = request.args.get('type', 'finalized') # Default to finalized

    if update_type not in ['finalized', 'closed', 'reopened']:
         abort(400, description="Invalid update type. Must be 'finalized', 'closed', or 'reopened'.")

    # Cache key including limit, offset, and type
    cache_key = f"grade_updates:type:{update_type}:limit:{limit}:offset:{offset}"

    # Base query common part
    base_sql_with = """
        WITH LatestRestaurantInspection AS (
            SELECT
                r.*,
                ROW_NUMBER() OVER(PARTITION BY r.camis ORDER BY r.inspection_date DESC) as rn
            FROM restaurants r
        ),
        LatestRestaurantState AS (
            SELECT * FROM LatestRestaurantInspection WHERE rn = 1
        )
    """

    if update_type == 'finalized':
        sql = base_sql_with + """
            SELECT
                gu.restaurant_camis AS camis,
                lr.dba, lr.boro, lr.building, lr.street, lr.zipcode,
                lr.cuisine_description, lr.latitude, lr.longitude,
                gu.previous_grade, gu.new_grade AS grade, gu.update_date AS grade_date,
                gu.inspection_date,
                lr.google_rating -- Include rating
            FROM grade_updates gu
            JOIN LatestRestaurantState lr ON gu.restaurant_camis = lr.camis
                                        AND gu.inspection_date = lr.inspection_date -- Ensure it matches the latest insp.
            WHERE gu.update_type = 'finalized'
            ORDER BY gu.update_date DESC
            LIMIT %s OFFSET %s;
        """
    elif update_type == 'closed':
         sql = base_sql_with + """
             SELECT
                 lr.camis, lr.dba, lr.boro, lr.building, lr.street, lr.zipcode,
                 lr.cuisine_description, lr.latitude, lr.longitude,
                 lr.action, lr.inspection_date,
                 lr.google_rating -- Include rating
             FROM LatestRestaurantState lr
             WHERE lr.action = 'Establishment Closed by DOHMH.'
             ORDER BY lr.inspection_date DESC
             LIMIT %s OFFSET %s;
         """
    else: # reopened
        sql = base_sql_with + """
             -- Find restaurants whose latest state is NOT closed, but whose PREVIOUS inspection WAS closed
             WITH PreviousInspections AS (
                 SELECT
                     r.camis, r.action, r.inspection_date,
                     ROW_NUMBER() OVER(PARTITION BY r.camis ORDER BY r.inspection_date DESC) as rn
                 FROM restaurants r
             ),
             SecondLatestState AS (
                  SELECT * FROM PreviousInspections WHERE rn = 2
             )
             SELECT
                 lr.camis, lr.dba, lr.boro, lr.building, lr.street, lr.zipcode,
                 lr.cuisine_description, lr.latitude, lr.longitude,
                 lr.action, lr.inspection_date, lr.grade, lr.grade_date,
                 lr.google_rating -- Include rating
             FROM LatestRestaurantState lr
             JOIN SecondLatestState sls ON lr.camis = sls.camis
             WHERE lr.action != 'Establishment Closed by DOHMH.' -- Current is open/other
             AND sls.action = 'Establishment Closed by DOHMH.'    -- Previous was closed
             ORDER BY lr.inspection_date DESC -- Order by the reopening inspection date
             LIMIT %s OFFSET %s;
         """

    params = (limit, offset)
    # Use cache, TTL 1 hour
    results = _execute_query(sql, params, use_cache=True, cache_key=cache_key, cache_ttl=3600)

    # Simplified shaping, adjust fields based on query type as needed
    shaped_results = [dict(row) for row in results] # Convert dict_row to standard dict for JSON

    return jsonify(shaped_results)


# --- Cache Invalidation ---
@app.route('/clear-cache', methods=['POST'])
def clear_cache_endpoint():
    # Basic security check - a real app should use a more robust method
    secret_key = request.headers.get('X-Update-Secret')
    if not secret_key or secret_key != APIConfig.UPDATE_SECRET_KEY:
        logging.warning("Unauthorized cache clear attempt.")
        abort(403, description="Unauthorized.")

    if not redis_client:
        logging.warning("Cache clear requested, but Redis client is not available.")
        return jsonify({"message": "Cache client not available."}), 503

    try:
        # Clear specific patterns related to dynamic data
        patterns_to_clear = ["search:*", "restaurant:*", "recently_graded:*", "grade_updates:*"]
        cleared_count = 0
        for pattern in patterns_to_clear:
            keys = redis_client.keys(pattern)
            if keys:
                deleted = redis_client.delete(*keys)
                cleared_count += deleted
                logging.info(f"Cleared {deleted} keys matching pattern '{pattern}'")
            else:
                 logging.info(f"No keys found for pattern '{pattern}'")

        logging.info(f"Cache clear successful. Total keys cleared: {cleared_count}")
        return jsonify({"message": f"Cache cleared successfully. Total keys cleared: {cleared_count}"}), 200
    except redis.RedisError as e:
        logging.error(f"Redis cache clear error: {e}")
        abort(500, description="Failed to clear cache.")
    except Exception as e:
        logging.error(f"Unexpected error during cache clear: {e}", exc_info=True)
        abort(500, description="An unexpected error occurred.")


# --- Error Handling ---
@app.errorhandler(HTTPException)
def handle_http_exception(e):
    """Return JSON instead of HTML for HTTP errors."""
    response = e.get_response()
    response.data = json.dumps({
        "code": e.code,
        "name": e.name,
        "description": e.description,
    })
    response.content_type = "application/json"
    logging.error(f"{e.code} {e.name}: {e.description} for request {request.url}")
    return response

@app.errorhandler(Exception)
def handle_general_exception(e):
    """Handle unexpected errors."""
    # Log the full traceback
    logging.error("An unexpected server error occurred.", exc_info=True)
    # Send error to Sentry if configured
    if SentryConfig.SENTRY_DSN:
        sentry_sdk.capture_exception(e)
    # Return a generic 500 error to the client
    response = jsonify({
        "code": 500,
        "name": "Internal Server Error",
        "description": "An unexpected error occurred on the server.",
    })
    response.status_code = 500
    return response

# --- Main ---
if __name__ == '__main__':
    DatabaseManager.initialize_pool()
    logging.info(f"Starting Flask app on {APIConfig.HOST}:{APIConfig.PORT} with DEBUG={APIConfig.DEBUG}")
    app.run(host=APIConfig.HOST, port=APIConfig.PORT, debug=APIConfig.DEBUG)
    # Ensure pool is closed when app exits (though usually handled by OS/container)
    # DatabaseManager.close_all_connections()
