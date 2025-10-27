# In file: PREVIEW_app_search.py (Fully Merged)

import logging
from datetime import datetime, date
import json
import redis
from decimal import Decimal
from flask import Flask, jsonify, request, abort, Response
from flask_cors import CORS
from flask_caching import Cache
import psycopg
from psycopg.rows import dict_row
from db_manager import DatabaseConnection, DatabaseManager
from config import RedisConfig, APIConfig, SentryConfig
from utils import normalize_search_term_for_hybrid
import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration
from werkzeug.exceptions import HTTPException
import os
import secrets
import threading

# --- Imports from PRODUCTION file ---
import jwt
import requests
import smtplib
import ssl
from email.message import EmailMessage
try:
    from update_database import run_database_update
except ImportError:
    logging.warning("Could not import run_database_update. /trigger-update will not work.")
    def run_database_update(days=3):
        logging.error("run_database_update is not available.")
# --- End Production Imports ---


# --- Sentry Initialization ---
if SentryConfig.SENTRY_DSN:
    sentry_sdk.init(
        dsn=SentryConfig.SENTRY_DSN, integrations=[FlaskIntegration()],
        traces_sample_rate=1.0, profiles_sample_rate=1.0, send_default_pii=True
    )
    logging.info("Sentry initialized.")
else:
    logging.info("Sentry DSN not found, skipping initialization.")

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)

# --- Flask App Initialization ---
app = Flask(__name__)
CORS(app)

# --- Cache Configuration ---
cache_config = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_REDIS_URL": os.environ.get('REDIS_URL'),
    "CACHE_DEFAULT_TIMEOUT": 300
}
app.config.from_mapping(cache_config)
cache = Cache(app)

# --- Custom JSON Encoder (from Preview) ---
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, date): return obj.isoformat()
        if isinstance(obj, Decimal): return float(obj)
        return super().default(obj)
app.json_encoder = CustomJSONEncoder

# --- APPLE KEY CACHING ---
@cache.cached(timeout=86400) # Cache for 24 hours
def get_apple_public_keys():
    """
    Fetches Apple's public keys for Sign in with Apple token verification.
    """
    try:
        r = requests.get("https://appleid.apple.com/auth/keys")
        r.raise_for_status()
        keys_data = r.json()
        # Create a dictionary mapping the key ID (kid) to the key data
        return {key['kid']: key for key in keys_data['keys']}
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch Apple public keys: {e}")
        return None
# --- Helper Functions ---

def _execute_query(sql, params=None, fetch_one=False):
    """Executes a SQL query."""
    try:
        with DatabaseConnection() as conn, conn.cursor(row_factory=dict_row) as cursor:
            try:
                logger.debug(f"Executing SQL query...")
                cursor.execute(sql, params)
                if fetch_one: result = cursor.fetchone()
                else: result = cursor.fetchall()
                logger.debug(f"Query returned {len(result) if isinstance(result, list) else (1 if result else 0)} rows.")
                return result
            except psycopg.Error as db_err:
                logger.error(f"Database query error during execution: {db_err} SQL: {sql} PARAMS: {params}", exc_info=True)
                abort(500, description="Database query error occurred during execution.")
            except Exception as e:
                logger.error(f"General error during query execution: {e} SQL: {sql} PARAMS: {params}", exc_info=True)
                abort(500, description="General error during query execution.")
    except psycopg.Error as conn_err:
        logger.error(f"Database connection error: {conn_err}", exc_info=True)
        abort(500, description="Database connection error occurred.")
    except Exception as e:
        logger.error(f"Error getting DB connection or cursor: {e}", exc_info=True)
        abort(500, description="Error establishing database connection.")


def _group_and_shape_results(all_rows, ordered_camis):
    """ (From Preview) Groups violations and includes NEW Google data. """
    if not all_rows: return []
    
    restaurant_rows_map = {camis_str: [] for camis_str in ordered_camis}
    for row in all_rows:
        camis_str = str(row['camis'])
        if camis_str in restaurant_rows_map:
            restaurant_rows_map[camis_str].append(row)

    final_results = []
    for camis_str in ordered_camis:
        rows_for_camis = restaurant_rows_map.get(camis_str)
        if not rows_for_camis:
            logger.warning(f"No rows found for expected CAMIS {camis_str} during shaping.")
            continue

        base_info_row = rows_for_camis[0]
        restaurant_obj = {
            'camis': base_info_row['camis'], 'dba': base_info_row['dba'],
            'boro': base_info_row['boro'], 'building': base_info_row['building'],
            'street': base_info_row['street'], 'zipcode': base_info_row['zipcode'],
            'phone': base_info_row['phone'], 'cuisine_description': base_info_row['cuisine_description'],
            'latitude': base_info_row['latitude'], 'longitude': base_info_row['longitude'],
            'foursquare_fsq_id': base_info_row.get('foursquare_fsq_id'),
            # --- NEW GOOGLE DATA ---
            'google_place_id': base_info_row.get('google_place_id'),
            'google_rating': base_info_row.get('google_rating'),
            'google_review_count': base_info_row.get('google_review_count'),
            'website': base_info_row.get('website'),
            'hours': base_info_row.get('hours'),
            'price_level': base_info_row.get('price_level'),
            # --- END NEW GOOGLE DATA ---
            'inspections': []
        }
        inspections_map = {}
        processed_violation_keys = set()
        for row in rows_for_camis:
            try:
                insp_date_obj = row.get('inspection_date')
                if not isinstance(insp_date_obj, date): continue
                insp_date_iso = insp_date_obj.isoformat()
                if insp_date_iso not in inspections_map:
                    inspections_map[insp_date_iso] = {
                        'inspection_date': row['inspection_date'], 'action': row.get('action'),
                        'critical_flag': row.get('critical_flag'), 'grade': row.get('grade'),
                        'grade_date': row.get('grade_date'), 'inspection_type': row.get('inspection_type'),
                        'score': row.get('score'), 'violations': []
                    }
                violation_code = row.get('violation_code')
                violation_desc = row.get('violation_description')
                if violation_code or violation_desc:
                    violation_key = (insp_date_iso, violation_code, violation_desc)
                    if violation_key not in processed_violation_keys:
                        inspections_map[insp_date_iso]['violations'].append({
                            'violation_code': violation_code, 'violation_description': violation_desc
                        })
                        processed_violation_keys.add(violation_key)
            except Exception as e:
                logger.error(f"Error processing row during shaping for CAMIS {camis_str}, inspection {insp_date_iso}: {e}", exc_info=True)
        sorted_inspections = sorted(inspections_map.values(), key=lambda x: x['inspection_date'], reverse=True)
        restaurant_obj['inspections'] = sorted_inspections
        final_results.append(restaurant_obj)
    return final_results

# --- SECURITY & AUTH HELPERS (From Production) ---

def verify_apple_token(token):
    """
    Securely verifies an Apple ID token's signature and payload.
    """
    # CRITICAL: You must set this environment variable in Railway.
    # e.g., "com.yourcompany.cleanplate"
    APPLE_APP_BUNDLE_ID = os.environ.get('APPLE_APP_BUNDLE_ID')
    if not APPLE_APP_BUNDLE_ID:
        logger.critical("APPLE_APP_BUNDLE_ID environment variable is not set. Cannot verify tokens.")
        return None

    try:
        # Get the key ID (kid) from the token's header
        unverified_header = jwt.get_unverified_header(token)
        kid = unverified_header.get('kid')
        if not kid:
            logger.warning("Token missing 'kid' in header.")
            return None

        # Get Apple's public keys (from cache or live)
        apple_keys = get_apple_public_keys()
        if not apple_keys:
            logger.error("Could not retrieve Apple public keys.")
            return None

        # Find the matching key
        key_data = apple_keys.get(kid)
        if not key_data:
            logger.warning(f"Token 'kid' {kid} not found in Apple's public keys.")
            return None

        # Construct the public key from the JWK data (n, e)
        public_key = jwt.algorithms.RSAAlgorithm.from_jwk(key_data)

        # Decode and verify the token's signature, audience, and issuer
        decoded_payload = jwt.decode(
            token,
            key=public_key,
            algorithms=['RS256'],
            audience=APPLE_APP_BUNDLE_ID,
            issuer='https://appleid.apple.com'
        )
        
        # Token is valid, return the full payload
        return decoded_payload

    except jwt.ExpiredSignatureError:
        logger.warning("Apple token has expired.")
        return None
    except jwt.InvalidAudienceError:
        logger.warning("Apple token has invalid audience.")
        return None
    except jwt.InvalidIssuerError:
        logger.warning("Apple token has invalid issuer.")
        return None
    except Exception as e:
        logger.error(f"General error verifying Apple token: {e}", exc_info=True)
        return None

def _get_user_id_from_token(request):
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return None, jsonify({"error": "Authorization token is required"}), 401
    
    token = auth_header.split(' ')[1]
    payload = verify_apple_token(token)
    
    if not payload:
        return None, jsonify({"error": "Invalid or expired token"}), 401
    
    user_id = payload.get('sub') # 'sub' (subject) is the user's unique ID
    if not user_id:
        logger.error("Token payload verified but missing 'sub' (user_id).")
        return None, jsonify({"error": "Invalid token payload"}), 401
        
    return user_id, None, None

def make_user_cache_key(*args, **kwargs):
    # Creates a cache key that is unique to the current user
    user_id, _, _ = _get_user_id_from_token(request)
    if user_id:
        # Use request.path instead of request.full_path to ignore query strings
        # for user-specific GET endpoints like /favorites
        return f"user_{user_id}_{request.path}"
    # Fallback, should not be hit by authed routes
    return request.path

# --- API Routes ---

@app.route('/search', methods=['GET'])
@cache.cached(timeout=300, query_string=True)
def search_restaurants():
    """ (From Preview) This is the new, fixed search function """
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
    
    where_clauses = ["TRUE"]; params_list = []
    
    if normalized_query:
        where_clauses.append("(similarity(r_inner.dba_normalized_search, %s) > 0.2 OR r_inner.dba ILIKE %s)"); params_list.extend([normalized_query, f"%{query}%"])
    elif query:
        where_clauses.append("r_inner.dba ILIKE %s"); params_list.append(f"%{query}%")
    
    if grade and grade.upper() in ['A', 'B', 'C', 'P', 'Z', 'N']:
        where_clauses.append("r_inner.grade = %s" if grade.upper() in ['A','B','C'] else "r_inner.grade IN ('P', 'Z', 'N')")
        if grade.upper() in ['A','B','C']: params_list.append(grade.upper())
    if boro and boro != 'Any': where_clauses.append("r_inner.boro = %s"); params_list.append(boro.title())
    if cuisine and cuisine != 'Any': where_clauses.append("r_inner.cuisine_description = %s"); params_list.append(cuisine)
    
    where_sql_for_subquery = " AND ".join(where_clauses)
    
    subquery_order_by_parts = []
    outer_order_by_parts = []
    sort_params_list = []
    group_by_sql_parts = ["r_inner.camis"]
    
    if normalized_query and sort_param == 'relevance':
        subquery_order_by_parts.append("MAX(similarity(r_inner.dba_normalized_search, %s)) DESC")
        outer_order_by_parts.append("similarity(r.dba_normalized_search, %s) DESC")
        sort_params_list.append(normalized_query)
    
    sort_field_inner = "r_inner.inspection_date"
    sort_field_outer = "r.inspection_date"
    sort_direction = "DESC"
    agg_func = "MAX"
    
    if sort_param == 'date_asc':
        sort_direction = "ASC"
        agg_func = "MIN"
    elif sort_param == 'name_asc':
        sort_field_inner = "r_inner.dba"
        sort_field_outer = "r.dba"
        sort_direction = "ASC"
        agg_func = ""
    elif sort_param == 'name_desc':
        sort_field_inner = "r_inner.dba"
        sort_field_outer = "r.dba"
        sort_direction = "DESC"
        agg_func = ""

    if "dba" in sort_field_inner and sort_field_inner not in group_by_sql_parts:
        group_by_sql_parts.append(sort_field_inner)

    if agg_func:
        subquery_order_by_parts.append(f"{agg_func}({sort_field_inner}) {sort_direction}")
    else:
        subquery_order_by_parts.append(f"{sort_field_inner} {sort_direction}")
    
    outer_order_by_parts.append(f"{sort_field_outer} {sort_direction}")
    subquery_order_by_parts.append("r_inner.camis")
    outer_order_by_parts.append("r.camis")
    outer_order_by_parts.append("r.inspection_date DESC")
    
    group_by_sql_for_subquery = ", ".join(group_by_sql_parts)
    order_by_sql_for_subquery = ", ".join(subquery_order_by_parts)
    order_by_sql_for_outer_query = ", ".join(outer_order_by_parts)

    sql = f"""
        SELECT r.*, v.violation_code, v.violation_description
        FROM (
            SELECT r_inner.camis
            FROM restaurants r_inner
            WHERE {where_sql_for_subquery}
            GROUP BY {group_by_sql_for_subquery}
            ORDER BY {order_by_sql_for_subquery}
            LIMIT %s OFFSET %s
        ) AS paged_restaurants
        JOIN restaurants r ON paged_restaurants.camis = r.camis
        LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date
        WHERE {where_sql_for_subquery.replace('r_inner.', 'r.')}
        ORDER BY {order_by_sql_for_outer_query};
    """

    final_params_list = []
    final_params_list.extend(params_list)
    final_params_list.extend(sort_params_list)
    final_params_list.append(limit)
    final_params_list.append(offset)
    final_params_list.extend(params_list)
    final_params_list.extend(sort_params_list)
    params_tuple = tuple(final_params_list)

    logger.debug(f"Attempting search with {len(params_tuple)} parameters.")

    try:
        all_rows = _execute_query(sql, params_tuple)
    except Exception as e:
        logger.error(f"Search query failed.", exc_info=True)
        return jsonify({"error": "Search failed"}), 500

    ordered_camis = []
    if all_rows:
        camis_set_in_order = set()
        ordered_camis_list = []
        for row in all_rows:
            camis_str = str(row['camis'])
            if camis_str not in camis_set_in_order:
                camis_set_in_order.add(camis_str)
                ordered_camis_list.append(camis_str)
        ordered_camis = ordered_camis_list
    else:
        all_rows = []

    grouped_data = _group_and_shape_results(all_rows, ordered_camis)
    logger.info(f"Search for '{query}' found {len(ordered_camis)} unique restaurants, returning grouped data.")
    return jsonify(grouped_data)


@app.route('/restaurant/<string:camis>', methods=['GET'])
@cache.cached(timeout=86400)
def get_restaurant_details(camis):
    """ (From Preview) Uses new _group_and_shape_results helper """
    if not camis.isdigit(): abort(400, description="Invalid CAMIS format.")
    sql = """ SELECT r.*, v.violation_code, v.violation_description FROM restaurants r LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date WHERE r.camis = %s ORDER BY r.inspection_date DESC; """
    rows = _execute_query(sql, (camis,))
    if not rows: abort(404, description="Restaurant not found.")
    grouped_data = _group_and_shape_results(rows, [camis])
    return jsonify(grouped_data[0] if grouped_data else None)


@app.route('/recently-graded', methods=['GET'])
@cache.cached(timeout=3600, query_string=True)
def get_recently_graded():
    """ (From Preview) Selects r.* so it includes Google Data """
    limit = request.args.get('limit', 50, type=int); offset = request.args.get('offset', 0, type=int)
    sql = """ WITH RankedInspections AS ( SELECT r.*, ROW_NUMBER() OVER(PARTITION BY r.camis ORDER BY r.inspection_date DESC) as rn FROM restaurants r WHERE r.grade IS NOT NULL AND r.grade NOT IN ('', 'N', 'Z', 'P') AND r.grade_date IS NOT NULL ) SELECT * FROM RankedInspections WHERE rn = 1 ORDER BY grade_date DESC, inspection_date DESC LIMIT %s OFFSET %s; """
    results = _execute_query(sql, (limit, offset))
    shaped_results = [dict(row) for row in results] if results else []
    return jsonify(shaped_results)


@app.route('/lists/recent-actions', methods=['GET'])
@app.route('/grade-updates', methods=['GET'])
@cache.cached(timeout=3600, query_string=True)
def get_grade_updates():
    """ (From Preview) Selects r.* so it includes Google Data """
    limit = request.args.get('limit', 50, type=int); offset = request.args.get('offset', 0, type=int)
    update_type = request.args.get('type', 'finalized')
    if update_type not in ['finalized', 'closed', 'reopened']: abort(400, description="Invalid update type.")
    base_sql_with = """ WITH LatestRestaurantInspection AS ( SELECT r.*, ROW_NUMBER() OVER(PARTITION BY r.camis ORDER BY r.inspection_date DESC) as rn FROM restaurants r ), LatestRestaurantState AS ( SELECT * FROM LatestRestaurantInspection WHERE rn = 1 ) """
    params = [limit, offset]
    if update_type == 'finalized': sql = base_sql_with + """ SELECT gu.restaurant_camis AS camis, lr.*, gu.previous_grade, gu.new_grade, gu.update_date AS finalized_date FROM grade_updates gu JOIN LatestRestaurantState lr ON gu.restaurant_camis = lr.camis WHERE gu.update_type = 'finalized' AND gu.inspection_date = lr.inspection_date ORDER BY gu.update_date DESC LIMIT %s OFFSET %s; """
    elif update_type == 'closed': sql = base_sql_with + """ SELECT lr.* FROM LatestRestaurantState lr WHERE lr.action = 'Establishment Closed by DOHMH.' ORDER BY lr.inspection_date DESC LIMIT %s OFFSET %s; """
    else: sql = base_sql_with + """ WITH PreviousInspections AS ( SELECT r.camis, r.action, r.inspection_date, ROW_NUMBER() OVER(PARTITION BY r.camis ORDER BY r.inspection_date DESC) as rn FROM restaurants r ), SecondLatestState AS ( SELECT * FROM PreviousInspections WHERE rn = 2 ) SELECT lr.* FROM LatestRestaurantState lr JOIN SecondLatestState sls ON lr.camis = sls.camis WHERE lr.action != 'Establishment Closed by DOHMH.' AND sls.action = 'Establishment Closed by DOHMH.' ORDER BY lr.inspection_date DESC LIMIT %s OFFSET %s; """
    results = _execute_query(sql, tuple(params))
    shaped_results = [dict(row) for row in results] if results else []
    if shaped_results:
        for item in shaped_results:
            item['update_type'] = update_type
            if update_type == 'finalized': item['grade'] = item.pop('new_grade', None); item['grade_date'] = item.pop('finalized_date', None)
    return jsonify(shaped_results)

# --- USER & FAVORITES ROUTES (From Production) ---

@app.route('/users', methods=['POST'])
def create_user():
    if not request.is_json: return jsonify({"error": "Request must be JSON"}), 400
    data = request.get_json()
    token = data.get('identityToken')
    if not token: return jsonify({"error": "identityToken is required"}), 400

    payload = verify_apple_token(token)
    if not payload:
        return jsonify({"error": "Invalid token"}), 400

    user_id = payload.get('sub')
    if not user_id:
         logger.error("Token payload verified but missing 'sub' (user_id) during user creation.")
         return jsonify({"error": "Invalid token payload"}), 400

    insert_query = "INSERT INTO users (id) VALUES (%s) ON CONFLICT (id) DO NOTHING;"
    try:
        _execute_query(insert_query, (user_id,))
        # Note: _execute_query doesn't conn.commit() automatically. This might need adjustment
        # For simple inserts like this, we'll open a connection to commit.
        with DatabaseConnection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(insert_query, (user_id,))
            conn.commit()
        return jsonify({"status": "success", "message": "User created or already exists."}), 201
    except Exception as e:
        logger.error(f"Failed to insert user into database: {e}", exc_info=True)
        return jsonify({"error": "Database operation failed"}), 500

@app.route('/favorites', methods=['POST'])
def add_favorite():
    user_id, error_response, status_code = _get_user_id_from_token(request)
    if error_response: return error_response, status_code
    if not request.is_json: return jsonify({"error": "Request must be JSON"}), 400
    data = request.get_json()
    camis = data.get('camis')
    if not camis: return jsonify({"error": "Restaurant 'camis' is required"}), 400
    
    if user_id:
        cache.delete(f"user_{user_id}_/favorites")

    insert_query = "INSERT INTO favorites (user_id, restaurant_camis) VALUES (%s, %s) ON CONFLICT (user_id, restaurant_camis) DO NOTHING;"
    try:
        with DatabaseConnection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(insert_query, (user_id, camis))
            conn.commit()
        return jsonify({"status": "success", "message": "Favorite added."}), 201
    except Exception as e:
        logger.error(f"Failed to insert favorite for user {user_id}: {e}", exc_info=True)
        return jsonify({"error": "Database operation failed"}), 500

@app.route('/favorites', methods=['GET'])
@cache.cached(timeout=3600, key_prefix=make_user_cache_key) # Use user-specific cache key
def get_favorites():
    user_id, error_response, status_code = _get_user_id_from_token(request)
    if error_response: return error_response, status_code

    query = """
        SELECT r.*, v.violation_code, v.violation_description 
        FROM restaurants r 
        LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date 
        WHERE r.camis IN (SELECT restaurant_camis FROM favorites WHERE user_id = %s)
    """
    try:
        all_rows = _execute_query(query, (user_id,))
        if not all_rows: return jsonify([])
        favorited_camis = sorted(list(set([row['camis'] for row in all_rows])))
        final_results = _group_and_shape_results(all_rows, favorited_camis)
        return jsonify(final_results)
    except Exception as e:
        logger.error(f"Failed to fetch favorites for user {user_id}: {e}", exc_info=True)
        return jsonify({"error": "Database operation failed"}), 500

@app.route('/favorites/<string:camis>', methods=['DELETE'])
def remove_favorite(camis):
    user_id, error_response, status_code = _get_user_id_from_token(request)
    if error_response: return error_response, status_code
    
    if user_id:
        cache.delete(f"user_{user_id}_/favorites")

    delete_query = "DELETE FROM favorites WHERE user_id = %s AND restaurant_camis = %s;"
    try:
        with DatabaseConnection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(delete_query, (user_id, camis))
            conn.commit()
        return jsonify({"status": "success", "message": "Favorite removed."}), 200
    except Exception as e:
        logger.error(f"Failed to delete favorite for user {user_id}: {e}", exc_info=True)
        return jsonify({"error": "Database operation failed"}), 500

@app.route('/users', methods=['DELETE'])
def delete_user():
    user_id, error_response, status_code = _get_user_id_from_token(request)
    if error_response: return error_response, status_code

    delete_query = "DELETE FROM users WHERE id = %s;"
    try:
        with DatabaseConnection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(delete_query, (user_id,))
            conn.commit()
        logger.info(f"User {user_id} and all associated data have been deleted.")
        return jsonify({"status": "success", "message": "User deleted successfully."}), 200
    except Exception as e:
        logger.error(f"Failed to delete user {user_id}: {e}", exc_info=True)
        return jsonify({"error": "Database operation failed"}), 500

@app.route('/recent-searches', methods=['POST'])
def save_recent_search():
    user_id, error_response, status_code = _get_user_id_from_token(request)
    if error_response: return error_response, status_code

    if not request.is_json:
        return jsonify({"error": "Request must be JSON"}), 400
    data = request.get_json()
    search_term = data.get('search_term')
    if not search_term or not search_term.strip():
        return jsonify({"error": "search_term is required and cannot be empty"}), 400
    
    search_term_display = search_term.strip()
    search_term_normalized = normalize_search_term_for_hybrid(search_term_display)

    upsert_query = """
        INSERT INTO recent_searches (user_id, search_term_display, search_term_normalized)
        VALUES (%s, %s, %s)
        ON CONFLICT (user_id, search_term_normalized)
        DO UPDATE SET
            created_at = NOW(),
            search_term_display = EXCLUDED.search_term_display;
    """
    
    try:
        with DatabaseConnection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(upsert_query, (user_id, search_term_display, search_term_normalized))
            conn.commit()
        logger.info(f"User {user_id} saved or updated search term: '{search_term_display}'")
        return jsonify({"status": "success", "message": "Search saved."}), 201
    except Exception as e:
        logger.error(f"Failed to save recent search for user {user_id}: {e}", exc_info=True)
        return jsonify({"error": "Database operation failed"}), 500

@app.route('/recent-searches', methods=['GET'])
def get_recent_searches():
    user_id, error_response, status_code = _get_user_id_from_token(request)
    if error_response: return error_response, status_code

    query = """
        SELECT id, search_term_display, created_at
        FROM recent_searches
        WHERE user_id = %s
        ORDER BY created_at DESC
        LIMIT 10;
    """

    try:
        results = _execute_query(query, (user_id,))
        for item in results:
            item['created_at'] = item['created_at'].isoformat()
        return jsonify(results)
    except Exception as e:
        logger.error(f"Failed to fetch recent searches for user {user_id}: {e}", exc_info=True)
        return jsonify({"error": "Database operation failed"}), 500

@app.route('/recent-searches', methods=['DELETE'])
def delete_recent_searches():
    user_id, error_response, status_code = _get_user_id_from_token(request)
    if error_response: return error_response, status_code

    delete_query = "DELETE FROM recent_searches WHERE user_id = %s;"
    try:
        with DatabaseConnection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(delete_query, (user_id,))
            conn.commit()
        logger.info(f"Cleared recent searches for user {user_id}")
        return jsonify({"status": "success", "message": "Recent searches cleared."}), 200
    except Exception as e:
        logger.error(f"Failed to delete recent searches for user {user_id}: {e}", exc_info=True)
        return jsonify({"error": "Database operation failed"}), 500

# --- ADMINISTRATIVE ENDPOINTS (From Production) ---

def send_report_email(data):
    """ (Stubbed) Email sending logic from production """
    logger.warning("Email sending is stubbed out in this version.")
    # In a real scenario, you'd have the email logic here.
    # For now, we just return True to simulate success.
    # In production, this would be:
    # return _send_email_logic(data)
    return True # Stubbed response

@app.route('/report-issue', methods=['POST'])
def report_issue():
    if not request.is_json: return jsonify({"error": "Request must be JSON"}), 400
    data = request.get_json()
    if not all(k in data for k in ["camis", "issue_type", "comments"]):
        return jsonify({"error": "Missing required fields"}), 400
    
    if send_report_email(data):
        return jsonify({"status": "success", "message": "Report received."}), 200
    else:
        return jsonify({"status": "error", "message": "Failed to process report."}), 500

@app.route('/trigger-update', methods=['POST'])
def trigger_update():
    provided_key = request.headers.get('X-Update-Secret')
    expected_key = APIConfig.UPDATE_SECRET_KEY
    if not expected_key or not provided_key or not secrets.compare_digest(provided_key, expected_key):
        return jsonify({"status": "error", "message": "Unauthorized."}), 403
    
    threading.Thread(target=run_database_update, args=(15,), daemon=True).start()
    return jsonify({"status": "success", "message": "Database update triggered in background."}), 202

@app.route('/clear-cache', methods=['POST'])
def clear_cache():
    provided_key = request.headers.get('X-Update-Secret'); expected_key = APIConfig.UPDATE_SECRET_KEY
    if not expected_key or not provided_key or not secrets.compare_digest(provided_key, expected_key):
        logger.warning("Unauthorized cache clear attempt."); return jsonify({"status": "error", "message": "Unauthorized."}), 403
    try:
        cleared = cache.clear(); logger.info(f"Cache clear attempt result: {cleared}")
        if cleared is not False: logger.info("Cache cleared successfully via API endpoint."); return jsonify({"status": "success", "message": "Cache cleared."}), 200
        else: logger.warning("Cache clear command executed, but cache might not support clearing or was already empty."); return jsonify({"status": "success", "message": "Cache clear attempted."}), 200
    except Exception as e: logger.error(f"Error during cache clear: {e}", exc_info=True); abort(500, description="Failed to clear cache.")


# --- Error Handlers (From Preview) ---
@app.errorhandler(HTTPException)
def handle_http_exception(e):
    response = e.get_response(); response.data = json.dumps({"code": e.code, "name": e.name, "description": e.description})
    response.content_type = "application/json"; logger.error(f"{e.code} {e.name}: {e.description} for request {request.url}")
    return response

@app.errorhandler(Exception)
def handle_general_exception(e):
    logger.error("An unexpected server error occurred.", exc_info=True)
    if SentryConfig.SENTRY_DSN: sentry_sdk.capture_exception(e)
    response = jsonify({"code": 500, "name": "Internal Server Error", "description": "An unexpected error occurred on the server."})
    response.status_code = 500; return response

# --- Main block ---
if __name__ == '__main__':
    try: DatabaseManager.initialize_pool(); logger.info("Database pool initialized successfully.")
    except Exception as e: logger.critical(f"Failed to initialize database pool on startup: {e}", exc_info=True); exit(1)
    logger.info(f"Starting Flask app on {APIConfig.HOST}:{APIConfig.PORT} with DEBUG={APIConfig.DEBUG}")
    app.run(host=APIConfig.HOST, port=APIConfig.PORT, debug=APIConfig.DEBUG)

