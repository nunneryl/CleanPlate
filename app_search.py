# In file: app_search.py (Fully Corrected, Secured, and Automated)

import os
import logging
import threading
import secrets
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_caching import Cache
import psycopg
from psycopg.rows import dict_row
import smtplib
import ssl
from email.message import EmailMessage
import jwt
import requests

from db_manager import DatabaseConnection
from utils import normalize_search_term_for_hybrid
from config import APIConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# --- CACHE CONFIGURATION ---
cache_config = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_REDIS_URL": os.environ.get('REDIS_URL'),
    "CACHE_DEFAULT_TIMEOUT": 300
}
app.config.from_mapping(cache_config)
cache = Cache(app)

# --- DATA SHAPING HELPERS ---
def _group_and_shape_results(all_rows, ordered_camis):
    # This function is correct and remains unchanged.
    if not all_rows:
        return []
    restaurant_details_map = {str(camis): [] for camis in ordered_camis}
    for row in all_rows:
        restaurant_details_map[str(row['camis'])].append(row)
    final_results = []
    for camis in ordered_camis:
        camis_str = str(camis)
        rows_for_restaurant = restaurant_details_map.get(camis_str)
        if not rows_for_restaurant:
            continue
        base_info = dict(rows_for_restaurant[0])
        inspections = {}
        for row in rows_for_restaurant:
            insp_date_str = row['inspection_date'].isoformat()
            if insp_date_str not in inspections:
                inspections[insp_date_str] = {
                    'inspection_date': insp_date_str,
                    'grade': row.get('grade'),
                    'grade_date': row['grade_date'].isoformat() if row.get('grade_date') else None,
                    'critical_flag': row.get('critical_flag'),
                    'inspection_type': row.get('inspection_type'),
                    'action': row.get('action'),
                    'violations': []
                }
            if row.get('violation_code'):
                v_data = {'violation_code': row['violation_code'], 'violation_description': row['violation_description']}
                if v_data not in inspections[insp_date_str]['violations']:
                    inspections[insp_date_str]['violations'].append(v_data)
        base_info['inspections'] = sorted(list(inspections.values()), key=lambda x: x['inspection_date'], reverse=True)
        for key in ['violation_code', 'violation_description', 'grade', 'grade_date', 'action', 'inspection_date', 'critical_flag', 'inspection_type']:
            base_info.pop(key, None)
        final_results.append(base_info)
    return final_results

def _shape_simple_restaurant_list(rows):
    shaped_results = []
    for row in rows:
        inspection_data = {
            'inspection_date': row.get('inspection_date').isoformat() if row.get('inspection_date') else None,
            'critical_flag': row.get('critical_flag'),
            'grade': row.get('grade'),
            'inspection_type': row.get('inspection_type'),
            'action': row.get('action'),
            'violations': []
        }
        restaurant_data = dict(row)
        restaurant_data['inspections'] = [inspection_data]
        keys_to_remove = [
            'critical_flag', 'grade', 'inspection_type', 'action',
            'violation_code', 'violation_description', 'rn',
            'sort_date'
        ]
        for key in keys_to_remove:
            if key in restaurant_data:
                del restaurant_data[key]
        shaped_results.append(restaurant_data)
    return shaped_results

# --- SECURITY & AUTH HELPERS ---

# Apple Sign In Configuration
APPLE_BUNDLE_ID = os.environ.get('APPLE_BUNDLE_ID', 'nunzo.CleanPlate')
APPLE_KEYS_URL = "https://appleid.apple.com/auth/keys"
APPLE_ISSUER = "https://appleid.apple.com"

# Cache for Apple's public keys (refreshed periodically)
_apple_public_keys_cache = {"keys": None, "fetched_at": 0}
APPLE_KEYS_CACHE_DURATION = 3600  # 1 hour

def _get_apple_public_keys():
    """
    Fetches and caches Apple's public keys for token verification.
    Keys are cached for 1 hour to avoid excessive API calls.
    """
    import time
    current_time = time.time()

    # Return cached keys if still valid
    if (_apple_public_keys_cache["keys"] is not None and
        current_time - _apple_public_keys_cache["fetched_at"] < APPLE_KEYS_CACHE_DURATION):
        return _apple_public_keys_cache["keys"]

    try:
        response = requests.get(APPLE_KEYS_URL, timeout=10)
        response.raise_for_status()
        keys = response.json().get("keys", [])

        # Update cache
        _apple_public_keys_cache["keys"] = keys
        _apple_public_keys_cache["fetched_at"] = current_time
        logger.info("Apple public keys fetched and cached successfully.")
        return keys
    except requests.RequestException as e:
        logger.error(f"Failed to fetch Apple public keys: {e}")
        # Return cached keys if available, even if expired
        if _apple_public_keys_cache["keys"] is not None:
            logger.warning("Using expired cached Apple public keys as fallback.")
            return _apple_public_keys_cache["keys"]
        return None

def _get_apple_key_by_kid(kid):
    """
    Finds the Apple public key matching the given key ID (kid).
    """
    from jwt import algorithms

    keys = _get_apple_public_keys()
    if not keys:
        return None

    for key in keys:
        if key.get("kid") == kid:
            # Convert JWK to PEM format for PyJWT
            return algorithms.RSAAlgorithm.from_jwk(key)

    logger.error(f"No Apple public key found for kid: {kid}")
    return None

def verify_apple_token(token):
    """
    Verifies an Apple Sign In identity token.
    Returns the user ID (sub claim) if valid, None otherwise.
    """
    try:
        # Decode header without verification to get the key ID
        unverified_header = jwt.get_unverified_header(token)
        kid = unverified_header.get("kid")

        if not kid:
            logger.error("Token missing 'kid' in header")
            return None

        # Get the matching public key from Apple
        public_key = _get_apple_key_by_kid(kid)
        if not public_key:
            logger.error("Could not retrieve Apple public key for verification")
            return None

        # Verify and decode the token
        payload = jwt.decode(
            token,
            public_key,
            algorithms=["RS256"],
            audience=APPLE_BUNDLE_ID,
            issuer=APPLE_ISSUER
        )

        user_id = payload.get("sub")
        if user_id:
            logger.debug(f"Apple token verified successfully for user: {user_id[:8]}...")
        return user_id

    except jwt.ExpiredSignatureError:
        logger.warning("Apple token has expired")
        return None
    except jwt.InvalidAudienceError:
        logger.error(f"Token audience doesn't match. Expected: {APPLE_BUNDLE_ID}")
        return None
    except jwt.InvalidIssuerError:
        logger.error(f"Token issuer invalid. Expected: {APPLE_ISSUER}")
        return None
    except jwt.PyJWTError as e:
        logger.error(f"Token verification failed: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error during token verification: {e}", exc_info=True)
        return None

def _get_user_id_from_token(request):
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return None, jsonify({"error": "Authorization token is required"}), 401
    
    token = auth_header.split(' ')[1]
    user_id = verify_apple_token(token)
    
    if not user_id:
        return None, jsonify({"error": "Invalid or expired token"}), 401
        
    return user_id, None, None

def make_user_cache_key(*args, **kwargs):
    # Creates a cache key that is unique to the current user
    user_id, _, _ = _get_user_id_from_token(request)
    if user_id:
        return f"user_{user_id}_{request.path}"
    # Fallback for non-user-specific endpoints, though not used by get_favorites
    return request.path

# --- EMAIL HELPER ---

def send_report_email(data):
    """
    Sends an issue report email using AOL SMTP.
    Returns True on success, False on failure.
    """
    sender_email = os.environ.get('SENDER_EMAIL')
    sender_password = os.environ.get('SENDER_PASSWORD')
    receiver_email = os.environ.get('RECEIVER_EMAIL')

    if not all([sender_email, sender_password, receiver_email]):
        logger.error("Email configuration incomplete. Missing SENDER_EMAIL, SENDER_PASSWORD, or RECEIVER_EMAIL.")
        return False

    camis = data.get('camis', 'Unknown')
    issue_type = data.get('issue_type', 'Unknown')
    comments = data.get('comments', 'No comments provided')

    subject = f"CleanPlate Issue Report: {issue_type} (CAMIS: {camis})"
    body = f"""
New Issue Report from CleanPlate App
=====================================

Restaurant CAMIS: {camis}
Issue Type: {issue_type}

User Comments:
{comments}

=====================================
This is an automated message from CleanPlate.
    """.strip()

    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg.set_content(body)

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL('smtp.aol.com', 465, context=context) as server:
            server.login(sender_email, sender_password)
            server.send_message(msg)
        logger.info(f"Issue report email sent successfully for CAMIS {camis}")
        return True
    except smtplib.SMTPAuthenticationError as e:
        logger.error(f"SMTP authentication failed: {e}")
        return False
    except smtplib.SMTPException as e:
        logger.error(f"SMTP error sending report email: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error sending report email: {e}", exc_info=True)
        return False

# --- PUBLIC API ENDPOINTS ---

# Input validation constants
MAX_SEARCH_TERM_LENGTH = 200
MAX_PAGE = 1000
MAX_PER_PAGE = 100
MIN_PER_PAGE = 1

@app.route('/search', methods=['GET'])
@cache.cached(timeout=3600, query_string=True)
def search():
    search_term = request.args.get('name', '').strip()
    grade_filter = request.args.get('grade', type=str)
    boro_filter = request.args.get('boro', type=str)
    cuisine_filter = request.args.get('cuisine', type=str)
    sort_option = request.args.get('sort', type=str)

    # Validate and bound pagination parameters
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 25, type=int)
    page = max(1, min(MAX_PAGE, page))
    per_page = max(MIN_PER_PAGE, min(MAX_PER_PAGE, per_page))

    if not search_term:
        return jsonify([])

    # Validate search term length
    if len(search_term) > MAX_SEARCH_TERM_LENGTH:
        return jsonify({"error": f"Search term too long. Maximum {MAX_SEARCH_TERM_LENGTH} characters."}), 400

    normalized_search = normalize_search_term_for_hybrid(search_term)
    
    where_conditions = ["(dba_normalized_search ILIKE %s OR similarity(dba_normalized_search, %s) > 0.4)"]
    params = [f"%{normalized_search}%", normalized_search]

    if grade_filter:
        grade_upper = grade_filter.upper()
        if grade_upper == 'P':
            where_conditions.append("grade IN ('P', 'Z')")
        elif grade_upper == 'CLOSED':
            where_conditions.append("action ILIKE %s")
            params.append('%closed by dohmh%')
        else:
            where_conditions.append("grade = %s")
            params.append(grade_upper)
            
    if boro_filter:
        where_conditions.append("boro ILIKE %s")
        params.append(boro_filter)
    if cuisine_filter:
        where_conditions.append("cuisine_description ILIKE %s")
        params.append(f"%{cuisine_filter}%")
        
    where_clause = " AND ".join(where_conditions)

    order_by_clause = ""
    order_by_params = []
    if sort_option == 'name_asc':
        order_by_clause = "ORDER BY dba ASC"
    elif sort_option == 'name_desc':
        order_by_clause = "ORDER BY dba DESC"
    elif sort_option == 'date_desc':
        order_by_clause = "ORDER BY inspection_date DESC"
    elif sort_option == 'grade_asc':
        order_by_clause = "ORDER BY CASE WHEN grade = 'A' THEN 1 WHEN grade = 'B' THEN 2 WHEN grade = 'C' THEN 3 ELSE 4 END, dba ASC"
    else:
        order_by_clause = "ORDER BY CASE WHEN dba_normalized_search = %s THEN 0 WHEN dba_normalized_search ILIKE %s THEN 1 ELSE 2 END, similarity(dba_normalized_search, %s) DESC, length(dba_normalized_search)"
        order_by_params = [normalized_search, f"{normalized_search}%", normalized_search]

    id_fetch_query = f"""
        WITH latest_restaurants AS (
            SELECT DISTINCT ON (camis) *
            FROM restaurants
            ORDER BY camis, inspection_date DESC
        )
        SELECT camis 
        FROM latest_restaurants 
        WHERE {where_clause}
        {order_by_clause}
        LIMIT %s OFFSET %s;
    """
    offset = (page - 1) * per_page
    id_fetch_params = tuple(params + order_by_params + [per_page, offset])

    try:
        with DatabaseConnection() as conn:
            conn.row_factory = dict_row
            with conn.cursor() as cursor:
                cursor.execute(id_fetch_query, id_fetch_params)
                paginated_camis_tuples = cursor.fetchall()
            if not paginated_camis_tuples:
                return jsonify([])
            paginated_camis = [item['camis'] for item in paginated_camis_tuples]
            details_query = "SELECT r.*, v.violation_code, v.violation_description FROM restaurants r LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date WHERE r.camis = ANY(%s)"
            with conn.cursor() as details_cursor:
                details_cursor.execute(details_query, (paginated_camis,))
                all_rows = details_cursor.fetchall()
    except Exception as e:
        logger.error(f"DB search failed for '{search_term}': {e}", exc_info=True)
        return jsonify({"error": "Database query failed"}), 500

    final_results = _group_and_shape_results(all_rows, paginated_camis)
    return jsonify(final_results)

@app.route('/restaurant/<string:camis>', methods=['GET'])
@cache.cached(timeout=3600)
def get_restaurant_by_camis(camis):
    if not camis.isdigit():
        return jsonify({"error": "Invalid CAMIS format"}), 400
    
    try:
        with DatabaseConnection() as conn:
            conn.row_factory = dict_row
            details_query = """
                SELECT r.*, v.violation_code, v.violation_description 
                FROM restaurants r 
                LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date 
                WHERE r.camis = %s
            """
            with conn.cursor() as details_cursor:
                details_cursor.execute(details_query, (camis,))
                all_rows = details_cursor.fetchall()

            if not all_rows:
                return jsonify({"error": "Restaurant not found"}), 404

    except Exception as e:
        logger.error(f"DB query failed for CAMIS '{camis}': {e}", exc_info=True)
        return jsonify({"error": "Database query failed"}), 500

    final_results = _group_and_shape_results(all_rows, [camis])
    if not final_results:
        return jsonify({"error": "Failed to shape restaurant data"}), 500

    return jsonify(final_results[0])

@app.route('/lists/recent-actions', methods=['GET'])
@cache.cached(timeout=3600)
def get_recent_actions():
    graded_query = """
        WITH all_recent_events AS (
            (
                -- Part 1: Select newly graded restaurants from the last 7 days
                SELECT *,
                       grade_date as sort_date,
                       'new_grade' as update_type,
                       NULL::timestamptz as finalized_date
                FROM restaurants
                -- Using NOW() for a consistent timestamp comparison
                WHERE grade_date >= (NOW() - INTERVAL '7 days')
            )
            UNION ALL
            (
                -- Part 2: Select restaurants whose pending grade was recently finalized
                SELECT r.*,
                       gu.update_date as sort_date,
                       gu.update_type,
                       gu.update_date as finalized_date
                FROM grade_updates gu
                JOIN restaurants r ON gu.restaurant_camis = r.camis AND gu.inspection_date = r.inspection_date::date
                WHERE gu.update_date >= (NOW() - INTERVAL '14 days')
            )
        ),
        most_recent_per_restaurant AS (
            -- From all events, pick only the single most recent one for each restaurant,
            -- PRIORITIZING the newest inspection date above all else.
            SELECT DISTINCT ON (camis) *
            FROM all_recent_events
            ORDER BY camis, inspection_date DESC, sort_date DESC
        )
        -- Finally, sort the result list for display by its relevant date
        SELECT *
        FROM most_recent_per_restaurant
        ORDER BY sort_date DESC
        LIMIT 200;
    """

    actions_query = """
        WITH latest_inspections AS (
            SELECT DISTINCT ON (camis) *
            FROM restaurants
            ORDER BY camis, inspection_date DESC
        )
        SELECT *
        FROM latest_inspections
        WHERE (action ILIKE '%%closed by dohmh%%' OR action ILIKE '%%re-opened%%')
          AND inspection_date >= '2022-01-01'
        ORDER BY inspection_date DESC;
    """

    try:
        with DatabaseConnection() as conn:
            conn.row_factory = dict_row
            with conn.cursor() as cursor:
                cursor.execute(graded_query)
                graded_results = cursor.fetchall()

                cursor.execute(actions_query)
                action_results = cursor.fetchall()

            closed_rows = [row for row in action_results if 'closed' in row.get('action', '').lower()]
            reopened_rows = [row for row in action_results if 're-opened' in row.get('action', '').lower()]

            shaped_graded = _shape_simple_restaurant_list(graded_results)
            shaped_closed = _shape_simple_restaurant_list(closed_rows)
            shaped_reopened = _shape_simple_restaurant_list(reopened_rows)

            return jsonify({
                "recently_graded": shaped_graded,
                "recently_closed": shaped_closed,
                "recently_reopened": shaped_reopened
            })
            
    except Exception as e:
        logger.error(f"DB query for recent-actions list failed: {e}", exc_info=True)
        return jsonify({"error": "Database query failed"}), 500

@app.route('/users', methods=['POST'])
def create_user():
    if not request.is_json: return jsonify({"error": "Request must be JSON"}), 400
    data = request.get_json()
    token = data.get('identityToken')
    if not token: return jsonify({"error": "identityToken is required"}), 400

    user_id = verify_apple_token(token)
    if not user_id:
        return jsonify({"error": "Invalid token"}), 400

    insert_query = "INSERT INTO users (id) VALUES (%s) ON CONFLICT (id) DO NOTHING;"
    try:
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

    # Validate camis format (should be numeric string, max 10 digits)
    camis_str = str(camis)
    if not camis_str.isdigit() or len(camis_str) > 10:
        return jsonify({"error": "Invalid CAMIS format"}), 400
    
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
        with DatabaseConnection() as conn:
            conn.row_factory = dict_row
            with conn.cursor() as cursor:
                cursor.execute(query, (user_id,))
                all_rows = cursor.fetchall()
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

    # Validate search term length
    if len(search_term) > MAX_SEARCH_TERM_LENGTH:
        return jsonify({"error": f"Search term too long. Maximum {MAX_SEARCH_TERM_LENGTH} characters."}), 400

    search_term_display = search_term.strip()
    search_term_normalized = search_term_display.lower()

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
        with DatabaseConnection() as conn:
            conn.row_factory = dict_row
            with conn.cursor() as cursor:
                cursor.execute(query, (user_id,))
                results = cursor.fetchall()
            
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

# --- ADMINISTRATIVE ENDPOINTS ---

MAX_COMMENTS_LENGTH = 2000
VALID_ISSUE_TYPES = {"Permanently Closed", "Wrong Address / Map Pin", "Wrong Restaurant Name", "Other"}

@app.route('/report-issue', methods=['POST'])
def report_issue():
    if not request.is_json: return jsonify({"error": "Request must be JSON"}), 400
    data = request.get_json()
    if not all(k in data for k in ["camis", "issue_type", "comments"]):
        return jsonify({"error": "Missing required fields"}), 400

    # Validate camis format
    camis = str(data.get('camis', ''))
    if not camis.isdigit() or len(camis) > 10:
        return jsonify({"error": "Invalid CAMIS format"}), 400

    # Validate issue type
    issue_type = data.get('issue_type', '')
    if issue_type not in VALID_ISSUE_TYPES:
        return jsonify({"error": f"Invalid issue type. Must be one of: {', '.join(VALID_ISSUE_TYPES)}"}), 400

    # Validate comments length
    comments = data.get('comments', '')
    if len(comments) > MAX_COMMENTS_LENGTH:
        return jsonify({"error": f"Comments too long. Maximum {MAX_COMMENTS_LENGTH} characters."}), 400

    if send_report_email(data):
        return jsonify({"status": "success", "message": "Report received."}), 200
    else:
        return jsonify({"status": "error", "message": "Failed to process report."}), 500

@app.route('/trigger-update', methods=['POST'])
def trigger_update():
    try:
        from update_database import run_database_update
    except ImportError:
        return jsonify({"status": "error", "message": "Update logic currently unavailable."}), 503
    
    provided_key = request.headers.get('X-Update-Secret')
    expected_key = APIConfig.UPDATE_SECRET_KEY
    if not expected_key or not provided_key or not secrets.compare_digest(provided_key, expected_key):
        return jsonify({"status": "error", "message": "Unauthorized."}), 403
    
    threading.Thread(target=run_database_update, args=(15,), daemon=True).start()
    return jsonify({"status": "success", "message": "Database update triggered in background."}), 202

@app.route('/clear-cache', methods=['POST'])
def clear_cache():
    provided_key = request.headers.get('X-Update-Secret')
    expected_key = APIConfig.UPDATE_SECRET_KEY
    if not expected_key or not provided_key or not secrets.compare_digest(provided_key, expected_key):
        return jsonify({"status": "error", "message": "Unauthorized."}), 403
    
    cache.clear()
    logger.info("Cache cleared successfully via API endpoint.")
    return jsonify({"status": "success", "message": "Cache cleared."}), 200

# --- ERROR HANDLERS ---

@app.errorhandler(404)
def not_found_error_handler(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_server_error_handler(error):
    logger.error(f"Internal Server Error (500): {error}", exc_info=True)
    return jsonify({"error": "An internal server error occurred"}), 500

if __name__ == "__main__":
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", 8080))
    app.run(host=host, port=port)
