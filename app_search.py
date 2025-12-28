# In file: app_search.py (Fully Corrected, Secured, and Automated)

import os
import logging
import threading
import secrets
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_caching import Cache
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import psycopg
from psycopg.rows import dict_row
import smtplib
import ssl
from email.message import EmailMessage
import jwt
import requests
import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration

from db_manager import DatabaseConnection
from utils import normalize_search_term_for_hybrid
from config import APIConfig, SentryConfig

if SentryConfig.SENTRY_DSN:
    sentry_sdk.init(
        dsn=SentryConfig.SENTRY_DSN,
        integrations=[FlaskIntegration()],
        traces_sample_rate=0.1,
        profiles_sample_rate=0.1,
        environment=os.environ.get("RAILWAY_ENVIRONMENT", "development"),
    )
    logging.info("Sentry initialized successfully")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# --- CORS CONFIGURATION (Restricted) ---
ALLOWED_ORIGINS = [
    "capacitor://localhost",  # iOS app
    "http://localhost",       # Local development
    "https://localhost",      # Local development
]
CORS(app, resources={
    r"/*": {
        "origins": ALLOWED_ORIGINS,
        "methods": ["GET", "POST", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization", "X-Update-Secret"]
    }
})

# --- RATE LIMITING CONFIGURATION ---
limiter = Limiter(
    key_func=get_remote_address,
    app=app,
    default_limits=["200 per day", "60 per minute"],
    storage_uri=os.environ.get('REDIS_URL', "memory://"),
)

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
        # Note: 'grade' is intentionally NOT removed - iOS uses it for list view display
        # 'update_type' and 'previous_grade' are kept for iOS to show "Updated from X"
        keys_to_remove = [
            'critical_flag', 'inspection_type', 'action',
            'violation_code', 'violation_description', 'rn',
            'sort_date'
        ]
        for key in keys_to_remove:
            if key in restaurant_data:
                del restaurant_data[key]
        shaped_results.append(restaurant_data)
    return shaped_results

# --- SECURITY & AUTH HELPERS (UPDATED) ---

# Cache for Apple's public keys (refreshed periodically)
_apple_keys_cache = {"keys": None, "fetched_at": 0}
_APPLE_KEYS_URL = "https://appleid.apple.com/auth/keys"
_APPLE_KEYS_CACHE_DURATION = 3600  # 1 hour

def _get_apple_public_keys():
    """Fetch and cache Apple's public keys for JWT verification."""
    import time
    current_time = time.time()

    if _apple_keys_cache["keys"] and (current_time - _apple_keys_cache["fetched_at"]) < _APPLE_KEYS_CACHE_DURATION:
        return _apple_keys_cache["keys"]

    try:
        response = requests.get(_APPLE_KEYS_URL, timeout=10)
        response.raise_for_status()
        keys = response.json().get("keys", [])
        _apple_keys_cache["keys"] = keys
        _apple_keys_cache["fetched_at"] = current_time
        logger.info("Successfully fetched Apple public keys")
        return keys
    except Exception as e:
        logger.error(f"Failed to fetch Apple public keys: {e}")
        # Return cached keys if available, even if expired
        return _apple_keys_cache["keys"] or []

def verify_apple_token(token):
    """Verify Apple Sign-In JWT token with proper signature validation."""
    try:
        # Get the key ID from the token header
        unverified_header = jwt.get_unverified_header(token)
        kid = unverified_header.get("kid")

        if not kid:
            logger.error("Token missing 'kid' in header")
            return None

        # Fetch Apple's public keys
        apple_keys = _get_apple_public_keys()

        # Find the matching key
        matching_key = None
        for key in apple_keys:
            if key.get("kid") == kid:
                matching_key = key
                break

        if not matching_key:
            logger.error(f"No matching Apple public key found for kid: {kid}")
            return None

        # Convert JWK to PEM format
        from jwt.algorithms import RSAAlgorithm
        public_key = RSAAlgorithm.from_jwk(matching_key)

        # Verify and decode the token
        payload = jwt.decode(
            token,
            public_key,
            algorithms=["RS256"],
            audience="nunzo.CleanPlate",  # Your app's bundle ID
            issuer="https://appleid.apple.com"
        )

        user_id = payload.get("sub")
        logger.info(f"Successfully verified token for user: {user_id[:8]}...")
        return user_id

    except jwt.ExpiredSignatureError:
        logger.warning("Apple token has expired")
        return None
    except jwt.InvalidAudienceError:
        logger.warning("Apple token has invalid audience")
        return None
    except jwt.InvalidIssuerError:
        logger.warning("Apple token has invalid issuer")
        return None
    except jwt.PyJWTError as e:
        logger.error(f"Token verification failed: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error verifying token: {e}")
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
    """Creates a cache key unique to the current user.
    Returns None to disable caching if user is not authenticated."""
    user_id, _, _ = _get_user_id_from_token(request)
    if user_id:
        return f"user_{user_id}_{request.path}"
    # Return unique key per request to prevent caching for unauthenticated users
    # This prevents data leakage between users
    return f"nocache_{id(request)}"

# --- PUBLIC API ENDPOINTS ---

@app.route('/search', methods=['GET'])
@limiter.limit("60 per minute")
@cache.cached(timeout=3600, query_string=True)
def search():
    search_term = request.args.get('name', '').strip()
    grade_filter = request.args.get('grade', type=str)
    boro_filter = request.args.get('boro', type=str)
    cuisine_filter = request.args.get('cuisine', type=str)
    sort_option = request.args.get('sort', type=str)
    page = int(request.args.get('page', 1, type=int))
    per_page = int(request.args.get('per_page', 25, type=int))

    if not search_term:
        return jsonify([])

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
# CAMIS validation: must be digits only and 8 characters (NYC standard)
    if not camis.isdigit() or len(camis) != 8:
        return jsonify({"error": "Invalid CAMIS format - must be 8 digits"}), 400

    
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
    graded_results = []
    action_results = []
    
    graded_query = """
        WITH grade_update_restaurants AS (
            SELECT DISTINCT ON (gu.restaurant_camis)
                r.*,
                gu.new_grade as grade,
                gu.update_type,
                gu.previous_grade,
                gu.update_date as sort_date
            FROM grade_updates gu
            JOIN (
                SELECT DISTINCT ON (camis) *
                FROM restaurants
                ORDER BY camis, inspection_date DESC
            ) r ON gu.restaurant_camis = r.camis
            WHERE gu.update_date >= (NOW() - INTERVAL '180 days')
              AND gu.new_grade IN ('A', 'B', 'C')
            ORDER BY gu.restaurant_camis, gu.update_date DESC
        )
        SELECT * FROM grade_update_restaurants
        ORDER BY sort_date DESC
        LIMIT 200;
    """

    # Query for closed/reopened restaurants
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
                try:
                    cursor.execute(graded_query)
                    graded_results = cursor.fetchall()
                    logger.info(f"Graded query returned {len(graded_results)} results")
                except Exception as e:
                    logger.error(f"Graded query failed: {e}")
                    graded_results = []

                try:
                    cursor.execute(actions_query)
                    action_results = cursor.fetchall()
                    logger.info(f"Actions query returned {len(action_results)} results")
                except Exception as e:
                    logger.error(f"Actions query failed: {e}")
                    action_results = []

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

def send_report_email(data):
    """Send report email to admin. Returns True on success, False on failure."""
    # Get email config from environment
    smtp_server = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", 587))
    sender_email = os.environ.get("REPORT_SENDER_EMAIL")
    sender_password = os.environ.get("REPORT_SENDER_PASSWORD")
    recipient_email = os.environ.get("REPORT_RECIPIENT_EMAIL")

    # If email not configured, log the report and return success
    if not all([sender_email, sender_password, recipient_email]):
        logger.warning("Email not configured. Logging report instead.")
        logger.info(f"REPORT: CAMIS={data.get('camis')}, Type={data.get('issue_type')}, Comments={data.get('comments')}")
        return True

    try:
        msg = EmailMessage()
        msg['Subject'] = f"CleanPlate Issue Report - {data.get('issue_type')}"
        msg['From'] = sender_email
        msg['To'] = recipient_email

        body = f"""
New issue report received:

Restaurant CAMIS: {data.get('camis')}
Issue Type: {data.get('issue_type')}
Comments: {data.get('comments')}

---
Sent from CleanPlate API
        """
        msg.set_content(body)

        context = ssl.create_default_context()
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls(context=context)
            server.login(sender_email, sender_password)
            server.send_message(msg)

        logger.info(f"Report email sent for CAMIS {data.get('camis')}")
        return True

    except Exception as e:
        logger.error(f"Failed to send report email: {e}")
        return False

@app.route('/report-issue', methods=['POST'])
@limiter.limit("5 per hour")  # Stricter limit to prevent spam
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
