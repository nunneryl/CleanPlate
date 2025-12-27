# app_search.py - CleanPlate Backend API
# Corrected to match actual database schema

import os
import json
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from functools import wraps

import jwt
import requests
import sentry_sdk
from flask import Flask, request, jsonify, g
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_caching import Cache
from dotenv import load_dotenv
from psycopg_pool import ConnectionPool

load_dotenv()

# Initialize Flask app
app = Flask(__name__)

# Sentry initialization (if configured)
if os.getenv("SENTRY_DSN"):
    sentry_sdk.init(
        dsn=os.getenv("SENTRY_DSN"),
        traces_sample_rate=0.1,
        environment=os.getenv("ENVIRONMENT", "production")
    )

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# SECURITY CONFIGURATION
# =============================================================================

ALLOWED_ORIGINS = [
    "https://cleanplate-production.up.railway.app",
    "https://cleanplate-cleanplate-pr-21.up.railway.app",
]

CORS(app, origins=ALLOWED_ORIGINS, supports_credentials=True)

limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["200 per day", "50 per hour"],
    storage_uri=os.getenv("REDIS_URL", "memory://"),
)

cache_config = {
    "CACHE_TYPE": "RedisCache" if os.getenv("REDIS_URL") else "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}
if os.getenv("REDIS_URL"):
    cache_config["CACHE_REDIS_URL"] = os.getenv("REDIS_URL")

app.config.from_mapping(cache_config)
cache = Cache(app)

# Apple Sign-In configuration
APPLE_BUNDLE_ID = "nunzo.CleanPlate"
APPLE_KEYS_URL = "https://appleid.apple.com/auth/keys"
_apple_public_keys_cache = {"keys": None, "expires_at": None}

# Database connection pool
DATABASE_URL = os.getenv("DATABASE_URL")
pool = ConnectionPool(DATABASE_URL, min_size=2, max_size=10) if DATABASE_URL else None

# Email configuration
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
REPORT_EMAIL_RECIPIENT = os.getenv("REPORT_EMAIL_RECIPIENT", "support@cleanplate.app")


# =============================================================================
# APPLE SIGN-IN TOKEN VERIFICATION
# =============================================================================

def get_apple_public_keys():
    """Fetch and cache Apple's public keys for JWT verification."""
    now = datetime.utcnow()
    
    if (_apple_public_keys_cache["keys"] is not None and
        _apple_public_keys_cache["expires_at"] is not None and
        now < _apple_public_keys_cache["expires_at"]):
        return _apple_public_keys_cache["keys"]
    
    try:
        response = requests.get(APPLE_KEYS_URL, timeout=10)
        response.raise_for_status()
        keys = response.json().get("keys", [])
        
        _apple_public_keys_cache["keys"] = keys
        _apple_public_keys_cache["expires_at"] = now + timedelta(hours=24)
        
        return keys
    except requests.RequestException as e:
        logger.error(f"Failed to fetch Apple public keys: {e}")
        if _apple_public_keys_cache["keys"]:
            return _apple_public_keys_cache["keys"]
        raise


def get_apple_public_key(kid):
    """Get a specific Apple public key by key ID."""
    keys = get_apple_public_keys()
    for key in keys:
        if key.get("kid") == kid:
            return key
    return None


def verify_apple_token(identity_token):
    """Verify an Apple Sign-In identity token."""
    try:
        unverified_header = jwt.get_unverified_header(identity_token)
        kid = unverified_header.get("kid")
        
        if not kid:
            raise ValueError("Token missing key ID (kid)")
        
        apple_key = get_apple_public_key(kid)
        if not apple_key:
            _apple_public_keys_cache["keys"] = None
            apple_key = get_apple_public_key(kid)
            if not apple_key:
                raise ValueError(f"No matching Apple public key found for kid: {kid}")
        
        from jwt.algorithms import RSAAlgorithm
        public_key = RSAAlgorithm.from_jwk(json.dumps(apple_key))
        
        decoded = jwt.decode(
            identity_token,
            public_key,
            algorithms=["RS256"],
            audience=APPLE_BUNDLE_ID,
            issuer="https://appleid.apple.com",
            options={
                "verify_signature": True,
                "verify_aud": True,
                "verify_iss": True,
                "verify_exp": True,
                "require": ["sub", "aud", "iss", "exp", "iat"]
            }
        )
        
        return decoded
        
    except jwt.ExpiredSignatureError:
        raise ValueError("Token has expired")
    except jwt.InvalidAudienceError:
        raise ValueError("Invalid token audience")
    except jwt.InvalidIssuerError:
        raise ValueError("Invalid token issuer")
    except jwt.InvalidSignatureError:
        raise ValueError("Invalid token signature")
    except jwt.DecodeError as e:
        raise ValueError("Invalid token format")
    except Exception as e:
        logger.error(f"Apple token verification failed: {e}")
        raise ValueError(f"Token verification failed: {str(e)}")


def require_auth(f):
    """Decorator to require valid Apple Sign-In authentication."""
    @wraps(f)
    def decorated(*args, **kwargs):
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return jsonify({"error": "Missing or invalid authorization header"}), 401
        
        token = auth_header.split(" ", 1)[1]
        
        try:
            decoded = verify_apple_token(token)
            g.user_id = decoded.get("sub")
        except ValueError as e:
            return jsonify({"error": str(e)}), 401
        
        return f(*args, **kwargs)
    return decorated


# =============================================================================
# EMAIL FUNCTIONALITY
# =============================================================================

def send_report_email(camis, issue_type, comments, user_id=None):
    """Send an email notification for a reported issue."""
    if not all([SMTP_USER, SMTP_PASSWORD]):
        logger.warning("Email not configured - SMTP_USER or SMTP_PASSWORD missing")
        return False
    
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"[CleanPlate] Issue Report - {issue_type} - CAMIS: {camis}"
        msg["From"] = SMTP_USER
        msg["To"] = REPORT_EMAIL_RECIPIENT
        
        text_content = f"""
New Issue Report from CleanPlate App

Restaurant CAMIS: {camis}
Issue Type: {issue_type}
Reporter User ID: {user_id or 'Anonymous'}

Comments:
{comments or 'No additional comments provided.'}

---
Reported at: {datetime.utcnow().isoformat()}Z
        """
        
        html_content = f"""
<html>
<body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
    <h2 style="color: #333;">New Issue Report</h2>
    <table style="width: 100%; border-collapse: collapse;">
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">Restaurant CAMIS</td>
            <td style="padding: 8px; border: 1px solid #ddd;">{camis}</td>
        </tr>
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">Issue Type</td>
            <td style="padding: 8px; border: 1px solid #ddd;">{issue_type}</td>
        </tr>
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">Reporter</td>
            <td style="padding: 8px; border: 1px solid #ddd;">{user_id or 'Anonymous'}</td>
        </tr>
    </table>
    <h3 style="color: #333; margin-top: 20px;">Comments</h3>
    <p style="background: #f5f5f5; padding: 15px; border-radius: 5px;">
        {comments or 'No additional comments provided.'}
    </p>
    <hr style="margin-top: 30px;">
    <p style="color: #666; font-size: 12px;">
        Reported at: {datetime.utcnow().isoformat()}Z
    </p>
</body>
</html>
        """
        
        msg.attach(MIMEText(text_content, "plain"))
        msg.attach(MIMEText(html_content, "html"))
        
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, REPORT_EMAIL_RECIPIENT, msg.as_string())
        
        logger.info(f"Report email sent successfully for CAMIS: {camis}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to send report email: {e}")
        return False


# =============================================================================
# CACHE KEY HELPERS
# =============================================================================

def get_user_cache_key(prefix, user_id):
    """Generate a user-specific cache key."""
    if not user_id:
        raise ValueError("user_id is required for user-specific cache keys")
    return f"{prefix}:user:{user_id}"


# =============================================================================
# API ENDPOINTS
# =============================================================================

@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint."""
    return jsonify({"status": "healthy", "timestamp": datetime.utcnow().isoformat()})


@app.route("/search", methods=["GET"])
@limiter.limit("30 per minute")
def search_restaurants():
    """
    Search for restaurants by name.
    Returns UNIQUE restaurants with their LATEST inspection data.
    """
    name = request.args.get("name", "").strip()
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 20, type=int)
    grade = request.args.get("grade", "").strip()
    boro = request.args.get("boro", "").strip()
    cuisine = request.args.get("cuisine", "").strip()
    sort = request.args.get("sort", "").strip()
    
    # Input validation
    if len(name) > 200:
        return jsonify({"error": "Search term too long (max 200 characters)"}), 400
    if per_page > 100:
        per_page = 100
    if page < 1:
        page = 1
    
    cache_key = f"search:{name}:{page}:{per_page}:{grade}:{boro}:{cuisine}:{sort}"
    cached_result = cache.get(cache_key)
    if cached_result:
        return jsonify(cached_result)
    
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Get unique restaurants with their LATEST inspection
                # Using DISTINCT ON to get one row per camis (the most recent)
                query = """
                    SELECT DISTINCT ON (camis)
                        camis, dba, building, street, boro, zipcode, phone,
                        cuisine_description, grade, grade_date, latitude, longitude,
                        google_rating, google_review_count, price_level
                    FROM restaurants
                    WHERE LOWER(dba) LIKE LOWER(%s)
                """
                params = [f"%{name}%"]
                
                if grade:
                    query += " AND grade = %s"
                    params.append(grade)
                if boro:
                    query += " AND LOWER(boro) = LOWER(%s)"
                    params.append(boro)
                if cuisine:
                    query += " AND LOWER(cuisine_description) LIKE LOWER(%s)"
                    params.append(f"%{cuisine}%")
                
                # DISTINCT ON requires ORDER BY to start with the same columns
                query += " ORDER BY camis, inspection_date DESC"
                
                # Wrap in subquery for additional sorting and pagination
                wrapper_query = f"""
                    SELECT * FROM ({query}) AS unique_restaurants
                """
                
                if sort == "grade":
                    wrapper_query += " ORDER BY grade ASC NULLS LAST"
                elif sort == "name":
                    wrapper_query += " ORDER BY dba ASC"
                elif sort == "rating":
                    wrapper_query += " ORDER BY google_rating DESC NULLS LAST"
                else:
                    wrapper_query += " ORDER BY grade_date DESC NULLS LAST"
                
                wrapper_query += " LIMIT %s OFFSET %s"
                params.extend([per_page, (page - 1) * per_page])
                
                cur.execute(wrapper_query, params)
                rows = cur.fetchall()
                
                results = []
                for row in rows:
                    results.append({
                        "camis": row[0],
                        "dba": row[1],
                        "building": row[2],
                        "street": row[3],
                        "boro": row[4],
                        "zipcode": row[5],
                        "phone": row[6],
                        "cuisine_description": row[7],
                        "grade": row[8],
                        "grade_date": row[9].isoformat() if row[9] else None,
                        "latitude": float(row[10]) if row[10] else None,
                        "longitude": float(row[11]) if row[11] else None,
                        "google_rating": float(row[12]) if row[12] else None,
                        "google_review_count": row[13],
                        "price_level": row[14],
                    })
                
                cache.set(cache_key, results, timeout=300)
                return jsonify(results)
                
    except Exception as e:
        logger.error(f"Search error: {e}")
        return jsonify({"error": "Search failed"}), 500


@app.route("/restaurant/<camis>", methods=["GET"])
@limiter.limit("60 per minute")
def get_restaurant(camis):
    """
    Get restaurant details with full inspection history and violations.
    Returns: restaurant info + list of inspections, each with their violations.
    """
    if not camis.isdigit() or len(camis) > 10:
        return jsonify({"error": "Invalid CAMIS format"}), 400
    
    cache_key = f"restaurant_detail:{camis}"
    cached_result = cache.get(cache_key)
    if cached_result:
        return jsonify(cached_result)
    
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Get the latest restaurant info (for header display)
                cur.execute("""
                    SELECT DISTINCT ON (camis)
                        camis, dba, building, street, boro, zipcode, phone,
                        cuisine_description, grade, grade_date, latitude, longitude,
                        google_rating, google_review_count, website, hours,
                        google_maps_url, price_level
                    FROM restaurants
                    WHERE camis = %s
                    ORDER BY camis, inspection_date DESC
                """, (camis,))
                
                row = cur.fetchone()
                if not row:
                    return jsonify({"error": "Restaurant not found"}), 404
                
                # Build restaurant base info
                result = {
                    "camis": row[0],
                    "dba": row[1],
                    "building": row[2],
                    "street": row[3],
                    "boro": row[4],
                    "zipcode": row[5],
                    "phone": row[6],
                    "cuisine_description": row[7],
                    "grade": row[8],  # Latest grade
                    "grade_date": row[9].isoformat() if row[9] else None,
                    "latitude": float(row[10]) if row[10] else None,
                    "longitude": float(row[11]) if row[11] else None,
                    "google_rating": float(row[12]) if row[12] else None,
                    "google_review_count": row[13],
                    "website": row[14],
                    "hours": row[15],  # JSONB field
                    "google_maps_url": row[16],
                    "price_level": row[17],
                    "inspections": []
                }
                
                # Get ALL inspections for this restaurant
                cur.execute("""
                    SELECT 
                        inspection_date, grade, grade_date, critical_flag,
                        inspection_type, action
                    FROM restaurants
                    WHERE camis = %s
                    ORDER BY inspection_date DESC
                """, (camis,))
                
                inspections_rows = cur.fetchall()
                
                # Get ALL violations for this restaurant (we'll group them by inspection_date)
                cur.execute("""
                    SELECT 
                        inspection_date, violation_code, violation_description
                    FROM violations
                    WHERE camis = %s
                    ORDER BY inspection_date DESC, violation_code
                """, (camis,))
                
                violations_rows = cur.fetchall()
                
                # Group violations by inspection_date
                violations_by_date = {}
                for v_row in violations_rows:
                    insp_date = v_row[0]
                    if insp_date not in violations_by_date:
                        violations_by_date[insp_date] = []
                    violations_by_date[insp_date].append({
                        "violation_code": v_row[1],
                        "violation_description": v_row[2]
                    })
                
                # Build inspections list with violations
                for insp_row in inspections_rows:
                    inspection_date = insp_row[0]
                    inspection = {
                        "inspection_date": inspection_date.isoformat() if inspection_date else None,
                        "grade": insp_row[1],
                        "grade_date": insp_row[2].isoformat() if insp_row[2] else None,
                        "critical_flag": insp_row[3],
                        "inspection_type": insp_row[4],
                        "action": insp_row[5],
                        "violations": violations_by_date.get(inspection_date, [])
                    }
                    result["inspections"].append(inspection)
                
                cache.set(cache_key, result, timeout=600)
                return jsonify(result)
                
    except Exception as e:
        logger.error(f"Restaurant lookup error: {e}")
        return jsonify({"error": "Lookup failed"}), 500


@app.route("/lists/recent-actions", methods=["GET"])
@limiter.limit("30 per minute")
def get_recent_actions():
    """
    Get recently graded, closed, and reopened restaurants.
    Returns full Restaurant objects as expected by iOS.
    """
    cache_key = "recent_actions"
    cached_result = cache.get(cache_key)
    if cached_result:
        return jsonify(cached_result)
    
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Get recently graded restaurants (full Restaurant objects)
                cur.execute("""
                    SELECT DISTINCT ON (g.restaurant_camis)
                        r.camis, r.dba, r.building, r.street, r.boro, r.zipcode,
                        r.phone, r.cuisine_description, r.grade, r.grade_date,
                        r.latitude, r.longitude, r.google_rating, r.google_review_count,
                        g.update_type, g.update_date
                    FROM grade_updates g
                    JOIN (
                        SELECT DISTINCT ON (camis) *
                        FROM restaurants
                        ORDER BY camis, inspection_date DESC
                    ) r ON g.restaurant_camis = r.camis
                    ORDER BY g.restaurant_camis, g.update_date DESC
                """)
                
                graded_rows = cur.fetchall()
                
                # Sort by update_date descending and limit to 50
                recently_graded = sorted([
                    {
                        "camis": row[0],
                        "dba": row[1],
                        "building": row[2],
                        "street": row[3],
                        "boro": row[4],
                        "zipcode": row[5],
                        "phone": row[6],
                        "cuisine_description": row[7],
                        "grade": row[8],
                        "grade_date": row[9].isoformat() if row[9] else None,
                        "latitude": float(row[10]) if row[10] else None,
                        "longitude": float(row[11]) if row[11] else None,
                        "google_rating": float(row[12]) if row[12] else None,
                        "google_review_count": row[13],
                        "update_type": row[14],
                        "activity_date": row[15].isoformat() if row[15] else None,
                    }
                    for row in graded_rows
                ], key=lambda x: x["activity_date"] or "", reverse=True)[:50]
                
                # Return empty arrays for closed/reopened for now
                result = {
                    "recently_graded": recently_graded,
                    "recently_closed": [],
                    "recently_reopened": [],
                }
                
                cache.set(cache_key, result, timeout=900)
                return jsonify(result)
                
    except Exception as e:
        logger.error(f"Recent actions error: {e}")
        return jsonify({"error": "Failed to fetch recent actions"}), 500


@app.route("/report-issue", methods=["POST"])
@limiter.limit("5 per hour")
def report_issue():
    """Submit an issue report for a restaurant."""
    data = request.get_json()
    
    if not data:
        return jsonify({"error": "Request body required"}), 400
    
    camis = str(data.get("camis", "")).strip()
    issue_type = str(data.get("issue_type", "")).strip()
    comments = str(data.get("comments", "")).strip()
    
    if not camis or not camis.isdigit() or len(camis) > 10:
        return jsonify({"error": "Valid CAMIS required"}), 400
    if not issue_type or len(issue_type) > 100:
        return jsonify({"error": "Valid issue type required"}), 400
    if len(comments) > 2000:
        return jsonify({"error": "Comments too long (max 2000 characters)"}), 400
    
    user_id = None
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        try:
            token = auth_header.split(" ", 1)[1]
            decoded = verify_apple_token(token)
            user_id = decoded.get("sub")
        except ValueError:
            pass
    
    email_sent = send_report_email(camis, issue_type, comments, user_id)
    logger.info(f"Issue reported - CAMIS: {camis}, Type: {issue_type}, Email sent: {email_sent}")
    
    return jsonify({"success": True, "message": "Report submitted successfully"})


@app.route("/users", methods=["POST"])
@limiter.limit("10 per hour")
def create_user():
    """Create or update a user from Apple Sign-In."""
    data = request.get_json()
    
    if not data or "identityToken" not in data:
        return jsonify({"error": "Identity token required"}), 400
    
    try:
        decoded = verify_apple_token(data["identityToken"])
        user_id = decoded.get("sub")  # This IS the user id in your schema
        
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Your users table just has id and created_at
                cur.execute("""
                    INSERT INTO users (id, created_at)
                    VALUES (%s, NOW())
                    ON CONFLICT (id) DO NOTHING
                    RETURNING id
                """, (user_id,))
                conn.commit()
        
        return jsonify({"success": True})
        
    except ValueError as e:
        return jsonify({"error": str(e)}), 401
    except Exception as e:
        logger.error(f"User creation error: {e}")
        return jsonify({"error": "Failed to create user"}), 500


@app.route("/users", methods=["DELETE"])
@require_auth
@limiter.limit("5 per day")
def delete_user():
    """Delete the authenticated user's account."""
    user_id = g.user_id
    
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # CASCADE will handle favorites and recent_searches
                cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
                conn.commit()
        
        cache.delete(get_user_cache_key("favorites", user_id))
        cache.delete(get_user_cache_key("recent_searches", user_id))
        
        return jsonify({"success": True})
        
    except Exception as e:
        logger.error(f"User deletion error: {e}")
        return jsonify({"error": "Failed to delete user"}), 500


@app.route("/favorites", methods=["GET"])
@require_auth
@limiter.limit("60 per minute")
def get_favorites():
    """Get the authenticated user's favorite restaurants."""
    user_id = g.user_id
    cache_key = get_user_cache_key("favorites", user_id)
    
    cached_result = cache.get(cache_key)
    if cached_result:
        return jsonify(cached_result)
    
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Join with DISTINCT ON to get latest restaurant info
                cur.execute("""
                    SELECT 
                        r.camis, r.dba, r.building, r.street, r.boro, r.zipcode,
                        r.phone, r.cuisine_description, r.grade, r.grade_date,
                        r.latitude, r.longitude, r.google_rating, r.google_review_count
                    FROM favorites f
                    JOIN (
                        SELECT DISTINCT ON (camis) *
                        FROM restaurants
                        ORDER BY camis, inspection_date DESC
                    ) r ON f.restaurant_camis = r.camis
                    WHERE f.user_id = %s
                    ORDER BY f.created_at DESC
                """, (user_id,))
                
                results = [
                    {
                        "camis": row[0],
                        "dba": row[1],
                        "building": row[2],
                        "street": row[3],
                        "boro": row[4],
                        "zipcode": row[5],
                        "phone": row[6],
                        "cuisine_description": row[7],
                        "grade": row[8],
                        "grade_date": row[9].isoformat() if row[9] else None,
                        "latitude": float(row[10]) if row[10] else None,
                        "longitude": float(row[11]) if row[11] else None,
                        "google_rating": float(row[12]) if row[12] else None,
                        "google_review_count": row[13],
                    }
                    for row in cur.fetchall()
                ]
                
                cache.set(cache_key, results, timeout=300)
                return jsonify(results)
                
    except Exception as e:
        logger.error(f"Favorites fetch error: {e}")
        return jsonify({"error": "Failed to fetch favorites"}), 500


@app.route("/favorites", methods=["POST"])
@require_auth
@limiter.limit("30 per minute")
def add_favorite():
    """Add a restaurant to the user's favorites."""
    user_id = g.user_id
    data = request.get_json()
    
    if not data or "camis" not in data:
        return jsonify({"error": "CAMIS required"}), 400
    
    camis = str(data["camis"]).strip()
    if not camis.isdigit() or len(camis) > 10:
        return jsonify({"error": "Invalid CAMIS format"}), 400
    
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO favorites (user_id, restaurant_camis, created_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (user_id, restaurant_camis) DO NOTHING
                """, (user_id, camis))
                conn.commit()
        
        cache.delete(get_user_cache_key("favorites", user_id))
        return jsonify({"success": True})
        
    except Exception as e:
        logger.error(f"Add favorite error: {e}")
        return jsonify({"error": "Failed to add favorite"}), 500


@app.route("/favorites/<camis>", methods=["DELETE"])
@require_auth
@limiter.limit("30 per minute")
def remove_favorite(camis):
    """Remove a restaurant from the user's favorites."""
    user_id = g.user_id
    
    if not camis.isdigit() or len(camis) > 10:
        return jsonify({"error": "Invalid CAMIS format"}), 400
    
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM favorites
                    WHERE user_id = %s AND restaurant_camis = %s
                """, (user_id, camis))
                conn.commit()
        
        cache.delete(get_user_cache_key("favorites", user_id))
        return jsonify({"success": True})
        
    except Exception as e:
        logger.error(f"Remove favorite error: {e}")
        return jsonify({"error": "Failed to remove favorite"}), 500


@app.route("/recent-searches", methods=["GET"])
@require_auth
@limiter.limit("60 per minute")
def get_recent_searches():
    """Get the authenticated user's recent searches."""
    user_id = g.user_id
    cache_key = get_user_cache_key("recent_searches", user_id)
    
    cached_result = cache.get(cache_key)
    if cached_result:
        return jsonify(cached_result)
    
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, search_term_display, created_at
                    FROM recent_searches
                    WHERE user_id = %s
                    ORDER BY created_at DESC
                    LIMIT 20
                """, (user_id,))
                
                results = [
                    {
                        "id": row[0],
                        "search_term_display": row[1],
                        "created_at": row[2].isoformat() if row[2] else None,
                    }
                    for row in cur.fetchall()
                ]
                
                cache.set(cache_key, results, timeout=300)
                return jsonify(results)
                
    except Exception as e:
        logger.error(f"Recent searches fetch error: {e}")
        return jsonify({"error": "Failed to fetch recent searches"}), 500


@app.route("/recent-searches", methods=["POST"])
@require_auth
@limiter.limit("60 per minute")
def save_recent_search():
    """Save a search term to the user's recent searches."""
    user_id = g.user_id
    data = request.get_json()
    
    if not data or "search_term" not in data:
        return jsonify({"error": "Search term required"}), 400
    
    search_term = str(data["search_term"]).strip()
    if not search_term or len(search_term) > 200:
        return jsonify({"error": "Invalid search term"}), 400
    
    # Normalize for deduplication (lowercase, extra spaces removed)
    search_term_normalized = " ".join(search_term.lower().split())
    
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Upsert - the trigger will handle trimming old searches
                cur.execute("""
                    INSERT INTO recent_searches (user_id, search_term_normalized, search_term_display, created_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (user_id, search_term_normalized) DO UPDATE SET
                        search_term_display = EXCLUDED.search_term_display,
                        created_at = NOW()
                """, (user_id, search_term_normalized, search_term))
                conn.commit()
        
        cache.delete(get_user_cache_key("recent_searches", user_id))
        return jsonify({"success": True})
        
    except Exception as e:
        logger.error(f"Save recent search error: {e}")
        return jsonify({"error": "Failed to save search"}), 500


@app.route("/recent-searches", methods=["DELETE"])
@require_auth
@limiter.limit("10 per hour")
def clear_recent_searches():
    """Clear all of the user's recent searches."""
    user_id = g.user_id
    
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM recent_searches WHERE user_id = %s", (user_id,))
                conn.commit()
        
        cache.delete(get_user_cache_key("recent_searches", user_id))
        return jsonify({"success": True})
        
    except Exception as e:
        logger.error(f"Clear recent searches error: {e}")
        return jsonify({"error": "Failed to clear searches"}), 500


# =============================================================================
# ERROR HANDLERS
# =============================================================================

@app.errorhandler(429)
def rate_limit_exceeded(e):
    return jsonify({
        "error": "Rate limit exceeded",
        "message": "Too many requests. Please try again later."
    }), 429


@app.errorhandler(500)
def internal_error(e):
    logger.error(f"Internal server error: {e}")
    return jsonify({"error": "Internal server error"}), 500


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    debug = os.getenv("ENVIRONMENT", "production") == "development"
    app.run(host="0.0.0.0", port=port, debug=debug)
