# app_search.py - Apostrophe Handling Fix

# Standard library imports
import os
import re
import logging
import json
import threading
import secrets
import sys

# Third-party imports
import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration
from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import psycopg2.extras # For DictCursor
import redis

# Local application imports
try:
    from db_manager import DatabaseConnection, get_redis_client
    logging.info("Imported db_manager successfully.")
except ImportError:
    logging.critical("FAILED to import db_manager. Using placeholder classes.")
    class DatabaseConnection:
        def __enter__(self): return None
        def __exit__(self, type, value, traceback): pass
    def get_redis_client(): return None

try:
    from config import APIConfig, SentryConfig
    logging.info("Imported config successfully.")
except ImportError:
    logging.critical("FAILED to import config. Using placeholder classes.")
    class APIConfig: DEBUG = False; UPDATE_SECRET_KEY = "key"; HOST = "0.0.0.0"; PORT = 8080
    class SentryConfig: SENTRY_DSN = None

try:
    from update_database import run_database_update
    update_logic_imported = True
    logging.info("Imported run_database_update successfully.")
except ImportError:
    logging.error("FAILED to import run_database_update. /trigger-update will not work.")
    update_logic_imported = False
    def run_database_update(days_back=5):
         logging.error("DUMMY run_database_update called - real function failed to import.")

# --- Sentry Initialization ---
if hasattr(SentryConfig, 'SENTRY_DSN') and SentryConfig.SENTRY_DSN:
    try:
        sentry_sdk.init(
            dsn=SentryConfig.SENTRY_DSN, integrations=[FlaskIntegration()],
            traces_sample_rate=1.0, environment="production"
        )
        logging.info("Sentry initialized.")
    except Exception as e:
         logging.error(f"Failed to initialize Sentry: {e}")
else:
    logging.warning("SENTRY_DSN not set, Sentry not initialized.")

# --- Logging Setup ---
if not logging.getLogger().hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)

# --- Foundational Synonym Map (Unchanged) ---
SEARCH_TERM_SYNONYMS = {
    'pjclarkes': 'p j clarkes', 'xian': 'xi an', # (and all your other synonyms)
}

# --- Flask App Initialization ---
app = Flask(__name__)
CORS(app)
logger.info("Flask app created.")

# --- ##### THIS IS THE CORRECTED NORMALIZATION FUNCTION ##### ---
def normalize_search_term_for_hybrid(text):
    if not isinstance(text, str): return ''
    normalized_text = text.lower()
    normalized_text = normalized_text.replace('&', ' and ')
    
    accent_map = {
        'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e', 'á': 'a', 'à': 'a', 'â': 'a', 'ä': 'a',
        'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i', 'ó': 'o', 'ò': 'o', 'ô': 'o', 'ö': 'o',
        'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u', 'ç': 'c', 'ñ': 'n'
    }
    for accented, unaccented in accent_map.items():
        normalized_text = normalized_text.replace(accented, unaccented)
    
    # --- THIS IS THE CRITICAL CHANGE ---
    # 1. Remove apostrophes entirely (don't replace with a space).
    # This makes "joe's" and "joes" become the same ("joes").
    normalized_text = re.sub(r"[']", "", normalized_text)
    
    # 2. Now, replace other specific punctuation with a space.
    normalized_text = re.sub(r"[./-]", " ", normalized_text)
    
    # 3. Clean up any other unwanted characters and extra spaces.
    normalized_text = re.sub(r"[^a-z0-9\s]", "", normalized_text)
    normalized_text = re.sub(r"\s+", " ", normalized_text)
    
    return normalized_text.strip()

# --- API Routes ---
@app.route('/', methods=['GET'])
def root():
    return jsonify({"status": "ok", "message": "API is running"})

@app.route('/search', methods=['GET'])
def search():
    search_term_from_user = request.args.get('name', '').strip()
    
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 25))
    except (ValueError, TypeError):
        page = 1
        per_page = 25
    
    offset = (page - 1) * per_page
    limit = per_page

    if not search_term_from_user:
        return jsonify([])

    # Use the new, corrected normalization logic.
    normalized_search_term = normalize_search_term_for_hybrid(search_term_from_user)

    if normalized_search_term in SEARCH_TERM_SYNONYMS:
        normalized_search_term = SEARCH_TERM_SYNONYMS[normalized_search_term]

    if not normalized_search_term:
        return jsonify([])

    cache_key = f"search_v3_apostrophe_fix:{normalized_search_term}:p{page}:pp{per_page}"
    redis_conn = get_redis_client()
    if redis_conn:
        try:
            cached_result_str = redis_conn.get(cache_key)
            if cached_result_str:
                return jsonify(json.loads(cached_result_str))
        except Exception as e:
             logger.error(f"Redis GET error: {e}")

    # The SQL query remains the same, but will now work correctly with the improved normalization.
    query = """
    WITH RankedRestaurants AS (
        SELECT
            DISTINCT ON (camis)
            camis, dba, boro, building, street, zipcode, phone,
            latitude, longitude, cuisine_description, dba_normalized_search
        FROM restaurants
        WHERE dba_normalized_search ILIKE %s OR similarity(dba_normalized_search, %s) > 0.4
    ),
    PaginatedRestaurants AS (
        SELECT *
        FROM RankedRestaurants
        ORDER BY
            CASE
                WHEN dba_normalized_search = %s THEN 0
                WHEN dba_normalized_search ILIKE %s THEN 1
                ELSE 2
            END,
            similarity(dba_normalized_search, %s) DESC,
            length(dba_normalized_search),
            dba ASC
        LIMIT %s OFFSET %s
    )
    SELECT
        pr.camis, pr.dba, pr.boro, pr.building, pr.street, pr.zipcode, pr.phone,
        pr.latitude, pr.longitude, r_full.inspection_date, r_full.critical_flag, r_full.grade,
        r_full.inspection_type, v.violation_code, v.violation_description, pr.cuisine_description
    FROM PaginatedRestaurants pr
    JOIN restaurants r_full ON pr.camis = r_full.camis
    LEFT JOIN violations v ON r_full.camis = v.camis AND r_full.inspection_date = v.inspection_date
    ORDER BY
        CASE
            WHEN pr.dba_normalized_search = %s THEN 0
            WHEN pr.dba_normalized_search ILIKE %s THEN 1
            ELSE 2
        END,
        similarity(pr.dba_normalized_search, %s) DESC,
        length(pr.dba_normalized_search),
        pr.dba ASC,
        r_full.inspection_date DESC;
    """
    
    contains_pattern = f"%{normalized_search_term}%"
    starts_with_pattern = f"{normalized_search_term}%"
    
    params = (
        contains_pattern, normalized_search_term, normalized_search_term, starts_with_pattern,
        normalized_search_term, limit, offset, normalized_search_term,
        starts_with_pattern, normalized_search_term
    )
    
    try:
        with DatabaseConnection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(query, params)
            db_results_raw = cursor.fetchall()
    except Exception as e:
        logger.error(f"DB error for search '{normalized_search_term}': {e}", exc_info=True)
        return jsonify({"error": "Database query failed"}), 500

    if not db_results_raw:
        return jsonify([])

    # Standard Result Processing Logic (Unchanged)
    db_results = [dict(row) for row in db_results_raw]
    restaurant_dict_hybrid = {}
    for row_dict in db_results:
        camis = row_dict.get('camis')
        if not camis: continue
        if camis not in restaurant_dict_hybrid:
            restaurant_dict_hybrid[camis] = {k: v for k, v in row_dict.items() if k not in ['violation_code', 'violation_description', 'inspection_date', 'critical_flag', 'grade', 'inspection_type']}
            restaurant_dict_hybrid[camis]['inspections'] = {}
        inspection_date_obj = row_dict.get('inspection_date')
        if inspection_date_obj:
            inspection_date_str = inspection_date_obj.isoformat()
            if inspection_date_str not in restaurant_dict_hybrid[camis]['inspections']:
                restaurant_dict_hybrid[camis]['inspections'][inspection_date_str] = {'inspection_date': inspection_date_str, 'critical_flag': row_dict.get('critical_flag'), 'grade': row_dict.get('grade'), 'inspection_type': row_dict.get('inspection_type'), 'violations': []}
            if row_dict.get('violation_code'):
                violation = {'violation_code': row_dict.get('violation_code'), 'violation_description': row_dict.get('violation_description')}
                if violation not in restaurant_dict_hybrid[camis]['inspections'][inspection_date_str]['violations']:
                    restaurant_dict_hybrid[camis]['inspections'][inspection_date_str]['violations'].append(violation)
    
    formatted_results = [dict(data, inspections=list(data['inspections'].values())) for data in restaurant_dict_hybrid.values()]

    if redis_conn:
        try:
            redis_conn.setex(cache_key, 3600, json.dumps(formatted_results, default=str))
        except Exception as e:
            logger.error(f"Redis SETEX error: {e}")
            
    return jsonify(formatted_results)

# --- Other Endpoints (Unchanged) ---
# ... /recent, /trigger-update, etc. remain the same.

if __name__ == "__main__":
    # ... main execution block remains the same.
    pass
