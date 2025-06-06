# app_search.py - Final Production Version v2

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
logger.info("Logging configured.")

# --- Foundational Synonym Map ---
SEARCH_TERM_SYNONYMS = {
    'pjclarkes': 'p j clarkes', 'sbarros': 's barros', 'bxdrafthouse': 'b x drafthouse',
    'foolsgrind': 'fools grind', 'mghotelenotecas': 'm g hotel enotecas', 'dandj': 'd and j',
    'bj': 'b j', 's&s': 's s', 'b&d': 'b d', 'b&c': 'b c', 'p&k': 'p k',
    'j&j': 'j j', 'j&d': 'j d', 'g&s': 'g s', 'p&j': 'p j', 'j&p': 'j p',
    'd&d': 'd d', 'p&p': 'p p', 'k&k': 'k k', 'b&a': 'b a', 'j&b': 'j b',
    'b&b': 'b b', 'j&r': 'j r', 'c&c': 'c c', 'l&b': 'l b', 'm&g': 'm g',
    'm&m': 'm m', 'j&g': 'j g', 'p&s': 'p s', 'k&d': 'k d', 'h&h': 'h h',
    'w&a': 'w a', 'a&s': 'a s', 'p&c': 'p c', 'a&w': 'a w', 'd&g': 'd g',
    'c&e': 'c e', 'a&d': 'a d', 'd&a': 'd a', 'c&k': 'c k', 'g&j': 'g j',
    'c&l': 'c l', 'a&e': 'a e', 'd&m': 'd m', 'g&g': 'g g', 'h&f': 'h f',
    'j&l': 'j l', 'j&s': 'j s', 'l&l': 'l l', 'n&w': 'n w', 'p&deli': 'p deli',
    'r&b': 'r b', 'r&d': 'r d', 'r&s': 'r s', 's&j': 's j', 's&k': 's k',
    't&b': 't b', 't&c': 't c', 'v&t': 'v t', 'y&y': 'y y', 'c&b': 'c b',
    'm&d': 'm d', 'm&t': 'm t', 's&d': 's d', 'c&n': 'c n', 'm&c': 'm c',
    'r&g': 'r g', 'r&r': 'r r', 'd-n-r': 'd n r', 'd & d': 'd d', 'k-mix': 'k mix',
    'eatalian': 'eatalian', 'e-style': 'e style', 'f-train': 'f train',
    'g-gourmet': 'g gourmet', 'g-u-s-t-o': 'g u s t o', 'h-bar': 'h bar',
    'j-mar': 'j mar', 'k-rico': 'k rico', 'k-town': 'k town', 'l-cafe': 'l cafe',
    'l-churro': 'l churro', 'l-express': 'l express', 'm-ry': 'm ry',
    'nu-look': 'nu look', 'o-bara': 'o bara', 'o-cean': 'o cean', 'o-este': 'o este',
    'o-mi': 'o mi', 'p-gu': 'p gu', 'p-j': 'p j', 'p-strami': 'p strami',
    's-k-y': 's k y', 't-bar': 't bar', 'u-like': 'u like', 'un-der': 'un der',
    'u-p': 'u p', 'u-topia': 'u topia', 'v-nam': 'v nam', 'x-press': 'x press',
    'y-not': 'y not', 'l`italiano': 'l italiano', 'l`uniko': 'l uniko',
    'm`lady': 'm lady', 'o`briens': 'o briens', 'o`caseys': 'o caseys',
    'o`connors': 'o connors', 'o`donnells': 'o donnells', 'o`haras': 'o haras',
    'o`keeffes': 'o keeffes', 'o`lunneys': 'o lunneys', 'o`neals': 'o neals',
    'o`neills': 'o neills', 'o`reillys': 'o reillys', 'o`sullivans': 'o sullivans',
    'ps450': 'p s 450', 'ps': 'p s', 'pjs': 'p j s',
    'p.j.clarke`s': 'p j clarke s', 'p.s.kitchen': 'p s kitchen', 'xian': 'xi an',
    'tcby': 't c b y', 'jimbos': 'jimbo s', 'tc': 't c', 'bk': 'b k', 'us': 'u s',
    'dj': 'd j', 'pc': 'p c', 'aj': 'a j', 'mj': 'm j', 'jp': 'j p', 'jc': 'j c',
    'jb': 'j b', 'dk': 'd k', 'pr': 'p r', 'jr': 'j r', 'cj': 'c j',
    'jg': 'j g', 'js': 'j s', 'tj': 't j', 'pj': 'p j', 'ej': 'e j',
    'gg': 'g g', 'ggs': 'g g s', 'l&j': 'l j', 's&a': 's a', 'a.j.s': 'a j s',
    'd.b.a.': 'd b a', 'd.d.': 'd d', 'd.j.s': 'd j s', 'e.j.s': 'e j s',
    'f.a.o.': 'f a o', 'g.i.': 'g i', 'h.i.m.': 'h i m', 'j-ax': 'j ax',
    'j.g.melon': 'j g melon', 'j.p.': 'j p', 'j.t.s': 'j t s', 'l.i.c.': 'l i c',
    'l.o.l.': 'l o l', 'm.o.c.': 'm o c', 'p.f.': 'p f', 'p.j.s': 'p j s',
    'p.s.': 'p s', 'r.j.s': 'r j s', 's.i.': 's i', 's.k.': 's k',
    't.j.s': 't j s', 't.l.c.': 't l c', 'u.s.': 'u s', 'y.m.c.a.': 'y m c a',
    'ymca': 'y m c a'
}

# --- Flask App Initialization ---
app = Flask(__name__)
CORS(app)
logger.info("Flask app created.")

# --- Canonical Normalization Function ---
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
    normalized_text = re.sub(r"['./-]", " ", normalized_text)
    normalized_text = re.sub(r"[^a-z0-9\s]", "", normalized_text)
    normalized_text = re.sub(r"\s+", " ", normalized_text)
    normalized_text = normalized_text.strip()
    return normalized_text

# --- ORIGINAL SANITIZE INPUT (For the old /search endpoint) ---
def original_sanitize_input(input_str):
    if not input_str: return "", ""
    input_str = input_str.replace("’", "'").replace("‘", "'")
    no_periods_version = input_str.replace(".", "")
    sanitized_input = re.sub(r"[^\w\s']", "", input_str)
    no_periods_sanitized = re.sub(r"[^\w\s']", "", no_periods_version)
    return sanitized_input, no_periods_sanitized

# --- API Routes ---
@app.route('/', methods=['GET'])
def root():
    return jsonify({"status": "ok", "message": "API is running"})

# --- ORIGINAL /search ENDPOINT ---
@app.route('/search', methods=['GET'])
def search_original():
    logger.info("Received request for ORIGINAL /search endpoint")
    name = request.args.get('name', '').strip()
    if not name:
        return jsonify({"error": "Search term is empty"}), 400
    normalized_name_for_key = name.replace("’", "'").replace("‘", "'").lower().strip()
    cache_key = f"search_orig:{normalized_name_for_key}"
    CACHE_TTL_SECONDS = 3600 * 4
    redis_conn = get_redis_client()
    if redis_conn:
        try:
            cached_result_str = redis_conn.get(cache_key)
            if cached_result_str:
                logger.info(f"Cache hit for original search: '{name}'")
                return jsonify(json.loads(cached_result_str))
        except Exception as e:
             logger.error(f"Original Search Redis GET error for key {cache_key}: {e}")
             sentry_sdk.capture_exception(e) if SentryConfig.SENTRY_DSN else None
    name_with_periods, name_without_periods = original_sanitize_input(name)
    if '.' not in name and len(name_without_periods) >= 2: name_with_added_periods = '.'.join(list(name_without_periods))
    else: name_with_added_periods = name_with_periods
    transformed_name = name_with_periods.replace("s", "'s"); transformed_name_no_periods = name_without_periods.replace("s", "'s"); transformed_with_added_periods = name_with_added_periods.replace("s", "'s")
    query = """
        SELECT r.camis, r.dba, r.boro, r.building, r.street, r.zipcode, r.phone, 
               r.latitude, r.longitude, r.inspection_date, r.critical_flag, r.grade, 
               r.inspection_type, v.violation_code, v.violation_description, r.cuisine_description 
        FROM restaurants r 
        LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date 
        WHERE r.dba ILIKE %s OR r.dba ILIKE %s OR r.dba ILIKE %s OR r.dba ILIKE %s OR r.dba ILIKE %s 
        ORDER BY 
            CASE WHEN UPPER(r.dba) = UPPER(%s) THEN 0 WHEN UPPER(r.dba) = UPPER(%s) THEN 1 
                 WHEN UPPER(r.dba) LIKE UPPER(%s) THEN 2 WHEN UPPER(r.dba) LIKE UPPER(%s) THEN 3 
                 WHEN UPPER(r.dba) LIKE UPPER(%s) THEN 4 ELSE 5 END, 
            r.dba, r.inspection_date DESC
    """
    where_params = [ f"%{p}%" for p in [name_with_periods, transformed_name, name_without_periods, transformed_name_no_periods, transformed_with_added_periods] ]
    order_params = [ name_with_periods, name_without_periods, f"{name_with_periods}%", f"{name_without_periods}%", f"{transformed_with_added_periods}%" ]
    params = where_params + order_params
    db_results_raw = None; columns = []
    try:
        with DatabaseConnection() as conn, conn.cursor() as cursor:
            cursor.execute(query, params)
            db_results_raw = cursor.fetchall()
            if db_results_raw is not None: columns = [desc[0] for desc in cursor.description]
    except Exception as e:
        logger.error(f"Original Search DB error: {e}", exc_info=True)
        sentry_sdk.capture_exception(e) if SentryConfig.SENTRY_DSN else None
        return jsonify({"error": "Database query failed"}), 500
    if not db_results_raw: return jsonify([])
    db_results = [dict(zip(columns, row)) for row in db_results_raw]
    restaurant_dict = {}
    for row_dict in db_results:
        camis = row_dict.get('camis'); inspection_date_obj = row_dict.get('inspection_date')
        if not camis: continue
        inspection_date_str = inspection_date_obj.isoformat() if hasattr(inspection_date_obj, 'isoformat') else str(inspection_date_obj)
        if camis not in restaurant_dict: restaurant_dict[camis] = {k: v for k, v in row_dict.items() if k not in ['violation_code', 'violation_description', 'inspection_date', 'critical_flag', 'grade', 'inspection_type']}; restaurant_dict[camis]['inspections'] = {}
        inspections = restaurant_dict[camis]["inspections"]
        if inspection_date_str and inspection_date_str not in inspections: inspections[inspection_date_str] = {'inspection_date': inspection_date_str, 'critical_flag': row_dict.get('critical_flag'), 'grade': row_dict.get('grade'), 'inspection_type': row_dict.get('inspection_type'), 'violations': []}
        if inspection_date_str and row_dict.get('violation_code'):
            violation = {'violation_code': row_dict.get('violation_code'), 'violation_description': row_dict.get('violation_description')}
            if violation not in inspections[inspection_date_str]["violations"]: inspections[inspection_date_str]["violations"].append(violation)
    formatted_results = [dict(data, inspections=list(data['inspections'].values())) for data in restaurant_dict.values()]
    if redis_conn:
        try: redis_conn.setex(cache_key, CACHE_TTL_SECONDS, json.dumps(formatted_results, default=str))
        except Exception as e: logger.error(f"Original Search Redis SETEX error: {e}"); sentry_sdk.capture_exception(e) if SentryConfig.SENTRY_DSN else None
    return jsonify(formatted_results)

# --- /search_fts_test ENDPOINT (Final Upgraded Version: ILIKE + Trigrams for Typos) ---
@app.route('/search_fts_test', methods=['GET'])
def search_fts_test():
    logger.info("---- /search_fts_test: Request received (ILIKE + Trigrams) ----")
    search_term_from_user = request.args.get('name', '').strip()
    
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 25))
        if page < 1: page = 1
        if per_page < 1: per_page = 25
        if per_page > 100: per_page = 100
    except ValueError:
        page = 1
        per_page = 25
    
    offset = (page - 1) * per_page
    limit = per_page

    if not search_term_from_user:
        return jsonify({"error": "Search term is empty"}), 400

    normalized_search_term = normalize_search_term_for_hybrid(search_term_from_user)

    if normalized_search_term in SEARCH_TERM_SYNONYMS:
        original_term = normalized_search_term
        normalized_search_term = SEARCH_TERM_SYNONYMS[original_term]
        logger.info(f"/search_fts_test: Applied synonym: '{original_term}' -> '{normalized_search_term}'")

    if not normalized_search_term:
        return jsonify([])

    cache_key = f"search_final_v1:{normalized_search_term}:p{page}:pp{per_page}"
    CACHE_TTL_SECONDS = 3600 * 4
    redis_conn = get_redis_client()
    if redis_conn:
        try:
            cached_result_str = redis_conn.get(cache_key)
            if cached_result_str:
                logger.info(f"/search_fts_test: Cache hit for key: {cache_key}")
                return jsonify(json.loads(cached_result_str))
            logger.info(f"/search_fts_test: Cache miss for key: {cache_key}")
        except Exception as e:
             logger.error(f"/search_fts_test: Redis GET error for key {cache_key}: {e}")
             sentry_sdk.capture_exception(e) if SentryConfig.SENTRY_DSN else None

    # Prepare patterns for the SQL query
    exact_pattern = normalized_search_term
    starts_with_pattern = f"{normalized_search_term}%"
    contains_pattern = f"%{normalized_search_term}%"
    
    # This query now includes a similarity check for typo tolerance
    query = """
    SELECT
        r.camis, r.dba, r.boro, r.building, r.street, r.zipcode, r.phone,
        r.latitude, r.longitude, v.inspection_date, v.critical_flag, v.grade,
        v.inspection_type, v.violation_code, v.violation_description, r.cuisine_description,
        similarity(r.dba_normalized_search, %s) AS similarity_score
    FROM
        restaurants r
        LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date
    WHERE
        -- Find records that either contain the substring OR are very similar (for typos)
        r.dba_normalized_search ILIKE %s
        OR similarity(r.dba_normalized_search, %s) > 0.4
    ORDER BY
        -- Rank by exact match, then starts-with, then by similarity score for typos
        CASE
            WHEN r.dba_normalized_search = %s THEN 0
            WHEN r.dba_normalized_search ILIKE %s THEN 1
            ELSE 2
        END,
        similarity(r.dba_normalized_search, %s) DESC,
        length(r.dba_normalized_search),
        r.dba ASC,
        r.inspection_date DESC
    LIMIT %s OFFSET %s;
    """
    
    # The normalized term is passed multiple times for the different parts of the query
    params = (
        exact_pattern,       # For calculating similarity_score
        contains_pattern,    # For ILIKE
        exact_pattern,       # For the main similarity() check
        exact_pattern,       # For the ORDER BY CASE exact match
        starts_with_pattern, # For the ORDER BY CASE starts-with match
        exact_pattern,       # For the ORDER BY similarity()
        limit,
        offset
    )

    db_results_raw = None
    try:
        with DatabaseConnection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(query, params)
            db_results_raw = cursor.fetchall()
    except Exception as e:
        logger.error(f"/search_fts_test: DB error for ILIKE search '{normalized_search_term}': {e}", exc_info=True)
        sentry_sdk.capture_exception(e) if SentryConfig.SENTRY_DSN else None
        return jsonify({"error": "Database query failed"}), 500

    if not db_results_raw:
        logger.info(f"/search_fts_test: No DB results for ILIKE search '{normalized_search_term}', returning empty list.")
        return jsonify([])

    # --- Standard Result Processing Logic ---
    db_results = [dict(row) for row in db_results_raw]
    restaurant_dict_hybrid = {}
    for row_dict in db_results:
        camis = row_dict.get('camis')
        if not camis: continue
        if camis not in restaurant_dict_hybrid:
            restaurant_dict_hybrid[camis] = {k: v for k, v in row_dict.items() if k not in ['violation_code', 'violation_description', 'inspection_date', 'critical_flag', 'grade', 'inspection_type', 'similarity_score']}
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
            redis_conn.setex(cache_key, CACHE_TTL_SECONDS, json.dumps(formatted_results, default=str))
            logger.info(f"/search_fts_test: Stored result in cache for key: {cache_key}")
        except Exception as e:
            logger.error(f"/search_fts_test: Redis SETEX error for key {cache_key}: {e}")
            sentry_sdk.capture_exception(e) if SentryConfig.SENTRY_DSN else None
            
    return jsonify(formatted_results)
    
    
@app.route('/recent', methods=['GET'])
def recent_restaurants():
    logger.info("Received request for /recent")
    days = request.args.get('days', '7');
    try: days = int(days); days = 7 if days <= 0 else days
    except ValueError: days = 7
    query = """ SELECT DISTINCT ON (r.camis) r.camis, r.dba, r.boro, r.building, r.street, r.zipcode, r.phone, r.latitude, r.longitude, r.grade, r.inspection_date, r.cuisine_description FROM restaurants r WHERE r.grade IN ('A', 'B', 'C') AND r.inspection_date >= (CURRENT_DATE - INTERVAL '%s days') ORDER BY r.camis, r.inspection_date DESC LIMIT 50 """
    try:
        with DatabaseConnection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(query, (days,)); results_raw = cursor.fetchall()
            results = [dict(row) for row in results_raw]
            return jsonify(results)
    except psycopg2.Error as db_err: logger.error(f"Error fetching recent restaurants: {db_err}"); sentry_sdk.capture_exception(db_err) if SentryConfig.SENTRY_DSN else None; return jsonify({"error": "DB error"}), 500
    except Exception as e: logger.error(f"Unexpected error fetching recent restaurants: {e}", exc_info=True); sentry_sdk.capture_exception(e) if SentryConfig.SENTRY_DSN else None; return jsonify({"error": "Unexpected error"}), 500

@app.route('/test-db-connection', methods=['GET'])
def test_db_connection():
    logger.info("Received request for /test-db-connection")
    try:
        with DatabaseConnection() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT 1"); result = cursor.fetchone()
            if result and result[0] == 1: return jsonify({"status": "success", "message": "Database connection successful"})
            else: return jsonify({"status": "error", "message": "DB query failed"}), 500
    except Exception as e: return jsonify({"status": "error", "message": f"DB connection error: {str(e)}"}), 500

@app.route('/trigger-update', methods=['POST'])
def trigger_update():
    logger.info("Received request for /trigger-update")
    try:
        if not update_logic_imported: return jsonify({"status": "error", "message": "Update logic unavailable."}), 500
        provided_key = request.headers.get('X-Update-Secret'); expected_key = APIConfig.UPDATE_SECRET_KEY
        if not expected_key: return jsonify({"status": "error", "message": "Update trigger not configured."}), 500
        if not provided_key or not secrets.compare_digest(provided_key, expected_key): return jsonify({"status": "error", "message": "Unauthorized."}), 403
        threading.Thread(target=run_database_update, args=(5,), daemon=True).start()
        return jsonify({"status": "success", "message": "Database update triggered in background."}), 202
    except Exception as e: logger.error(f"Err in /trigger-update: {e}", exc_info=True); sentry_sdk.capture_exception(e) if SentryConfig.SENTRY_DSN else None; return jsonify({"status": "error"}), 500

# --- Error Handlers ---
@app.errorhandler(404)
def not_found_error_handler(error):
    return jsonify({"error": "Not found"}), 404

@app.errorhandler(500)
def internal_server_error_handler(error):
    logger.error(f"500 Error: {error}", exc_info=True)
    sentry_sdk.capture_exception(error) if SentryConfig.SENTRY_DSN else None
    return jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    host = os.environ.get("HOST", getattr(APIConfig, 'HOST', "0.0.0.0"))
    port = int(os.environ.get("PORT", getattr(APIConfig, 'PORT', 8080)))
    debug_mode = getattr(APIConfig, 'DEBUG', False)
    app.run(host=host, port=port, debug=debug_mode)

logger.info("app_search.py: Module loaded completely.")
