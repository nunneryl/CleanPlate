# app_search.py - Final version with FTS search logic

import os
import re
import logging
import json
import threading
import secrets
import sys
import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration
from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import redis
from db_manager import DatabaseConnection, get_redis_client
from config import APIConfig, SentryConfig
from update_database import run_database_update

# --- Initialization & Config (condensed for brevity) ---
if SentryConfig.SENTRY_DSN:
    sentry_sdk.init(dsn=SentryConfig.SENTRY_DSN, integrations=[FlaskIntegration()], traces_sample_rate=1.0, environment="production")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)
app = Flask(__name__)
CORS(app)
logger.info("Flask app created and configured.")
# --- End Initialization ---

# --- Helper Functions ---
def format_tsquery(search_term):
    """
    Formats a user's search term into a tsquery string for prefix matching.
    Example: "two's din" -> "two's:* & din:*"
    """
    if not isinstance(search_term, str):
        return ""
    # Clean and split the search term into words
    words = search_term.lower().strip().split()
    if not words:
        return ""
    # Append ':*' to each word for prefix matching and join with '&'
    # This tells Postgres to find docs with words STARTING WITH these terms
    tsquery_parts = [re.sub(r'[\'"]', '', word) + ':*' for word in words]
    return ' & '.join(tsquery_parts)
# --- End Helper Functions ---

# --- API Routes ---
@app.route('/', methods=['GET'])
def root():
    return jsonify({"status": "ok"})

@app.route('/search', methods=['GET'])
def search():
    search_term = request.args.get('name', '').strip()
    if not search_term:
        return jsonify({"error": "Search term is empty"}), 400

    # Format the search term into a prefix-matching tsquery
    tsquery_string = format_tsquery(search_term)
    if not tsquery_string:
        return jsonify([])

    cache_key = f"search_v3:{tsquery_string}" # v3 for FTS version
    CACHE_TTL_SECONDS = 3600 * 4

    redis_conn = get_redis_client()
    if redis_conn:
        try:
            cached_result = redis_conn.get(cache_key)
            if cached_result:
                logger.info(f"Cache hit for tsquery: '{tsquery_string}'")
                return jsonify(json.loads(cached_result))
        except redis.exceptions.RedisError as e:
            logger.error(f"Redis GET error: {e}"); sentry_sdk.capture_exception(e)

    # --- Database Query Logic (FTS) ---
    logger.info(f"DB query for tsquery: '{tsquery_string}'")
    query = """
        SELECT
            r.camis, r.dba, r.boro, r.building, r.street, r.zipcode, r.phone,
            r.latitude, r.longitude, r.inspection_date, r.critical_flag, r.grade,
            r.inspection_type, v.violation_code, v.violation_description, r.cuisine_description,
            -- Add a rank column to sort by relevance
            ts_rank_cd(r.dba_tsv, to_tsquery('english', %s)) AS rank
        FROM restaurants r
        LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date
        WHERE r.dba_tsv @@ to_tsquery('english', %s)
        ORDER BY rank DESC, r.dba, r.inspection_date DESC
        LIMIT 100; -- Add a limit to prevent excessively large responses
    """
    params = (tsquery_string, tsquery_string)

    try:
        with DatabaseConnection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(query, params)
            db_results = [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"FTS DB error for tsquery '{tsquery_string}': {e}", exc_info=True)
        sentry_sdk.capture_exception(e)
        return jsonify({"error": "Database query failed"}), 500

    if not db_results:
        logger.info(f"No FTS results for tsquery: '{tsquery_string}'")
        if redis_conn:
            try: redis_conn.setex(cache_key, 60 * 15, json.dumps([]))
            except redis.exceptions.RedisError as e: logger.error(f"Redis SETEX error for empty result: {e}")
        return jsonify([])

    # --- Result Processing ---
    restaurant_dict = {}
    for row in db_results:
        camis = row.get('camis')
        if not camis: continue
        if camis not in restaurant_dict:
            restaurant_dict[camis] = {k: v for k, v in row.items() if k not in ['rank', 'violation_code', 'violation_description', 'inspection_date', 'critical_flag', 'grade', 'inspection_type']}
            restaurant_dict[camis]['inspections'] = {}
        
        inspection_date_obj = row.get('inspection_date')
        if inspection_date_obj:
            inspection_date_str = inspection_date_obj.isoformat()
            if inspection_date_str not in restaurant_dict[camis]['inspections']:
                restaurant_dict[camis]['inspections'][inspection_date_str] = {
                    'inspection_date': inspection_date_str,
                    'critical_flag': row.get('critical_flag'),
                    'grade': row.get('grade'),
                    'inspection_type': row.get('inspection_type'),
                    'violations': []
                }
            if row.get('violation_code'):
                violation = {'violation_code': row.get('violation_code'), 'violation_description': row.get('violation_description')}
                if violation not in restaurant_dict[camis]['inspections'][inspection_date_str]['violations']:
                    restaurant_dict[camis]['inspections'][inspection_date_str]['violations'].append(violation)

    formatted_results = []
    for restaurant in restaurant_dict.values():
        restaurant['inspections'] = list(restaurant['inspections'].values())
        formatted_results.append(restaurant)
    # --- End Result Processing ---

    if redis_conn:
        try:
            redis_conn.setex(cache_key, CACHE_TTL_SECONDS, json.dumps(formatted_results, default=str)) # Use default=str for dates
        except redis.exceptions.RedisError as e:
            logger.error(f"Redis SETEX error: {e}")

    logger.info(f"FTS search OK, returning {len(formatted_results)} restaurants.")
    return jsonify(formatted_results)

# --- Other Routes (condensed) ---
@app.route('/trigger-update', methods=['POST'])
def trigger_update():
    provided_key = request.headers.get('X-Update-Secret')
    if not secrets.compare_digest(provided_key or '', APIConfig.UPDATE_SECRET_KEY or ''):
        return jsonify({"status": "error", "message": "Unauthorized."}), 403
    threading.Thread(target=run_database_update, daemon=True).start()
    return jsonify({"status": "success", "message": "Database update triggered."}), 202

@app.errorhandler(500)
def server_error(e):
    logger.error(f"500 Internal Server Error: {e}", exc_info=True)
    return jsonify({"error": "An internal server error occurred"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=False)
