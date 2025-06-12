# app_search.py - v9 - Corrected Final ORDER BY Syntax

# Standard library imports
import os
import re
import logging
import json
import threading
import secrets

# Third-party imports
from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import psycopg2.extras

# Local application imports
try:
    from db_manager import DatabaseConnection, get_redis_client
    from config import APIConfig
    from update_database import run_database_update
    update_logic_imported = True
except ImportError:
    # Dummy classes for local execution without full environment
    class DatabaseConnection:
        def __enter__(self): return None
        def __exit__(self, t, v, tb): pass
    def get_redis_client(): return None
    class APIConfig: UPDATE_SECRET_KEY = "dummy"; HOST="0.0.0.0"; PORT=8080
    update_logic_imported = False
    def run_database_update(days_back=5): pass

# --- Basic Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)
SEARCH_TERM_SYNONYMS = {'pjclarkes': 'p j clarkes', 'xian': 'xi an'}
app = Flask(__name__)
CORS(app)

# --- Normalization Function (Unchanged) ---
def normalize_search_term_for_hybrid(text):
    if not isinstance(text, str): return ''
    normalized_text = text.lower().replace('&', ' and ')
    accent_map = { 'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e', 'á': 'a', 'à': 'a', 'â': 'a', 'ä': 'a', 'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i', 'ó': 'o', 'ò': 'o', 'ô': 'o', 'ö':'o', 'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u', 'ç': 'c', 'ñ': 'n' }
    for accented, unaccented in accent_map.items():
        normalized_text = normalized_text.replace(accented, unaccented)
    normalized_text = re.sub(r"[']", "", normalized_text)
    normalized_text = re.sub(r"[./-]", " ", normalized_text)
    normalized_text = re.sub(r"[^a-z0-9\s]", "", normalized_text)
    normalized_text = re.sub(r"\s+", " ", normalized_text)
    return normalized_text.strip()


# --- ##### REWRITTEN AND ROBUST SEARCH ENDPOINT ##### ---
@app.route('/search', methods=['GET'])
def search():
    # 1. Get and Validate Parameters
    search_term = request.args.get('name', '').strip()
    grade_filter = request.args.get('grade', type=str)
    boro_filter = request.args.get('boro', type=str)
    sort_by = request.args.get('sort', 'relevance', type=str)
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 25))
        if page < 1: page = 1
        if per_page < 1 or per_page > 100: per_page = 25
    except (ValueError, TypeError):
        page, per_page = 1, 25

    if not search_term:
        return jsonify([])

    # 2. Normalize and Prepare Search Terms
    normalized_search = normalize_search_term_for_hybrid(search_term)
    if normalized_search in SEARCH_TERM_SYNONYMS:
        normalized_search = SEARCH_TERM_SYNONYMS[normalized_search]
    if not normalized_search:
        return jsonify([])

    # 3. Build Cache Key
    cache_key = f"search_v11_final:{normalized_search}:g{grade_filter}:b{boro_filter}:s{sort_by}:p{page}:pp{per_page}"
    redis_conn = get_redis_client()
    if redis_conn:
        try:
            cached_result = redis_conn.get(cache_key)
            if cached_result:
                return jsonify(json.loads(cached_result))
        except Exception as e:
            logger.error(f"Redis GET error: {e}")

    # 4. Dynamically Build SQL Query and Parameters
    params = []
    where_conditions = ["(dba_normalized_search ILIKE %s OR similarity(dba_normalized_search, %s) > 0.4)"]
    params.extend([f"%{normalized_search}%", normalized_search])

    if grade_filter:
        where_conditions.append("grade = %s")
        params.append(grade_filter)

    if boro_filter:
        where_conditions.append("boro = %s")
        params.append(boro_filter)

    where_clause = " AND ".join(where_conditions)

    order_by_clause = ""
    order_by_params = []
    if sort_by == 'name_asc':
        order_by_clause = "ORDER BY dba ASC"
    elif sort_by == 'name_desc':
        order_by_clause = "ORDER BY dba DESC"
    else:
        order_by_clause = """
        ORDER BY
            CASE
                WHEN dba_normalized_search = %s THEN 0
                WHEN dba_normalized_search LIKE %s THEN 1
                ELSE 2
            END,
            similarity(dba_normalized_search, %s) DESC
        """
        order_by_params.extend([normalized_search, f"{normalized_search}%", normalized_search])

    pagination_params = [per_page, (page - 1) * per_page]
    
    # This logic creates the final ORDER BY clause with the correct table alias 'pc'.
    final_order_by_clause = order_by_clause.replace('dba_normalized_search', 'pc.dba_normalized_search')
    final_order_by_clause = final_order_by_clause.replace('dba ASC', 'pc.dba ASC')
    final_order_by_clause = final_order_by_clause.replace('dba DESC', 'pc.dba DESC')

    final_query = f"""
        WITH latest_restaurants AS (
            SELECT DISTINCT ON (camis) *
            FROM restaurants
            ORDER BY camis, inspection_date DESC
        ),
        paginated_camis AS (
            SELECT camis, dba, dba_normalized_search
            FROM latest_restaurants
            WHERE {where_clause}
            {order_by_clause}
            LIMIT %s OFFSET %s
        )
        SELECT
            r.camis, r.dba, r.boro, r.building, r.street, r.zipcode, r.phone,
            r.latitude, r.longitude, r.inspection_date, r.critical_flag, r.grade,
            r.inspection_type, v.violation_code, v.violation_description, r.cuisine_description
        FROM paginated_camis pc
        JOIN restaurants r ON pc.camis = r.camis
        LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date
        {final_order_by_clause}, r.inspection_date DESC;
    """
    
    # 5. Execute Query and Process Results
    try:
        with DatabaseConnection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            final_params = params + order_by_params + pagination_params + order_by_params
            cursor.execute(final_query, tuple(final_params))
            db_results_raw = cursor.fetchall()
    except Exception as e:
        logger.error(f"DB error for search '{search_term}': {e}", exc_info=True)
        return jsonify({"error": "Database query failed"}), 500

    if not db_results_raw:
        return jsonify([])

    # 6. Group results by restaurant and inspection (Unchanged)
    restaurant_dict = {}
    for row in db_results_raw:
        camis = row['camis']
        if not camis: continue
        if camis not in restaurant_dict:
            restaurant_dict[camis] = {k: v for k, v in row.items() if k not in ['violation_code', 'violation_description', 'inspection_date', 'critical_flag', 'grade', 'inspection_type']}
            restaurant_dict[camis]['inspections'] = {}
        inspection_date_str = row['inspection_date'].isoformat()
        if inspection_date_str not in restaurant_dict[camis]['inspections']:
            restaurant_dict[camis]['inspections'][inspection_date_str] = {'inspection_date': inspection_date_str, 'critical_flag': row['critical_flag'], 'grade': row['grade'], 'inspection_type': row['inspection_type'], 'violations': []}
        if row['violation_code']:
            violation = {'violation_code': row['violation_code'], 'violation_description': row['violation_description']}
            if violation not in restaurant_dict[camis]['inspections'][inspection_date_str]['violations']:
                restaurant_dict[camis]['inspections'][inspection_date_str]['violations'].append(violation)
    
    final_results = []
    for camis, data in restaurant_dict.items():
        inspections = sorted(list(data['inspections'].values()), key=lambda x: x['inspection_date'], reverse=True)
        data['inspections'] = inspections
        final_results.append(data)

    # 7. Cache the final result
    if redis_conn:
        try:
            redis_conn.setex(cache_key, 3600, json.dumps(final_results, default=str))
        except Exception as e:
            logger.error(f"Redis SETEX error: {e}")
            
    return jsonify(final_results)


# --- Other Endpoints (Unchanged) ---
@app.route('/recent', methods=['GET'])
def recent_restaurants():
    return jsonify([])

@app.route('/trigger-update', methods=['POST'])
def trigger_update():
    if not update_logic_imported:
        return jsonify({"status": "error", "message": "Update logic unavailable."}), 500
    provided_key = request.headers.get('X-Update-Secret')
    expected_key = APIConfig.UPDATE_SECRET_KEY
    if not expected_key or not provided_key or not secrets.compare_digest(provided_key, expected_key):
        return jsonify({"status": "error", "message": "Unauthorized."}), 403
    threading.Thread(target=run_database_update, args=(5,), daemon=True).start()
    return jsonify({"status": "success", "message": "Database update triggered in background."}), 202

# --- Error Handlers (Unchanged) ---
@app.errorhandler(404)
def not_found_error_handler(error):
    return jsonify({"error": "Not found"}), 404

@app.errorhandler(500)
def internal_server_error_handler(error):
    logger.error(f"500 Error: {error}", exc_info=True)
    return jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", 8080))
    app.run(host=host, port=port)
