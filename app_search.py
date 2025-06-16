# app_search.py - LIVE Production Version with Final Normalization Fix

import os
import re
import logging
import json
import threading
import secrets
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
    update_logic_imported = False
    def run_database_update(*args, **kwargs): pass

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)
app = Flask(__name__)
CORS(app)

# --- DEFINITIVE NORMALIZATION FUNCTION ---
def normalize_search_term_for_hybrid(text):
    """
    Cleans a search term for robust matching.
    - Lowercases text.
    - Removes accents.
    - Removes apostrophes.
    - Replaces certain punctuation with spaces.
    - **Removes all spaces** to make terms like "pjs" and "p j s" identical.
    """
    if not isinstance(text, str): return ''
    normalized_text = text.lower().replace('&', ' and ')
    
    accent_map = { 'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e', 'á': 'a', 'à': 'a', 'â': 'a', 'ä': 'a', 'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i', 'ó': 'o', 'ò': 'o', 'ô': 'o', 'ö': 'o', 'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u', 'ç': 'c', 'ñ': 'n' }
    for accented, unaccented in accent_map.items():
        normalized_text = normalized_text.replace(accented, unaccented)
    
    normalized_text = re.sub(r"[']", "", normalized_text)
    normalized_text = re.sub(r"[./-]", " ", normalized_text)
    normalized_text = re.sub(r"[^a-z0-9\s]", "", normalized_text)
    
    # THE FIX: Remove all whitespace. "p j clarkes" and "pjs" both become "pjclarkes".
    normalized_text = re.sub(r"\s+", "", normalized_text)
    
    return normalized_text.strip()


# --- Live Search Endpoint ---
@app.route('/search', methods=['GET'])
def search():
    search_term = request.args.get('name', '').strip()
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 25))

    if not search_term:
        return jsonify([])

    # The new normalization is applied here
    normalized_search = normalize_search_term_for_hybrid(search_term)
    
    if not normalized_search:
        return jsonify([])

    # The query is now much simpler as it doesn't need complex relevance ranking
    query = """
    WITH latest_restaurants AS (
        SELECT DISTINCT ON (camis) * FROM restaurants ORDER BY camis, inspection_date DESC
    ), paginated_camis AS (
        SELECT camis, dba, dba_normalized_search FROM latest_restaurants
        WHERE dba_normalized_search LIKE %s
        ORDER BY dba ASC
        LIMIT %s OFFSET %s
    )
    SELECT pc.camis, pc.dba, pc.dba_normalized_search, r.boro, r.building, r.street, r.zipcode, r.phone, r.latitude, r.longitude,
           r.inspection_date, r.critical_flag, r.grade, r.inspection_type,
           v.violation_code, v.violation_description, r.cuisine_description
    FROM paginated_camis pc JOIN restaurants r ON pc.camis = r.camis
    LEFT JOIN violations v ON pc.camis = v.camis AND r.inspection_date = v.inspection_date
    ORDER BY pc.dba ASC, r.inspection_date DESC;
    """
    
    contains_pattern = f"%{normalized_search}%"
    offset = (page - 1) * per_page
    params = (contains_pattern, per_page, offset)

    try:
        with DatabaseConnection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(query, params)
            results = cursor.fetchall()
    except Exception as e:
        logger.error(f"DB search failed for '{search_term}': {e}", exc_info=True)
        return jsonify({"error": "Database query failed"}), 500

    if not results: return jsonify([])

    # Formatting the response remains the same
    restaurant_dict = {}
    for row in results:
        camis = str(row['camis'])
        if camis not in restaurant_dict:
            restaurant_dict[camis] = {k: v for k, v in row.items() if k not in ['violation_code', 'violation_description', 'inspection_date', 'critical_flag', 'grade', 'inspection_type']}
            restaurant_dict[camis]['inspections'] = {}
        insp_date_str = row['inspection_date'].isoformat()
        if insp_date_str not in restaurant_dict[camis]['inspections']:
            restaurant_dict[camis]['inspections'][insp_date_str] = {'inspection_date': insp_date_str, 'grade': row['grade'], 'critical_flag': row['critical_flag'], 'inspection_type': row['inspection_type'], 'violations': []}
        if row['violation_code']:
            v_data = {'violation_code': row['violation_code'], 'violation_description': row['violation_description']}
            if v_data not in restaurant_dict[camis]['inspections'][insp_date_str]['violations']:
                restaurant_dict[camis]['inspections'][insp_date_str]['violations'].append(v_data)

    final_results = [{**data, 'inspections': sorted(list(data['inspections'].values()), key=lambda x: x['inspection_date'], reverse=True)} for data in restaurant_dict.values()]
    return jsonify(final_results)

# --- Other Endpoints ---
@app.route('/recent', methods=['GET'])
def recent_restaurants(): return jsonify([])
@app.route('/trigger-update', methods=['POST'])
def trigger_update():
    if not update_logic_imported: return jsonify({"status": "error", "message": "Update logic unavailable."}), 500
    provided_key = request.headers.get('X-Update-Secret')
    expected_key = APIConfig.UPDATE_SECRET_KEY
    if not expected_key or not provided_key or not secrets.compare_digest(provided_key, expected_key): return jsonify({"status": "error", "message": "Unauthorized."}), 403
    threading.Thread(target=run_database_update, args=(15,), daemon=True).start()
    return jsonify({"status": "success", "message": "Database update triggered in background."}), 202
@app.errorhandler(404)
def not_found_error_handler(error): return jsonify({"error": "Endpoint not found"}), 404
@app.errorhandler(500)
def internal_server_error_handler(error):
    logger.error(f"Internal Server Error (500): {error}", exc_info=True)
    return jsonify({"error": "An internal server error occurred"}), 500

if __name__ == "__main__":
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", 8080))
    app.run(host=host, port=port)
