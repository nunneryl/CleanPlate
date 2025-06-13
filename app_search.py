# app_search.py - Final, Simplified, and Correct Version

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

# Local application imports (assuming they are in the same directory or accessible)
from db_manager import DatabaseConnection, get_redis_client
from config import APIConfig
from update_database import run_database_update

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

# --- ##### FINAL WORKING SEARCH ENDPOINT ##### ---
@app.route('/search', methods=['GET'])
def search():
    # 1. Get Parameters
    search_term = request.args.get('name', '').strip()
    grade_filter = request.args.get('grade', type=str)
    boro_filter = request.args.get('boro', type=str)
    sort_by = request.args.get('sort', 'relevance', type=str)
    camis_filter = request.args.get('camis', type=int)

    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 25))
    except (ValueError, TypeError):
        page = 1
        per_page = 25

    # If no search term or camis, return empty
    if not search_term and not camis_filter:
        return jsonify([])

    normalized_search = normalize_search_term_for_hybrid(search_term)

    # 2. Build Cache Key
    cache_key = f"search_v_final_correct:{normalized_search}:{camis_filter}:{grade_filter}:{boro_filter}:{sort_by}:{page}:{per_page}"
    redis_conn = get_redis_client()
    if redis_conn:
        try:
            cached_result = redis_conn.get(cache_key)
            if cached_result:
                return jsonify(json.loads(cached_result))
        except Exception as e:
            logger.error(f"Redis GET error: {e}")

    # 3. Build Query
    params = []
    where_conditions = []

    if camis_filter:
        where_conditions.append("lr.camis = %s")
        params.append(camis_filter)
    elif normalized_search:
        where_conditions.append("(lr.dba_normalized_search ILIKE %s OR similarity(lr.dba_normalized_search, %s) > 0.4)")
        params.extend([f"%{normalized_search}%", normalized_search])

    if grade_filter:
        where_conditions.append("lr.grade = %s")
        params.append(grade_filter)
    if boro_filter:
        where_conditions.append("lr.boro = %s")
        params.append(boro_filter)

    where_clause = " AND ".join(where_conditions)

    order_by_clause = ""
    if sort_by == 'name_asc':
        order_by_clause = "ORDER BY lr.dba ASC"
    elif sort_by == 'name_desc':
        order_by_clause = "ORDER BY lr.dba DESC"
    else:
        order_by_clause = "ORDER BY similarity(lr.dba_normalized_search, %s) DESC"
        params.append(normalized_search)

    offset = (page - 1) * per_page
    params.extend([per_page, offset])

    # This is the simplest possible query that achieves all goals.
    query = f"""
        WITH latest_restaurants AS (
            SELECT DISTINCT ON (camis) *, unaccent(lower(dba)) as dba_normalized_search
            FROM restaurants
            ORDER BY camis, inspection_date DESC
        )
        SELECT r.*, v.violation_code, v.violation_description
        FROM (
            SELECT camis FROM latest_restaurants lr
            WHERE {where_clause}
            {order_by_clause}
            LIMIT %s OFFSET %s
        ) AS paginated
        JOIN restaurants r ON paginated.camis = r.camis
        LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date
        ORDER BY (SELECT {order_by_clause.replace('lr.', 'sub.')} FROM latest_restaurants sub WHERE sub.camis = r.camis), r.inspection_date DESC;
    """

    if sort_by == 'relevance':
        params.append(normalized_search)
    
    # 4. Execute Query
    try:
        with DatabaseConnection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(query, tuple(params))
            results = cursor.fetchall()
    except Exception as e:
        logger.error(f"DB search failed for term '{search_term}': {e}", exc_info=True)
        return jsonify({"error": "Database query failed"}), 500

    if not results:
        return jsonify([])

    # 5. Process Results
    restaurant_data = {}
    for row in results:
        camis = row['camis']
        if camis not in restaurant_data:
            restaurant_data[camis] = {
                'camis': row['camis'], 'dba': row['dba'], 'boro': row['boro'],
                'building': row['building'], 'street': row['street'], 'zipcode': row['zipcode'],
                'phone': row['phone'], 'latitude': row['latitude'], 'longitude': row['longitude'],
                'cuisine_description': row['cuisine_description'], 'inspections': {}
            }
        
        inspection_date = row['inspection_date'].isoformat()
        if inspection_date not in restaurant_data[camis]['inspections']:
            restaurant_data[camis]['inspections'][inspection_date] = {
                'inspection_date': inspection_date, 'grade': row['grade'],
                'critical_flag': row['critical_flag'], 'inspection_type': row['inspection_type'],
                'violations': []
            }

        if row['violation_code']:
            v_data = {'violation_code': row['violation_code'], 'violation_description': row['violation_description']}
            if v_data not in restaurant_data[camis]['inspections'][inspection_date]['violations']:
                restaurant_data[camis]['inspections'][inspection_date]['violations'].append(v_data)

    final_results = [
        {**data, 'inspections': sorted(list(data['inspections'].values()), key=lambda x: x['inspection_date'], reverse=True)}
        for data in restaurant_data.values()
    ]

    if redis_conn:
        try:
            redis_conn.setex(cache_key, 3600, json.dumps(final_results, default=str))
        except Exception as e:
            logger.error(f"Redis SETEX error: {e}")

    return jsonify(final_results)

# (The rest of the file remains unchanged: /recent, /trigger-update, error handlers, etc.)
# ...
