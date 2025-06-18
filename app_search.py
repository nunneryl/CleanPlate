import os
import re
import logging
import json
from flask import Flask, jsonify, request, g
from flask_cors import CORS
import psycopg2
import psycopg2.extras

try:
    from db_manager import DatabaseConnection
except ImportError:
    # This allows the app to run even if db_manager is not found,
    # though database operations will fail.
    DatabaseConnection = None

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# --- FULL & COMPLETE SYNONYM MAP ---
SEARCH_TERM_SYNONYMS = {
    'allar': 'all ar', 'allantico': 'all antico', 'amore': 'a more', 'annam': 'an nam',
    'annestccd': 'anne stccd', 'apizza': 'a pizza', 'apou': 'a pou', 'arcteryx': 'arc teryx',
    'baal': 'ba al', 'baires': 'b aires', 'baluk': 'baluk', 'barb': 'bar b',
    'bartusi': 'b artusi', 'bklyn': 'b klyn', 'bobsang': 'bob sang', 'bouote': 'bou ote',
    'boxralph': 'box ralph', 'cafeflor': 'cafe flor', 'cafelounge': 'cafe lounge', 'capt': 'cap t',
    'centanni': 'cent anni', 'centerseoul': 'center seoul', 'chickn': 'chick n', 'chilld': 'chill d',
    'chopt': 'chop t', 'cmon': 'c mon', 'cocacola': 'coca cola', 'cookup': 'cook up',
    'dalsace': 'd alsace', 'damico': 'd amico', 'damigos': 'd amigos', 'damilio': 'da milio',
    'damore': 'd amore', 'damour': 'd amour', 'dangelo': 'd angelo', 'danna': 'd anna',
    'dantan': 'd antan', 'dantigua': 'd antigua', 'danvers': 'd anvers', 'darturo': 'd arturo',
    'davignon': 'd avignon', 'dbichote': 'd bichote', 'dblass': 'd blass', 'delisa': 'd elisa',
    'dellanima': 'dell anima', 'dellaria': 'dell aria', 'dellarte': 'dell arte', 'dhote': 'd hote',
    'dippindots': 'dippin dots', 'dlenny': 'd lenny', 'dlili': 'd lili', 'dlioz': 'd lioz',
    'dmaritza': 'd maritza', 'dmelting': 'd melting', 'dmorena': 'd morena', 'dont': 'don t',
    'dor': 'd or', 'doro': 'doro', 'dpikete': 'd pikete', 'eatme': 'eat me',
    'eatrite': 'eat rite', 'eightyseven': 'eighty seven', 'esca': 'es ca', 'geez': 'gee z',
    'guisao': 'guisa o', 'hobrah': 'ho brah', 'hookd': 'hook d', 'hookt': 'hook t',
    'hotelsunken': 'hotel sunken', 'im': 'i m', 'imilky': 'i milky', 'intl': 'int l',
    'jamit': 'jam it', 'jeangeorges': 'jean georges', 'kapet': 'kape t', 'kchicken': 'k chicken',
    'kfeteria': 'k feteria', 'kfood': 'k food', 'kind': 'kin d', 'ktown': 'k town',
    'labeille': 'l abeille', 'laccolade': 'l accolade', 'ladresse': 'l adresse', 'lalbero': 'l albero',
    'lalgeroise': 'l algeroise', 'lamericana': 'l americana', 'lami': 'l ami', 'lamico': 'l amico',
    'lamore': 'l amore', 'lamour': 'l amour', 'langeletto': 'l angeletto', 'langolo': 'l angolo',
    'lantica': 'l antica', 'lappartement': 'l appartement', 'largot': 'l argot', 'laroma': 'l aroma',
    'larte': 'l arte', 'lartusi': 'l artusi', 'lavenue': 'l avenue', 'lavion': 'l avion',
    'lentrecote': 'l entrecote', 'lexpress': 'l express', 'lifechanging': 'life changing',
    'limprimerie': 'l imprimerie', 'lindustrie': 'l industrie', 'litaliano': 'l italiano', 'loreal': 'l oreal',
    'lores': 'lo res', 'losteria': 'l osteria', 'lunique': 'l unique', 'lwren': 'l wren',
    'mahalmannan': 'mahal mannan', 'mugz': 'mug z', 'munchn': 'munch n', 'nmore': 'n more',
    'nroll': 'n roll', 'nshpi': 'n shpi', 'ocasey': 'o casey', 'occaffe': 'o ccaffe',
    'oconnor': 'o connor', 'odonoghue': 'o donoghue', 'ogrady': 'o grady', 'ohanlon': 'o hanlon',
    'ohara': 'o hara', 'oharas': 'o haras', 'oldays': 'ol days', 'oneals': 'o neals',
    'oneill': 'o neill', 'onieals': 'o nieals', 'onsite': 'on site', 'osake': 'o sake',
    'osfizio': 'o sfizio', 'osun': 'o sun', 'osur': 'osu r', 'ote': 'o te',
    'otoole': 'o toole', 'otooles': 'o tooles', 'pal': 'pa l', 'palacetrouble': 'palace trouble',
    'picka': 'pick a', 'piecea': 'piece a', 'qrico': 'q rico', 'regz': 'reg z',
    'ritzcarlton': 'ritz carlton', 'said': 'sa id', 'saimer': 's aimer', 'satacos': 'sa tacos',
    'sekend': 'sek end', 'shaken': 'shake n', 'smashd': 'smash d', 'smores': 's mores',
    'songe': 'song e', 'stackd': 'stack d', 'steamr': 'steam r', 'ststage': 'st stage',
    'stuffd': 'stuff d', 'sugard': 'sugar d', 'taeem': 'ta eem', 'taime': 't aime',
    'takeout': 'take out', 'taverndanny': 'tavern danny', 'tearoom': 'tea room', 'treadz': 'tread z',
    'twigm': 'twig m', 'walkin': 'walk in', 'winemakher': 'winemak her', 'woodfired': 'wood fired',
    'zaatar': 'za atar', 'zgrill': 'z grill', 'pjclarkes': 'p j clarkes',
    'mcdonalds': 'mcdonalds', 'papajohns': 'papa johns', 'burgerking': 'burger king', 'kfc': 'kfc',
    'popeyes': 'popeyes', 'starbucks': 'starbucks', 'dunkin': 'dunkin', 'chipotle': 'chipotle',
    'subway': 'subway', 'tacobell': 'taco bell', 'pizzahut': 'pizza hut', 'wendys': 'wendy s',
    'fiveguys': 'five guys', 'chickfila': 'chick fil a', 'panera': 'panera bread', 'cinnabon': 'cinnabon',
    'baskinrobbins': 'baskin robbins', 'haagendazs': 'haagen dazs', 'benandjerrys': 'ben & jerrys',
    'auntieannes': 'auntie anne s', 'pretzeltime': 'pretzel time', 'nathansfamous': 'nathan s famous',
    'sbarro': 'sbarro', 'halalguys': 'the halal guys', 'shakeshack': 'shake shack', 'wholefoods': 'whole foods market', 'traderjoes': 'trader joes'
}

# --- STABLE NORMALIZATION FUNCTION ---
def normalize_search_term_for_hybrid(text):
    if not isinstance(text, str): return ''
    normalized_text = text.lower().replace('&', ' and ')
    accent_map = { 'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e', 'á': 'a', 'à': 'a', 'â': 'a', 'ä': 'a', 'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i', 'ó': 'o', 'ò': 'o', 'ô': 'o', 'ö': 'o', 'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u', 'ç': 'c', 'ñ': 'n' }
    for accented, unaccented in accent_map.items():
        normalized_text = normalized_text.replace(accented, unaccented)
    normalized_text = re.sub(r"[']", "", normalized_text)
    normalized_text = re.sub(r"[.\-/]", " ", normalized_text)
    normalized_text = re.sub(r"[^a-z0-9\s]", "", normalized_text)
    normalized_text = re.sub(r"\s+", " ", normalized_text)
    return normalized_text.strip()

# Teardown function to close the database connection
@app.teardown_appcontext
def teardown_db(exception):
    db_conn = g.pop('db_conn', None)
    if db_conn is not None:
        db_conn.close()
        logger.info("Database connection closed.")

# --- SEARCH ROUTE WITH DETAILED LOGGING ---
@app.route('/search', methods=['GET'])
def search():
    # 1. Get parameters
    search_term = request.args.get('name', '').strip()
    grade_filter = request.args.get('grade', type=str)
    boro_filter = request.args.get('boro', type=str)
    sort_by = request.args.get('sort', 'relevance', type=str)
    page = int(request.args.get('page', 1, type=int))
    per_page = int(request.args.get('per_page', 25, type=int))

    # --- CHECKPOINT 1: Log incoming request parameters ---
    logger.info(f"[BACKEND LOG] Received request: name='{search_term}', grade='{grade_filter}', boro='{boro_filter}', sort='{sort_by}'")

    if not search_term:
        return jsonify([])

    # 2. Normalize search term
    normalized_search = normalize_search_term_for_hybrid(search_term)
    term_for_synonym_check = re.sub(r"\s+", "", normalized_search)
    if term_for_synonym_check in SEARCH_TERM_SYNONYMS:
        normalized_search = SEARCH_TERM_SYNONYMS[term_for_synonym_check]
    
    if not normalized_search:
        return jsonify([])

    # 3. Build query components and parameter list
    params = []
    
    where_conditions = ["(dba_normalized_search ILIKE %s OR similarity(dba_normalized_search, %s) > 0.4)"]
    params.extend([f"%{normalized_search}%", normalized_search])

    if grade_filter and grade_filter.upper() in ['A', 'B', 'C', 'P', 'Z', 'N']:
        where_conditions.append("grade = %s")
        params.append(grade_filter.upper())
    if boro_filter:
        where_conditions.append("boro ILIKE %s")
        params.append(boro_filter)
    
    where_clause = " AND ".join(where_conditions)

    order_by_clause = ""
    sort_columns = ["camis", "dba"]

    if sort_by == 'name_asc':
        order_by_clause = "ORDER BY dba ASC"
    elif sort_by == 'name_desc':
        order_by_clause = "ORDER BY dba DESC"
    else: # Default relevance
        sort_columns.append("similarity(dba_normalized_search, %s) as relevance_score")
        params.append(normalized_search)
        order_by_clause = """
        ORDER BY
            CASE WHEN dba_normalized_search = %s THEN 0
                 WHEN dba_normalized_search ILIKE %s THEN 1
                 ELSE 2
            END,
            relevance_score DESC
        """
        params.extend([normalized_search, f"{normalized_search}%"])
    
    offset = (page - 1) * per_page
    params.extend([per_page, offset])

    # 4. Construct the final query string in a single, clear step
    full_query = f"""
        WITH paginated_camis AS (
            SELECT {", ".join(sort_columns)}
            FROM (
                SELECT DISTINCT ON (camis) * FROM restaurants ORDER BY camis, inspection_date DESC
            ) as latest_restaurants
            WHERE {where_clause}
            {order_by_clause}
            LIMIT %s OFFSET %s
        )
        SELECT r.*, v.violation_code, v.violation_description
        FROM restaurants r
        JOIN paginated_camis pc ON r.camis = pc.camis
        LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date
        ORDER BY r.camis, r.inspection_date DESC;
    """

    # 5. Execute query and process results
    try:
        if DatabaseConnection is None:
            raise ImportError("DatabaseConnection not available.")
        with DatabaseConnection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # --- CHECKPOINT 2: Log the exact query and parameters before execution ---
            mogrified_query = cursor.mogrify(full_query, tuple(params))
            logger.info(f"[BACKEND LOG] EXECUTING SQL:\n{mogrified_query.decode('utf-8')}\n")
            
            cursor.execute(full_query, tuple(params))
            results = cursor.fetchall()
            
            # --- CHECKPOINT 3: Log the number of rows returned from DB ---
            logger.info(f"[BACKEND LOG] Database returned {len(results)} rows.")

    except Exception as e:
        logger.error(f"DB search failed for '{search_term}': {e}", exc_info=True)
        return jsonify({"error": "Database query failed"}), 500

    if not results: return jsonify([])
        
    # 6. Process results into the final JSON structure
    restaurant_dict = {}
    for row in results:
        camis = str(row['camis'])
        if camis not in restaurant_dict:
            restaurant_data = {k: v for k, v in row.items() if k not in ['violation_code', 'violation_description', 'inspection_date', 'critical_flag', 'grade', 'inspection_type']}
            restaurant_dict[camis] = restaurant_data
            restaurant_dict[camis]['inspections'] = {}
        
        insp_date_str = row['inspection_date'].isoformat()
        if insp_date_str not in restaurant_dict[camis]['inspections']:
            restaurant_dict[camis]['inspections'][insp_date_str] = {
                'inspection_date': insp_date_str,
                'grade': row['grade'],
                'critical_flag': row['critical_flag'],
                'inspection_type': row['inspection_type'],
                'violations': []
            }

        if row['violation_code']:
            v_data = {'violation_code': row['violation_code'], 'violation_description': row['violation_description']}
            if v_data not in restaurant_dict[camis]['inspections'][insp_date_str]['violations']:
                restaurant_dict[camis]['inspections'][insp_date_str]['violations'].append(v_data)

    final_results = [{**data, 'inspections': sorted(list(data['inspections'].values()), key=lambda x: x['inspection_date'], reverse=True)} for data in restaurant_dict.values()]
    
    return jsonify(final_results)

# Other endpoints

@app.route('/recent', methods=['GET'])
def recent_restaurants(): return jsonify([])

@app.route('/trigger-update', methods=['POST'])
def trigger_update():
    if not update_logic_imported: return jsonify({"status": "error", "message": "Update logic unavailable."}), 500
    provided_key = request.headers.get('X-Update-Secret')
    expected_key = APIConfig.UPDATE_SECRET_KEY
    if not expected_key or not provided_key or not secrets.compare_digest(provided_key, expected_key):
        return jsonify({"status": "error", "message": "Unauthorized."}), 403
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
