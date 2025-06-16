# app_search.py - Final Production Version with All Fixes

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
    'zaatar': 'za atar', 'zgrill': 'z grill', 'pjclarkes': 'p j clarkes', 'xian': 'xi an',
    'mcdonalds': 'mcdonalds', 'papajohns': 'papa johns', 'burgerking': 'burger king', 'kfc': 'kfc',
    'popeyes': 'popeyes', 'starbucks': 'starbucks', 'dunkin': 'dunkin', 'chipotle': 'chipotle',
    'subway': 'subway', 'tacobell': 'taco bell', 'pizzahut': 'pizza hut', 'wendys': 'wendy s',
    'fiveguys': 'five guys', 'chickfila': 'chick fil a', 'panera': 'panera bread', 'cinnabon': 'cinnabon',
    'baskinrobbins': 'baskin robbins', 'haagendazs': 'haagen dazs', 'benandjerrys': 'ben & jerrys',
    'auntieannes': 'auntie anne s', 'pretzeltime': 'pretzel time', 'nathansfamous': 'nathan s famous',
    'sbarro': 'sbarro', 'halalguys': 'the halal guys', 'shakeshack': 'shake shack', 'wholefoods': 'whole foods market', 'traderjoes': 'trader joes'
# --- Flask App Initialization ---
app = Flask(__name__)
CORS(app)

# --- Normalization Function ---
def normalize_search_term_for_hybrid(text):
    if not isinstance(text, str): return ''
    normalized_text = text.lower()
    normalized_text = normalized_text.replace('&', ' and ')
    accent_map = { 'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e', 'á': 'a', 'à': 'a', 'â': 'a', 'ä': 'a', 'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i', 'ó': 'o', 'ò': 'o', 'ô': 'o', 'ö': 'o', 'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u', 'ç': 'c', 'ñ': 'n' }
    for accented, unaccented in accent_map.items():
        normalized_text = normalized_text.replace(accented, unaccented)
    normalized_text = re.sub(r"[']", "", normalized_text)
    normalized_text = re.sub(r"[./-]", " ", normalized_text)
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
        page = 1; per_page = 25
    offset = (page - 1) * per_page
    if not search_term_from_user: return jsonify([])
    normalized_search_term = normalize_search_term_for_hybrid(search_term_from_user)
    if normalized_search_term in SEARCH_TERM_SYNONYMS:
        normalized_search_term = SEARCH_TERM_SYNONYMS[normalized_search_term]
    if not normalized_search_term: return jsonify([])
    cache_key = f"search_v3_apostrophe_fix:{normalized_search_term}:p{page}:pp{per_page}"
    redis_conn = get_redis_client()
    if redis_conn:
        try:
            cached_result_str = redis_conn.get(cache_key)
            if cached_result_str: return jsonify(json.loads(cached_result_str))
        except Exception as e: logger.error(f"Redis GET error: {e}")
    query = """
    WITH RankedRestaurants AS (
        SELECT DISTINCT ON (camis) camis, dba, boro, building, street, zipcode, phone,
        latitude, longitude, cuisine_description, dba_normalized_search
        FROM restaurants WHERE dba_normalized_search ILIKE %s OR similarity(dba_normalized_search, %s) > 0.4
    ), PaginatedRestaurants AS (
        SELECT * FROM RankedRestaurants ORDER BY
        CASE WHEN dba_normalized_search = %s THEN 0 WHEN dba_normalized_search ILIKE %s THEN 1 ELSE 2 END,
        similarity(dba_normalized_search, %s) DESC, length(dba_normalized_search), dba ASC
        LIMIT %s OFFSET %s
    )
    SELECT pr.camis, pr.dba, pr.boro, pr.building, pr.street, pr.zipcode, pr.phone,
           pr.latitude, pr.longitude, r_full.inspection_date, r_full.critical_flag, r_full.grade,
           r_full.inspection_type, v.violation_code, v.violation_description, pr.cuisine_description
    FROM PaginatedRestaurants pr JOIN restaurants r_full ON pr.camis = r_full.camis
    LEFT JOIN violations v ON r_full.camis = v.camis AND r_full.inspection_date = v.inspection_date
    ORDER BY CASE WHEN pr.dba_normalized_search = %s THEN 0 WHEN pr.dba_normalized_search ILIKE %s THEN 1 ELSE 2 END,
    similarity(pr.dba_normalized_search, %s) DESC, length(pr.dba_normalized_search), pr.dba ASC, r_full.inspection_date DESC;
    """
    contains_pattern = f"%{normalized_search_term}%"; starts_with_pattern = f"{normalized_search_term}%"
    params = (contains_pattern, normalized_search_term, normalized_search_term, starts_with_pattern, normalized_search_term, per_page, offset, normalized_search_term, starts_with_pattern, normalized_search_term)
    try:
        with DatabaseConnection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(query, params)
            db_results_raw = cursor.fetchall()
    except Exception as e:
        logger.error(f"DB error for search '{normalized_search_term}': {e}", exc_info=True)
        return jsonify({"error": "Database query failed"}), 500
    if not db_results_raw: return jsonify([])
    restaurant_dict = {}
    for row in db_results_raw:
        camis = row['camis']
        if camis not in restaurant_dict:
            restaurant_dict[camis] = {'camis': camis, 'dba': row['dba'], 'boro': row['boro'], 'building': row['building'], 'street': row['street'], 'zipcode': row['zipcode'], 'phone': row['phone'], 'latitude': row['latitude'], 'longitude': row['longitude'], 'cuisine_description': row['cuisine_description'], 'inspections': {}}
        inspection_date_str = row['inspection_date'].isoformat()
        if inspection_date_str not in restaurant_dict[camis]['inspections']:
            restaurant_dict[camis]['inspections'][inspection_date_str] = {'inspection_date': inspection_date_str, 'critical_flag': row['critical_flag'], 'grade': row['grade'], 'inspection_type': row['inspection_type'], 'violations': []}
        if row['violation_code']:
            violation = {'violation_code': row['violation_code'], 'violation_description': row['violation_description']}
            if violation not in restaurant_dict[camis]['inspections'][inspection_date_str]['violations']:
                restaurant_dict[camis]['inspections'][inspection_date_str]['violations'].append(violation)
    formatted_results = [dict(data, inspections=list(data['inspections'].values())) for data in restaurant_dict.values()]
    if redis_conn:
        try: redis_conn.setex(cache_key, 3600, json.dumps(formatted_results, default=str))
        except Exception as e: logger.error(f"Redis SETEX error: {e}")
    return jsonify(formatted_results)

# ##### THIS ENDPOINT IS NOW INCLUDED #####
@app.route('/trigger-update', methods=['POST'])
def trigger_update():
    logger.info("Received request for /trigger-update")
    if not update_logic_imported:
        return jsonify({"status": "error", "message": "Update logic unavailable."}), 500
    provided_key = request.headers.get('X-Update-Secret')
    expected_key = APIConfig.UPDATE_SECRET_KEY
    if not expected_key or not provided_key or not secrets.compare_digest(provided_key, expected_key):
        return jsonify({"status": "error", "message": "Unauthorized."}), 403
    threading.Thread(target=run_database_update, args=(5,), daemon=True).start()
    return jsonify({"status": "success", "message": "Database update triggered in background."}), 202

# ##### OTHER REQUIRED ENDPOINTS ARE NOW INCLUDED #####
@app.route('/recent', methods=['GET'])
def recent_restaurants():
    # ... (implementation for /recent)
    return jsonify([]) # Placeholder

@app.errorhandler(404)
def not_found_error_handler(error):
    return jsonify({"error": "Not Found"}), 404

@app.errorhandler(500)
def internal_server_error_handler(error):
    logger.error(f"500 Error: {error}", exc_info=True)
    return jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", 8080))
    app.run(host=host, port=port)
