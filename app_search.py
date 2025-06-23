import os
import re
import logging
import json
import threading
import secrets
from flask import Flask, jsonify, request, g
from flask_cors import CORS
import psycopg2
import psycopg2.extras

from db_manager import DatabaseConnection
from config import APIConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

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

def normalize_search_term_for_hybrid(text):
    if not isinstance(text, str): return ''
    normalized_text = text.lower().replace('&', ' and ')
    accent_map = { 'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e', 'á': 'a', 'à': 'a', 'â': 'a', 'ä': 'a', 'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i', 'ó': 'o', 'ò': 'o', 'ô': 'o', 'ö': 'o', 'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u', 'ç': 'c', 'ñ': 'n' }
    for accented, unaccented in accent_map.items():
        normalized_text = normalized_text.replace(accented, unaccented)
    normalized_text = re.sub(r"['.]", "", normalized_text)
    normalized_text = re.sub(r"[-/]", " ", normalized_text)
    normalized_text = re.sub(r"[^a-z0-9\s]", "", normalized_text)
    normalized_text = re.sub(r"\s+", " ", normalized_text)
    return normalized_text.strip()

@app.teardown_appcontext
def teardown_db(exception):
    db_conn = g.pop('db_conn', None)
    if db_conn is not None:
        db_conn.close()

@app.route('/search', methods=['GET'])
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
        where_conditions.append("grade = %s")
        params.append(grade_filter.upper())
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
    else: # Default relevance sort
        order_by_clause = """
        ORDER BY
            CASE WHEN dba_normalized_search = %s THEN 0
                 WHEN dba_normalized_search ILIKE %s THEN 1
                 ELSE 2
            END,
            similarity(dba_normalized_search, %s) DESC,
            length(dba_normalized_search)
        """
        order_by_params = [normalized_search, f"{normalized_search}%", normalized_search]

    id_fetch_query = f"""
        SELECT camis FROM (
            SELECT DISTINCT ON (camis) camis, dba, dba_normalized_search, grade, inspection_date, cuisine_description, boro
            FROM restaurants
            ORDER BY camis, inspection_date DESC
        ) AS latest_restaurants
        WHERE {where_clause}
        {order_by_clause}
        LIMIT %s OFFSET %s;
    """
    
    offset = (page - 1) * per_page
    id_fetch_params = tuple(params + order_by_params + [per_page, offset])

    try:
        with DatabaseConnection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(id_fetch_query, id_fetch_params)
                paginated_camis_tuples = cursor.fetchall()
            
            if not paginated_camis_tuples:
                return jsonify([])

            paginated_camis = [item[0] for item in paginated_camis_tuples]
            
            details_query = """
                SELECT r.*, v.violation_code, v.violation_description
                FROM restaurants r
                LEFT JOIN violations v ON r.camis = v.camis AND r.inspection_date = v.inspection_date
                WHERE r.camis = ANY(%s)
            """
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as details_cursor:
                details_cursor.execute(details_query, (paginated_camis,))
                all_rows = details_cursor.fetchall()

    except Exception as e:
        logger.error(f"DB search failed for '{search_term}': {e}", exc_info=True)
        return jsonify({"error": "Database query failed"}), 500

    restaurant_details_map = {str(camis): [] for camis in paginated_camis}
    for row in all_rows:
        restaurant_details_map[str(row['camis'])].append(row)

    final_results = []
    for camis in paginated_camis:
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
                    'inspection_date': insp_date_str, 'grade': row['grade'],
                    'critical_flag': row['critical_flag'], 'inspection_type': row['inspection_type'],
                    'violations': []
                }
            if row['violation_code']:
                v_data = {'violation_code': row['violation_code'], 'violation_description': row['violation_description']}
                if v_data not in inspections[insp_date_str]['violations']:
                    inspections[insp_date_str]['violations'].append(v_data)

        base_info['inspections'] = sorted(list(inspections.values()), key=lambda x: x['inspection_date'], reverse=True)
        for key in ['violation_code', 'violation_description', 'grade', 'inspection_date', 'critical_flag', 'inspection_type']:
            base_info.pop(key, None)
            
        final_results.append(base_info)
        
    return jsonify(final_results)

@app.route('/trigger-update', methods=['POST'])
def trigger_update():
    # Import locally to avoid circular dependency at startup
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
