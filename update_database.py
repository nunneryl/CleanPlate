# In file: update_database.py

import logging
import argparse
from utils import normalize_search_term_for_hybrid
from datetime import datetime, timedelta
from dateutil.parser import parse as date_parse
import requests
import psycopg

from db_manager import DatabaseConnection, DatabaseManager
from config import APIConfig

# --- Constants ---
CRITICAL_FLAG = 'Critical'
NOT_CRITICAL_FLAG = 'Not Critical'
NOT_APPLICABLE = 'N/A'
NYC_API_BASE_URL = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"
API_RECORD_LIMIT = 500000
PENDING_GRADES = {'P', 'Z', 'N'}
FINAL_GRADES = {'A', 'B', 'C'}

# --- Logger Setup ---
logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

def _to_float_or_none(value_str):
    if value_str and value_str not in [NOT_APPLICABLE, '']:
        try:
            return float(value_str)
        except (ValueError, TypeError):
            return None
    return None

def convert_date(date_str):
    if not date_str or not isinstance(date_str, str): return None
    try:
        return date_parse(date_str).date()
    except (ValueError, TypeError):
        return None

def fetch_data(days_back=3):
    logger.info(f"Fetching records updated in the last {days_back} days from NYC API...")
    api_params = {
        "$where": f":updated_at >= '{(datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')}T00:00:00.000'",
        "$limit": API_RECORD_LIMIT
    }
    try:
        response = requests.get(NYC_API_BASE_URL, params=api_params, timeout=180)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Total records fetched: {len(data)}")
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"API fetch error: {e}")
        return []

def update_database_batch(data):
    if not data: return 0, 0, 0
    
    inspections_data = {}
    for item in data:
        camis = item.get("camis")
        inspection_date = convert_date(item.get("inspection_date"))
        if not (camis and inspection_date): continue
        
        inspection_key = (camis, inspection_date)
        if inspection_key not in inspections_data:
            inspections_data[inspection_key] = {"details": item, "violations": []}
        
        if item.get("violation_code"):
            inspections_data[inspection_key]["violations"].append(item)

    restaurants_to_insert = []
    violations_to_insert = []
    
    # ---List to hold grade update events ---
    grade_updates_to_insert = []

    with DatabaseConnection() as conn, conn.cursor() as cursor:
        for key, inspection in inspections_data.items():
            camis, inspection_date = key
            new_grade = inspection["details"].get("grade")

            # --- NEW: Logic to check for stale-to-fresh updates ---
            if new_grade and new_grade in FINAL_GRADES:
                # Query for the previous most recent grade
                cursor.execute(
                    "SELECT grade FROM restaurants WHERE camis = %s ORDER BY inspection_date DESC LIMIT 1",
                    (camis,)
                )
                result = cursor.fetchone()
                previous_grade = result[0] if result else None

                if previous_grade and previous_grade in PENDING_GRADES:
                    logger.info(f"Grade Update DETECTED for CAMIS {camis}: {previous_grade} -> {new_grade}")
                    grade_updates_to_insert.append((camis, previous_grade, new_grade))
            # --- END NEW ---

            has_final_grade = next((v for v in inspection["violations"] if v.get("grade") and v.get("grade") in ['A', 'B', 'C']), None)
            has_any_action = next((v for v in inspection["violations"] if v.get("action")), None)
            details_item = has_final_grade or has_any_action or inspection["details"]

            dba = details_item.get("dba")
            normalized_dba = normalize_search_term_for_hybrid(dba) if dba else None
            
            is_critical = any(v.get("critical_flag") == CRITICAL_FLAG for v in inspection["violations"])
            critical_flag_for_inspection = CRITICAL_FLAG if is_critical else NOT_CRITICAL_FLAG

            restaurants_to_insert.append((
                camis, dba, normalized_dba,
                details_item.get("boro"), details_item.get("building"), details_item.get("street"),
                details_item.get("zipcode"), details_item.get("phone"),
                _to_float_or_none(details_item.get("latitude")),
                _to_float_or_none(details_item.get("longitude")),
                details_item.get("grade"), inspection_date, critical_flag_for_inspection,
                details_item.get("inspection_type"), details_item.get("cuisine_description"),
                convert_date(details_item.get("grade_date")),
                details_item.get("action")
            ))
            
            for v_item in inspection["violations"]:
                if v_item.get("violation_code"):
                    violations_to_insert.append((camis, inspection_date, v_item.get("violation_code"), v_item.get("violation_description")))

        r_count, v_count, u_count = 0, 0, 0
        if restaurants_to_insert:
            logger.info(f"Executing batch insert for {len(restaurants_to_insert)} unique restaurant inspections...")
            upsert_sql = """
                INSERT INTO restaurants (
                    camis, dba, dba_normalized_search, boro, building, street, zipcode, phone,
                    latitude, longitude, grade, inspection_date, critical_flag,
                    inspection_type, cuisine_description, grade_date, action
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                ON CONFLICT (camis, inspection_date) DO UPDATE SET
                    dba = EXCLUDED.dba,
                    dba_normalized_search = EXCLUDED.dba_normalized_search,
                    boro = EXCLUDED.boro,
                    grade = EXCLUDED.grade,
                    critical_flag = EXCLUDED.critical_flag,
                    action = EXCLUDED.action;
            """
            cursor.executemany(upsert_sql, restaurants_to_insert)
            r_count = cursor.rowcount
            logger.info(f"Restaurant insert executed. Affected rows: {r_count}")
        if violations_to_insert:
            unique_violations = list(set(violations_to_insert))
            logger.info(f"Executing batch insert for {len(unique_violations)} unique violations...")
            insert_sql = "INSERT INTO violations (camis, inspection_date, violation_code, violation_description) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;"
            cursor.executemany(insert_sql, unique_violations)
            v_count = cursor.rowcount
            logger.info(f"Violation insert executed. Affected rows: {v_count}")
        
        # --- NEW: Insert any detected grade updates ---
        if grade_updates_to_insert:
            logger.info(f"Executing batch insert for {len(grade_updates_to_insert)} grade updates...")
            update_sql = "INSERT INTO grade_updates (restaurant_camis, previous_grade, new_grade) VALUES (%s, %s, %s);"
            cursor.executemany(update_sql, grade_updates_to_insert)
            u_count = cursor.rowcount
            logger.info(f"Grade updates insert executed. Affected rows: {u_count}")
        # --- END NEW ---

        conn.commit()
        logger.info("DB transaction explicitly committed.")
    return r_count, v_count, u_count

def run_database_update(days_back=3):
    logger.info(f"Starting DB update (days_back={days_back})")
    data = fetch_data(days_back)
    if data:
        r_upd, v_ins, u_ins = update_database_batch(data)
        logger.info(f"Update complete. Restaurants: {r_upd}, Violations: {v_ins}, Grade Updates: {u_ins}")
    else:
        logger.warning("No data from API.")
    logger.info("DB update finished.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Update restaurant inspection database.")
    parser.add_argument("--days", type=int, default=3, help="Number of past days to fetch data for.")
    args = parser.parse_args()
    run_database_update(days_back=args.days)
    DatabaseManager.close_all_connections()
    logger.info("Database connection pool closed.")
