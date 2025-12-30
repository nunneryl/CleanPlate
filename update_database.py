# In file: update_database.py (Final Optimized Version)

import logging
import argparse
from utils import normalize_search_term_for_hybrid
from datetime import datetime, timedelta
from dateutil.parser import parse as date_parse
import requests
import psycopg
from psycopg.rows import dict_row

from db_manager import DatabaseConnection, DatabaseManager
from config import APIConfig

# --- Constants ---
CRITICAL_FLAG = 'Critical'
NOT_CRITICAL_FLAG = 'Not Critical'
NOT_APPLICABLE = 'N/A'
NYC_API_BASE_URL = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"
API_RECORD_LIMIT = 500000
PENDING_GRADES = {'P', 'Z', 'N', None, ''}
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
    
    logger.info("Aggregating inspection data from API response...")
    inspections_data = {}
    for item in data:
        camis = item.get("camis")
        inspection_date = convert_date(item.get("inspection_date"))
        if not (camis and inspection_date): continue
        
        inspection_key = (camis, inspection_date)
        if inspection_key not in inspections_data:
            inspections_data[inspection_key] = {"details": item, "violations": set(), "critical_flags": []}
        
        violation_code = item.get("violation_code")
        if violation_code:
            inspections_data[inspection_key]["violations"].add(
                (violation_code, item.get("violation_description"))
            )
        inspections_data[inspection_key]["critical_flags"].append(item.get("critical_flag"))

    restaurants_to_update = []
    violations_to_insert = []
    grade_updates_to_insert = []

    with DatabaseConnection() as conn, conn.cursor(row_factory=dict_row) as cursor:
        
        logger.info(f"Fetching corresponding records from local database for comparison...")
        inspection_keys_to_check = list(inspections_data.keys())
        existing_records = {}
        camis_list = []
        
        if inspection_keys_to_check:
            camis_list = list(set([key[0] for key in inspection_keys_to_check]))
            dates_list = [key[1] for key in inspection_keys_to_check]

            query = """
                SELECT r.camis, r.inspection_date, r.grade, r.action, r.critical_flag
                FROM unnest(%s::varchar[], %s::date[]) AS t(camis, inspection_date)
                JOIN restaurants r ON r.camis = t.camis AND CAST(r.inspection_date AS DATE) = t.inspection_date;
            """
            cursor.execute(query, (camis_list, dates_list))
            existing_records_raw = cursor.fetchall()
            
            for rec in existing_records_raw:
                existing_records[(rec['camis'], rec['inspection_date'].date())] = rec
            
            logger.info(f"Found {len(existing_records)} existing records to compare against.")

        # --- Fetch recently logged updates to prevent duplicates ---
        cursor.execute("""
            SELECT restaurant_camis, inspection_date FROM grade_updates
            WHERE update_date >= NOW() - INTERVAL '30 days'
        """)
        logged_updates = {(row['restaurant_camis'], row['inspection_date']) for row in cursor.fetchall()}
        logger.info(f"Found {len(logged_updates)} recently logged grade updates to cross-reference.")
        
        # --- Fetch the LATEST known inspection date for each relevant restaurant ---
        if camis_list:
            cursor.execute("""
                SELECT camis, MAX(inspection_date) as max_date
                FROM restaurants
                WHERE camis = ANY(%s)
                GROUP BY camis
            """, (camis_list,))
            latest_inspection_dates = {row['camis']: row['max_date'].date() for row in cursor.fetchall()}
            logger.info(f"Fetched latest known inspection dates for {len(latest_inspection_dates)} restaurants.")
        else:
            latest_inspection_dates = {}
        
        logger.info("Comparing API data with local data to find what's new or changed...")
        for key, inspection in inspections_data.items():
            camis, inspection_date = key
            details_item = inspection["details"]
            
            # --- FIX #2 in action: Check if the incoming inspection is outdated ---
            latest_known_date = latest_inspection_dates.get(camis)
            if latest_known_date and inspection_date < latest_known_date:
                # We already have a newer inspection on file for this restaurant.
                # This update is for an older, irrelevant inspection. Skip it entirely.
                continue
            
            existing_record = existing_records.get(key)
            
            is_critical = any(flag == CRITICAL_FLAG for flag in inspection["critical_flags"])
            critical_flag_for_inspection = CRITICAL_FLAG if is_critical else NOT_CRITICAL_FLAG
            
            needs_db_update = False
            if not existing_record:
                needs_db_update = True
            else:
                if (details_item.get("grade") != existing_record.get("grade") or
                    details_item.get("action") != existing_record.get("action") or
                    critical_flag_for_inspection != existing_record.get("critical_flag")):
                    needs_db_update = True

            if needs_db_update:
                new_grade = details_item.get("grade")
                # ---  in action: Check if the event was already logged ---
                already_logged = key in logged_updates
              
                # --- Now includes the check for duplicates ---
                if existing_record and existing_record['grade'] in PENDING_GRADES and new_grade in FINAL_GRADES and not already_logged:
                    logger.info(f"Grade Finalized DETECTED for CAMIS {camis} on {inspection_date}: {existing_record['grade'] or 'NULL'} -> {new_grade}")
                    grade_updates_to_insert.append((camis, existing_record['grade'], new_grade, 'finalized', inspection_date))
                
                # Also capture new inspections that come in with immediate grades
                elif not existing_record and new_grade in FINAL_GRADES and not already_logged:
                    logger.info(f"New Graded Inspection for CAMIS {camis} on {inspection_date}: {new_grade}")
                    grade_updates_to_insert.append((camis, None, new_grade, 'new_inspection', inspection_date))

                dba = details_item.get("dba")
                normalized_dba = normalize_search_term_for_hybrid(dba) if dba else None
                
                restaurants_to_update.append((
                    camis, dba, normalized_dba, details_item.get("boro"), details_item.get("building"),
                    details_item.get("street"), details_item.get("zipcode"), details_item.get("phone"),
                    _to_float_or_none(details_item.get("latitude")), _to_float_or_none(details_item.get("longitude")),
                    details_item.get("grade"), inspection_date, critical_flag_for_inspection,
                    details_item.get("inspection_type"), details_item.get("cuisine_description"),
                    convert_date(details_item.get("grade_date")), details_item.get("action")
                ))

            for v_code, v_desc in inspection["violations"]:
                violations_to_insert.append((camis, inspection_date, v_code, v_desc))

        r_count, v_count, u_count = 0, 0, 0
        if restaurants_to_update:
            logger.info(f"Found {len(restaurants_to_update)} restaurants that are new or have changed. Updating now...")
            upsert_sql = """
                INSERT INTO restaurants (camis, dba, dba_normalized_search, boro, building, street, zipcode, phone, latitude, longitude, grade, inspection_date, critical_flag, inspection_type, cuisine_description, grade_date, action)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                ON CONFLICT (camis, inspection_date) DO UPDATE SET dba = EXCLUDED.dba, dba_normalized_search = EXCLUDED.dba_normalized_search, boro = EXCLUDED.boro, building = EXCLUDED.building, street = EXCLUDED.street, zipcode = EXCLUDED.zipcode, phone = EXCLUDED.phone, latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude, grade = COALESCE(EXCLUDED.grade, restaurants.grade), critical_flag = EXCLUDED.critical_flag, inspection_type = EXCLUDED.inspection_type, cuisine_description = EXCLUDED.cuisine_description, grade_date = COALESCE(EXCLUDED.grade_date, restaurants.grade_date), action = EXCLUDED.action;
            """

            cursor.executemany(upsert_sql, restaurants_to_update)
            r_count = cursor.rowcount
        else:
            logger.info("No new or changed restaurant records to update.")

        if violations_to_insert:
            insert_sql = "INSERT INTO violations (camis, inspection_date, violation_code, violation_description) VALUES (%s, %s, %s, %s) ON CONFLICT (camis, inspection_date, violation_code, violation_description) DO NOTHING;"
            cursor.executemany(insert_sql, violations_to_insert)
            v_count = cursor.rowcount
        
        if grade_updates_to_insert:
            update_sql = "INSERT INTO grade_updates (restaurant_camis, previous_grade, new_grade, update_type, inspection_date) VALUES (%s, %s, %s, %s, %s);"
            cursor.executemany(update_sql, grade_updates_to_insert)
            u_count = cursor.rowcount
            logger.info(f"Grade updates insert executed. Affected rows: {u_count}")

        conn.commit()
    return r_count, v_count, u_count
def run_database_update(days_back=3):
    logger.info(f"Starting DB update (days_back={days_back})")
    data = fetch_data(days_back)
    if data:
        r_upd, v_ins, u_ins = update_database_batch(data)
        logger.info(f"Update complete. Total Restaurants processed: {r_upd}, Total Violations: {v_ins}, Total Grade Updates: {u_ins}")
    else:
        logger.warning("No data from API.")
    logger.info("DB update finished.")
    
    logger.info("Attempting to clear API cache...")
    try:
        api_url = f"{APIConfig.API_BASE_URL}/clear-cache"
        headers = {'X-Update-Secret': APIConfig.UPDATE_SECRET_KEY}
        response = requests.post(api_url, headers=headers, timeout=30)
        if response.status_code == 200:
            logger.info("Successfully cleared API cache.")
        else:
            logger.error(f"Failed to clear API cache. Status: {response.status_code}, Response: {response.text}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Could not connect to API to clear cache: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Update restaurant inspection database.")
    parser.add_argument("--days", type=int, default=3, help="Number of past days to fetch data for.")
    args = parser.parse_args()
    run_database_update(days_back=args.days)
    DatabaseManager.close_all_connections()
    logger.info("Database connection pool closed.")
