# update_database.py - Simplified for use with database triggers

import os
import requests
import logging
import argparse
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
from dateutil.parser import parse as date_parse

# Assuming db_manager.py and config.py are in the same directory or accessible via PYTHONPATH
try:
    from db_manager import DatabaseConnection
    from config import APIConfig
except ImportError:
    logging.critical("Failed to import db_manager or config for update_database.py. Using placeholders.")
    class DatabaseConnection:
        def __enter__(self): return None
        def __exit__(self, type, value, traceback): pass
    class APIConfig:
        API_REQUEST_LIMIT = 1000
        NYC_API_URL = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"
        NYC_API_APP_TOKEN = None

logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def print_debug(message):
    logger.info(f"---> SCRIPT DEBUG: {message}")

def convert_date(date_str):
    if not date_str: return None
    try:
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try: return datetime.strptime(date_str, fmt).date()
            except ValueError: continue
        return date_parse(date_str).date()
    except Exception as e:
        logger.warning(f"Could not parse date '{date_str}': {e}")
        return None

def fetch_data(days_back=5, max_retries=3): # Normal operational days_back
    logger.info(f"Fetching data from NYC API for past {days_back} days...")
    results = []
    limit = APIConfig.API_REQUEST_LIMIT
    offset = 0
    total_fetched = 0
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days_back)
    date_filter = f"inspection_date between '{start_date.strftime('%Y-%m-%d')}T00:00:00.000' and '{end_date.strftime('%Y-%m-%d')}T23:59:59.999'"

    while True:
        base_url = APIConfig.NYC_API_URL
        params = {"$limit": limit, "$offset": offset, "$where": date_filter, "$order": "inspection_date DESC"}
        headers = {"X-App-Token": APIConfig.NYC_API_APP_TOKEN} if APIConfig.NYC_API_APP_TOKEN else {}
        current_data_batch = None; attempt = 0
        while attempt < max_retries:
            attempt += 1
            try:
                response = requests.get(base_url, headers=headers, params=params, timeout=60)
                response.raise_for_status()
                current_data_batch = response.json()
                break
            except requests.exceptions.RequestException as req_err:
                logger.error(f"Network error fetch attempt {attempt}/{max_retries}: {req_err}", exc_info=True)
                if attempt == max_retries: return results
                import time; time.sleep(5 * attempt)
            except Exception as e:
                logger.error(f"Unexpected API fetch error attempt {attempt}/{max_retries}: {e}", exc_info=True)
                return results
        if current_data_batch is None or not current_data_batch: break
        results.extend(current_data_batch)
        total_fetched += len(current_data_batch)
        logger.info(f"Fetched {len(current_data_batch)} records, total: {total_fetched}")
        if len(current_data_batch) < limit: break
        offset += len(current_data_batch)
    logger.info(f"Total records fetched: {total_fetched}")
    return results

def update_database_batch(data):
    if not data: return 0, 0
    logger.info(f"Preparing batch update for {len(data)} records (DB triggers handle derived columns)...")
    restaurants_to_upsert = []
    violations_to_insert = []
    processed_restaurant_keys = set()

    for item in data:
        try:
            camis = item.get("camis")
            dba = item.get("dba") # Raw DBA
            inspection_date = convert_date(item.get("inspection_date"))
            if not (camis and inspection_date): continue
            restaurant_key = (camis, inspection_date)
            if restaurant_key not in processed_restaurant_keys:
                restaurants_to_upsert.append((
                    camis, dba, item.get("boro"), item.get("building"), item.get("street"),
                    item.get("zipcode"), item.get("phone"),
                    float(item.get("latitude")) if item.get("latitude") and item.get("latitude") not in ['N/A', ''] else None,
                    float(item.get("longitude")) if item.get("longitude") and item.get("longitude") not in ['N/A', ''] else None,
                    item.get("grade"), inspection_date, item.get("critical_flag"),
                    item.get("inspection_type"), item.get("cuisine_description"),
                    convert_date(item.get("grade_date"))
                    # Note: dba_normalized_search and dba_tsv are NOT included here; DB triggers will populate them.
                ))
                processed_restaurant_keys.add(restaurant_key)
            if item.get("violation_code"):
                 violations_to_insert.append((camis, inspection_date, item.get("violation_code"), item.get("violation_description")))
        except Exception as e:
            logger.error(f"Error preparing record CAMIS={item.get('camis', 'N/A')} for batch: {e}", exc_info=True)

    conn = None; success = False; r_count = 0; v_count = 0
    try:
        with DatabaseConnection() as conn, conn.cursor() as cursor:
            if restaurants_to_upsert:
                logger.info(f"Executing batch upsert for {len(restaurants_to_upsert)} restaurants...")
                # SQL only includes columns directly provided by the script
                upsert_sql = """
                    INSERT INTO restaurants (
                        camis, dba, boro, building, street, zipcode, phone,
                        latitude, longitude, grade, inspection_date, critical_flag,
                        inspection_type, cuisine_description, grade_date
                    ) VALUES %s
                    ON CONFLICT (camis, inspection_date) DO UPDATE SET
                        dba = EXCLUDED.dba, boro = EXCLUDED.boro, building = EXCLUDED.building,
                        street = EXCLUDED.street, zipcode = EXCLUDED.zipcode, phone = EXCLUDED.phone,
                        latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude, grade = EXCLUDED.grade,
                        critical_flag = EXCLUDED.critical_flag, inspection_type = EXCLUDED.inspection_type,
                        cuisine_description = EXCLUDED.cuisine_description, grade_date = EXCLUDED.grade_date;
                """
                psycopg2.extras.execute_values(cursor, upsert_sql, restaurants_to_upsert, page_size=100)
                r_count = cursor.rowcount if cursor.rowcount != -1 else len(restaurants_to_upsert)
                logger.info(f"Restaurant upsert executed. Affected rows (approx): {r_count}")
            if violations_to_insert:
                unique_violations = list(set(violations_to_insert))
                logger.info(f"Executing batch insert for {len(unique_violations)} unique violations...")
                insert_sql = "INSERT INTO violations (camis, inspection_date, violation_code, violation_description) VALUES %s ON CONFLICT DO NOTHING;"
                psycopg2.extras.execute_values(cursor, insert_sql, unique_violations, page_size=1000)
                v_count = cursor.rowcount if cursor.rowcount != -1 else len(unique_violations)
                logger.info(f"Violation insert executed. Affected rows (approx): {v_count}")
            conn.commit()
            success = True
            logger.info("DB transaction committed.")
    except Exception as e:
        logger.error(f"Batch DB update error: {e}", exc_info=True)
        if conn: conn.rollback()
    return r_count if success else 0, v_count if success else 0

def run_database_update(days_back=5): # Default for daily runs
    logger.info(f"Starting DB update (days_back={days_back})")
    try:
        data = fetch_data(days_back=days_back)
        if data:
            r_upd, v_ins = update_database_batch(data)
            logger.info(f"Update complete. Restaurants processed: {r_upd}, Violations: {v_ins}")
        else: logger.warning("No data from API.")
    except Exception as e:
        logger.critical(f"Uncaught exception in run_database_update: {e}", exc_info=True)
    finally: logger.info("DB update finished.")

if __name__ == '__main__':
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', force=True)
    parser = argparse.ArgumentParser(description="Update restaurant inspection database.")
    parser.add_argument("--days", type=int, default=5, help="Number of past days to fetch data for.")
    args = parser.parse_args() # Removed --full-backfill as local script handles that now
    run_database_update(days_back=args.days)
    logger.info("Script execution finished.")
