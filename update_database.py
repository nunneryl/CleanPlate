# update_database.py - More Resilient Version

import os
import requests
import logging
import argparse
from datetime import datetime, timedelta
from dateutil.parser import parse as date_parse
import psycopg2
import psycopg2.extras

try:
    from db_manager import DatabaseConnection
    from config import APIConfig
except ImportError:
    logging.critical("Failed to import db_manager or config. Using placeholders.")
    class DatabaseConnection:
        def __enter__(self): return None
        def __exit__(self, type, value, traceback): pass
    class APIConfig:
        API_REQUEST_LIMIT = 50000
        NYC_API_URL = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"
        NYC_API_APP_TOKEN = None

logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def convert_date(date_str):
    """
    More robustly converts a string to a date object.
    It now handles more formats and gracefully returns None for invalid input.
    """
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        # dateutil.parser is very flexible and can handle various date formats.
        return date_parse(date_str).date()
    except (ValueError, TypeError):
        logger.warning(f"Could not parse date '{date_str}'. Skipping.")
        return None

def fetch_data(days_back=5, max_retries=3):
    logger.info(f"Fetching data from NYC API for past {days_back} days...")
    results = []
    limit = APIConfig.API_REQUEST_LIMIT
    offset = 0
    total_fetched = 0
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    date_filter = f"inspection_date >= '{start_date.strftime('%Y-%m-%d')}T00:00:00.000'"

    while True:
        base_url = APIConfig.NYC_API_URL
        params = {"$limit": limit, "$offset": offset, "$where": date_filter, "$order": "inspection_date DESC"}
        headers = {"X-App-Token": APIConfig.NYC_API_APP_TOKEN} if APIConfig.NYC_API_APP_TOKEN else {}
        try:
            response = requests.get(base_url, headers=headers, params=params, timeout=60)
            response.raise_for_status()
            current_data_batch = response.json()
            if not current_data_batch:
                break
            results.extend(current_data_batch)
            total_fetched += len(current_data_batch)
            logger.info(f"Fetched {len(current_data_batch)} records, total: {total_fetched}")
            if len(current_data_batch) < limit:
                break
            offset += len(current_data_batch)
        except requests.exceptions.RequestException as e:
            logger.error(f"API fetch error: {e}")
            break
    logger.info(f"Total records fetched: {total_fetched}")
    return results

def update_database_batch(data):
    if not data: return 0, 0
    logger.info(f"Preparing batch update for {len(data)} records...")
    restaurants_to_upsert = []
    violations_to_insert = []
    processed_restaurant_keys = set()

    for item in data:
        camis = item.get("camis")
        inspection_date = convert_date(item.get("inspection_date"))
        
        # This is the main gatekeeper: if either of these are missing, skip the record.
        if not (camis and inspection_date):
            continue

        restaurant_key = (camis, inspection_date)
        if restaurant_key not in processed_restaurant_keys:
            restaurants_to_upsert.append((
                camis, item.get("dba"), item.get("boro"), item.get("building"), item.get("street"),
                item.get("zipcode"), item.get("phone"),
                float(item.get("latitude")) if item.get("latitude") and item.get("latitude") not in ['N/A', ''] else None,
                float(item.get("longitude")) if item.get("longitude") and item.get("longitude") not in ['N/A', ''] else None,
                item.get("grade"), inspection_date, item.get("critical_flag"),
                item.get("inspection_type"), item.get("cuisine_description"),
                convert_date(item.get("grade_date"))
            ))
            processed_restaurant_keys.add(restaurant_key)
        
        if item.get("violation_code"):
             violations_to_insert.append((camis, inspection_date, item.get("violation_code"), item.get("violation_description")))

    r_count, v_count = 0, 0
    with DatabaseConnection() as conn, conn.cursor() as cursor:
        if restaurants_to_upsert:
            logger.info(f"Executing batch upsert for {len(restaurants_to_upsert)} restaurants...")
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
        logger.info("DB transaction committed.")

    return r_count, v_count

def run_database_update(days_back=5):
    logger.info(f"Starting DB update (days_back={days_back})")
    data = fetch_data(days_back=days_back)
    if data:
        r_upd, v_ins = update_database_batch(data)
        logger.info(f"Update complete. Restaurants processed: {r_upd}, Violations: {v_ins}")
    else:
        logger.warning("No data from API.")
    logger.info("DB update finished.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Update restaurant inspection database.")
    parser.add_argument("--days", type=int, default=5, help="Number of past days to fetch data for.")
    args = parser.parse_args()
    run_database_update(days_back=args.days)
    logger.info("Script execution finished.")
