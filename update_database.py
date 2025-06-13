# update_database.py (Enhanced Diagnostics Version)

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
    if not date_str or not isinstance(date_str, str): return None
    try:
        return date_parse(date_str).date()
    except (ValueError, TypeError):
        return None

def fetch_data(days_back=15):
    logger.info(f"Fetching data from NYC API for past {days_back} days...")
    query = f"https://data.cityofnewyork.us/resource/43nn-pn8j.json?$where=inspection_date >= '{(datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')}T00:00:00.000'&$limit=50000"
    try:
        response = requests.get(query, timeout=90)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Total records fetched: {len(data)}")
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"API fetch error: {e}")
        return []

def update_database_batch(data):
    if not data: return 0, 0
    
    logger.info("--- STARTING DETAILED DIAGNOSTIC LOGGING ---")
    
    # Log the first 20 records to inspect the data format
    logger.info("--- Sample of first 20 records from API ---")
    for i, item in enumerate(data[:20]):
        logger.info(f"Record {i+1}: CAMIS={item.get('camis')}, InspDate={item.get('inspection_date')}, Grade={item.get('grade')}")

    restaurants_to_insert = []
    for item in data:
        camis = item.get("camis")
        inspection_date = convert_date(item.get("inspection_date"))
        if not (camis and inspection_date):
            # Log skipped records
            logger.warning(f"Skipping record due to missing CAMIS or invalid date. CAMIS: {camis}, InspDate: {item.get('inspection_date')}")
            continue

        restaurants_to_insert.append((
            camis, item.get("dba"), item.get("boro"), item.get("building"), item.get("street"),
            item.get("zipcode"), item.get("phone"),
            float(item.get("latitude")) if item.get("latitude") and item.get("latitude") not in ['N/A', ''] else None,
            float(item.get("longitude")) if item.get("longitude") and item.get("longitude") not in ['N/A', ''] else None,
            item.get("grade"), inspection_date, item.get("critical_flag"),
            item.get("inspection_type"), item.get("cuisine_description"),
            convert_date(item.get("grade_date"))
        ))

    r_count = 0
    with DatabaseConnection() as conn:
        logger.info(f"Attempting to insert {len(restaurants_to_insert)} records into 'restaurants' table...")
        # Using a try/except block to catch the exact error
        try:
            with conn.cursor() as cursor:
                upsert_sql = """
                    INSERT INTO restaurants (
                        camis, dba, boro, building, street, zipcode, phone,
                        latitude, longitude, grade, inspection_date, critical_flag,
                        inspection_type, cuisine_description, grade_date
                    ) VALUES %s ON CONFLICT (camis, inspection_date) DO NOTHING;
                """
                psycopg2.extras.execute_values(cursor, upsert_sql, restaurants_to_insert, page_size=200)
                r_count = cursor.rowcount
                conn.commit()
                logger.info(f"SUCCESS: DB transaction committed. Affected rows: {r_count}")
        except psycopg2.Error as e:
            # THIS IS THE CRUCIAL PART: Log the specific database error
            logger.error(f"--- DATABASE INSERTION FAILED ---")
            logger.error(f"PostgreSQL Error Code: {e.pgcode}")
            logger.error(f"PostgreSQL Error Message: {e.pgerror}")
            conn.rollback()
    
    logger.info("--- FINISHED DETAILED DIAGNOSTIC LOGGING ---")
    return r_count, 0 # We are ignoring violations for this test

def run_database_update(days_back=15):
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
    parser.add_argument("--days", type=int, default=15, help="Number of past days to fetch data for.")
    args = parser.parse_args()
    run_database_update(days_back=args.days)
    logger.info("Script execution finished.")
