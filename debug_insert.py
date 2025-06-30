# debug_insert.py (Final Version with Explicit Transaction Control)
import os
import logging
from datetime import datetime, timedelta
from dateutil.parser import parse as date_parse
import requests
import psycopg
from psycopg.rows import dict_row

from db_manager import DatabaseConnection, DatabaseManager
from utils import normalize_search_term_for_hybrid
from config import APIConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TARGET_CAMIS = '50032994'

def convert_date(date_str):
    if not date_str or not isinstance(date_str, str): return None
    try: return date_parse(date_str).date()
    except (ValueError, TypeError): return None

def fetch_data(days_back=30):
    logger.info(f"--> Fetching data from NYC API for past {days_back} days...")
    query_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    query = f"https://data.cityofnewyork.us/resource/43nn-pn8j.json?$where=inspection_date >= '{query_date}T00:00:00.000'&$limit=50000"
    try:
        response = requests.get(query, timeout=90)
        response.raise_for_status()
        data = response.json()
        logger.info(f"--> Successfully fetched {len(data)} records.")
        return data
    except Exception as e:
        logger.error(f"--> API fetch error: {e}")
        return []

def find_and_insert_specific_restaurant():
    DatabaseManager.initialize_pool()
    logger.info(f"--- STARTING DEBUG SCRIPT for CAMIS: {TARGET_CAMIS} ---")

    data = fetch_data()
    if not data:
        logger.error("STOP: Failed to fetch any data from API.")
        return

    target_item = None
    for item in data:
        if item.get("camis") == TARGET_CAMIS:
            if target_item is None or item.get('inspection_date', '') > target_item.get('inspection_date', ''):
                target_item = item
    
    if not target_item:
        logger.error(f"STOP: Could not find CAMIS {TARGET_CAMIS} in the fetched API data.")
        DatabaseManager.close_all_connections()
        return

    logger.info(f"SUCCESS: Found target record to process.")

    try:
        inspection_date = convert_date(target_item.get("inspection_date"))
        restaurant_tuple = (
            target_item.get("camis"), target_item.get("dba"), normalize_search_term_for_hybrid(target_item.get("dba")),
            target_item.get("boro"), target_item.get("building"), target_item.get("street"),
            target_item.get("zipcode"), target_item.get("phone"),
            float(target_item.get("latitude")) if target_item.get("latitude") and target_item.get("latitude") not in ['N/A', ''] else None,
            float(target_item.get("longitude")) if target_item.get("longitude") and target_item.get("longitude") not in ['N/A', ''] else None,
            target_item.get("grade"), inspection_date, target_item.get("critical_flag"),
            target_item.get("inspection_type"), target_item.get("cuisine_description"),
            convert_date(target_item.get("grade_date"))
        )
        violation_tuple = (
            target_item.get("camis"), inspection_date,
            target_item.get("violation_code"), target_item.get("violation_description")
        ) if target_item.get("violation_code") else None
    except Exception as e:
        logger.error(f"STOP: Failed to prepare data tuples: {e}")
        DatabaseManager.close_all_connections()
        return

    # --- NEW EXPLICIT TRANSACTION BLOCK ---
    conn = None
    try:
        conn = DatabaseManager.get_connection()
        conn.autocommit = False # Turn off autocommit for this transaction
        
        with conn.cursor() as cursor:
            logger.info("--> Attempting to insert restaurant record...")
            upsert_sql = """
                INSERT INTO restaurants (camis, dba, dba_normalized_search, boro, building, street, zipcode, phone, latitude, longitude, grade, inspection_date, critical_flag, inspection_type, cuisine_description, grade_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (camis, inspection_date) DO NOTHING;
            """
            cursor.execute(upsert_sql, restaurant_tuple)
            logger.info("--> Restaurant INSERT command executed.")

            if violation_tuple:
                logger.info("--> Attempting to insert violation record...")
                insert_sql = "INSERT INTO violations (camis, inspection_date, violation_code, violation_description) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;"
                cursor.execute(insert_sql, violation_tuple)
                logger.info("--> Violation INSERT command executed.")
        
        logger.info("--> Explicitly committing transaction...")
        conn.commit() # This is the crucial step to save the data
        logger.info("--> Transaction committed successfully.")

    except Exception as e:
        logger.error(f"STOP: DATABASE TRANSACTION FAILED: {e}", exc_info=True)
        if conn: conn.rollback() # Rollback on error
    finally:
        if conn: DatabaseManager.return_connection(conn) # Return connection to the pool
    # ------------------------------------

    # ... Verification step remains the same ...
    logger.info("--- VERIFICATION STEP ---")
    try:
        with DatabaseConnection() as conn, conn.cursor(row_factory=dict_row) as cursor:
            logger.info(f"--> Querying DB for CAMIS {TARGET_CAMIS} to verify insert...")
            verify_sql = "SELECT dba, inspection_date, grade FROM restaurants WHERE camis = %s ORDER BY inspection_date DESC;"
            cursor.execute(verify_sql, (TARGET_CAMIS,))
            results = cursor.fetchall()
            if results and any(r['inspection_date'].strftime('%Y-%m-%d') == inspection_date.strftime('%Y-%m-%d') for r in results):
                logger.info(f"***** SUCCESS! Found a matching record in the DB! *****")
                logger.info(f"All records in DB: {results}")
            else:
                logger.error(f"***** FAILURE! No matching record found in DB after insert attempt. *****")
    except Exception as e:
        logger.error(f"STOP: DATABASE VERIFICATION FAILED: {e}", exc_info=True)
    
    DatabaseManager.close_all_connections()

if __name__ == '__main__':
    find_and_insert_specific_restaurant()
