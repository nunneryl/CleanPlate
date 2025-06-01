# update_database.py - Reverted to version before FTS/Normalization changes

import os
import requests
import logging
import argparse # Was in your original upload
import traceback
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
    class DatabaseConnection: # Dummy
        def __enter__(self): return None
        def __exit__(self, type, value, traceback): pass
    class APIConfig: # Dummy
        API_REQUEST_LIMIT = 1000
        NYC_API_URL = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"
        NYC_API_APP_TOKEN = None

# Get logger instance
logger = logging.getLogger(__name__)
if not logger.hasHandlers(): # Basic logging config if not set by calling app
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


# --- Helper Print Function (from your original) ---
def print_debug(message):
    logger.info(f"---> SCRIPT DEBUG: {message}")

# --- convert_date function (from your original) ---
def convert_date(date_str):
    if not date_str:
        return None
    try:
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.date()
            except ValueError:
                continue
        dt = date_parse(date_str)
        return dt.date()
    except Exception as e:
        logger.warning(f"Could not parse date '{date_str}': {e}")
        return None

# --- fetch_data function (from your original) ---
def fetch_data(days_back=5, max_retries=4): # Original default was 5
    print_debug(f"Entering fetch_data for past {days_back} days...")
    logger.info(f"Fetching data from the NYC API for the past {days_back} days...")
    results = []
    limit = APIConfig.API_REQUEST_LIMIT
    offset = 0
    total_fetched = 0
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days_back)
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    date_filter = f"inspection_date between '{start_date_str}T00:00:00.000' and '{end_date_str}T23:59:59.999'"
    print_debug(f"Date filter: {date_filter}")

    while True:
        base_url = APIConfig.NYC_API_URL
        params = {
            "$limit": limit,
            "$offset": offset,
            "$where": date_filter
        }
        print_debug(f"Fetching URL: {base_url} with params: {params}")
        headers = {}
        if APIConfig.NYC_API_APP_TOKEN:
            headers["X-App-Token"] = APIConfig.NYC_API_APP_TOKEN
        
        data = None # Initialize data for the retry loop
        for attempt in range(max_retries):
            print_debug(f"API fetch attempt {attempt + 1}/{max_retries}...")
            try:
                response = requests.get(base_url, headers=headers, params=params, timeout=60)
                print_debug(f"API response status code: {response.status_code}")
                response.raise_for_status()
                data = response.json()
                if not data:
                    print_debug("API returned no data for this offset.")
                    logger.info("No more data to fetch for this offset.")
                    break # Break from retry loop, will also break outer while loop
                print_debug(f"API fetch successful, got {len(data)} records.")
                results.extend(data)
                total_fetched += len(data)
                logger.info(f"Fetched {len(data)} records, total: {total_fetched}")
                if len(data) < limit:
                    print_debug("Fetched less than limit, assuming end of data.")
                    break # Break from retry loop, will also break outer while loop
                break # Success, exit retry loop
            except requests.exceptions.Timeout:
                 logger.error(f"Network timeout on attempt {attempt + 1}/{max_retries}")
                 print_debug(f"Network timeout on attempt {attempt + 1}/{max_retries}")
                 if attempt < max_retries - 1: logger.info(f"Retrying in 5 seconds..."); import time; time.sleep(5)
                 else: logger.error("Max retries reached after timeout"); break # Break from retry loop
            except requests.exceptions.HTTPError as http_err:
                logger.error(f"HTTP error on attempt {attempt + 1}/{max_retries}: {http_err}")
                print_debug(f"HTTP error on attempt {attempt + 1}/{max_retries}: {http_err}")
                break # Break from retry loop
            except requests.exceptions.RequestException as req_err: # Other network errors
                logger.error(f"Network error on attempt {attempt + 1}/{max_retries}: {req_err}")
                print_debug(f"Network error on attempt {attempt + 1}/{max_retries}: {req_err}")
                if attempt < max_retries - 1: logger.info(f"Retrying in 5 seconds..."); import time; time.sleep(5)
                else: logger.error("Max retries reached after network error"); break # Break from retry loop
            except Exception as e: # Catch other errors like JSONDecodeError
                logger.error(f"Unexpected error during fetch attempt {attempt + 1}/{max_retries}: {e}", exc_info=True)
                print_debug(f"Unexpected error during fetch attempt {attempt + 1}/{max_retries}: {e}")
                if attempt < max_retries - 1: logger.info(f"Retrying in 5 seconds..."); import time; time.sleep(5)
                else: logger.error("Max retries reached after unexpected error"); break # Break from retry loop
        
        if data is None or not data: # Check if loop should break (failed all retries or no data)
            print_debug("Breaking outer fetch loop (no data or fetch failed).")
            break
        offset += limit # Increment offset for next page
        
    logger.info(f"Total records fetched: {total_fetched}")
    print_debug(f"Exiting fetch_data. Total fetched: {total_fetched}")
    return results

# --- update_database_batch function (Original, without dba_normalized/dba_tsv) ---
def update_database_batch(data):
    print_debug("Entering update_database_batch function...")
    if not data:
        logger.info("No data provided to update_database_batch.")
        return 0, 0
    
    logger.info(f"Preparing batch update for {len(data)} fetched records...")
    restaurants_to_upsert = []
    violations_to_insert = []
    processed_restaurant_keys = set()
    
    for i, item in enumerate(data):
        if (i + 1) % 1000 == 0:
            print_debug(f"Preparing record {i + 1}/{len(data)} for batch...")
        try:
            camis = item.get("camis")
            inspection_date = convert_date(item.get("inspection_date"))
            grade_date = convert_date(item.get("grade_date"))
            latitude_val = item.get("latitude")
            longitude_val = item.get("longitude")
            restaurant_key = (camis, inspection_date)

            if not (camis and inspection_date): # Basic validation
                logger.warning(f"Skipping record due to missing CAMIS or inspection_date: {item}")
                continue

            if restaurant_key not in processed_restaurant_keys:
                restaurant_tuple = (
                    camis, item.get("dba"), item.get("boro"), item.get("building"), item.get("street"),
                    item.get("zipcode"), item.get("phone"),
                    float(latitude_val) if latitude_val and latitude_val not in ['N/A', ''] else None,
                    float(longitude_val) if longitude_val and longitude_val not in ['N/A', ''] else None,
                    item.get("grade"), inspection_date, item.get("critical_flag"), item.get("inspection_type"),
                    item.get("cuisine_description"), grade_date
                )
                restaurants_to_upsert.append(restaurant_tuple)
                processed_restaurant_keys.add(restaurant_key)

            violation_code = item.get("violation_code")
            if violation_code: # Only add if there's a violation code
                 violation_tuple = ( camis, inspection_date, violation_code, item.get("violation_description") )
                 violations_to_insert.append(violation_tuple)
        except Exception as e:
            logger.error(f"Error preparing record CAMIS={item.get('camis','N/A')}, InspDate={item.get('inspection_date','N/A')} for batch: {e}", exc_info=True)
            continue # Skip problematic record
            
    print_debug(f"Prepared {len(restaurants_to_upsert)} unique restaurant records for upsert.")
    print_debug(f"Prepared {len(violations_to_insert)} violation records for insert.")
    
    conn = None
    success = False
    restaurants_updated_count = 0
    violations_inserted_count = 0
    
    try:
        print_debug("Attempting to get DB connection for batch operations...")
        with DatabaseConnection() as conn:
            print_debug("DB connection acquired successfully.")
            with conn.cursor() as cursor:
                print_debug("DB cursor acquired.")
                
                if restaurants_to_upsert:
                    logger.info(f"Executing batch upsert for {len(restaurants_to_upsert)} restaurants...")
                    upsert_sql = """
                        INSERT INTO restaurants ( camis, dba, boro, building, street, zipcode, phone,
                            latitude, longitude, grade, inspection_date, critical_flag,
                            inspection_type, cuisine_description, grade_date )
                        VALUES %s
                        ON CONFLICT (camis, inspection_date) DO UPDATE SET
                            dba = EXCLUDED.dba, boro = EXCLUDED.boro, building = EXCLUDED.building, street = EXCLUDED.street,
                            zipcode = EXCLUDED.zipcode, phone = EXCLUDED.phone, latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude,
                            grade = EXCLUDED.grade, critical_flag = EXCLUDED.critical_flag, inspection_type = EXCLUDED.inspection_type,
                            cuisine_description = EXCLUDED.cuisine_description, grade_date = EXCLUDED.grade_date; 
                    """
                    psycopg2.extras.execute_values( cursor, upsert_sql, restaurants_to_upsert, template=None, page_size=100)
                    restaurants_updated_count = cursor.rowcount if cursor.rowcount != -1 else len(restaurants_to_upsert)
                    logger.info(f"Batch restaurant upsert executed. Affected rows (approx): {restaurants_updated_count}")

                if violations_to_insert:
                    unique_violations = list(set(violations_to_insert)) # Remove duplicates before inserting
                    logger.info(f"Executing batch insert for {len(unique_violations)} unique violations...")
                    insert_sql = """ INSERT INTO violations ( camis, inspection_date, violation_code, violation_description ) VALUES %s ON CONFLICT DO NOTHING; """
                    psycopg2.extras.execute_values( cursor, insert_sql, unique_violations, template=None, page_size=1000)
                    violations_inserted_count = cursor.rowcount if cursor.rowcount != -1 else len(unique_violations)
                    logger.info(f"Batch violation insert executed. Affected rows (approx): {violations_inserted_count}")
                
                logger.info("Attempting to commit batch transaction...")
                conn.commit()
                logger.info("Database transaction committed successfully!")
                success = True
                
    except psycopg2.Error as db_err:
        logger.error(f"Database Error during batch update: {db_err}", exc_info=True)
        if conn:
            try: conn.rollback(); logger.info("Database transaction rolled back.")
            except Exception as rb_e: logger.error(f"Error during rollback: {rb_e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error during batch database update: {e}", exc_info=True)
        if conn:
            try: conn.rollback(); logger.info("Database transaction rolled back.")
            except Exception as rb_e: logger.error(f"Error during rollback: {rb_e}", exc_info=True)
    finally:
        print_debug("Exiting update_database_batch function (finally block).")

    if success:
        logger.info(f"Batch database update finished. Restaurants processed (approx): {restaurants_updated_count}, Violations processed (approx): {violations_inserted_count}.")
        return restaurants_updated_count, violations_inserted_count
    else:
        logger.error("Batch database update failed.")
        return 0, 0

# --- run_database_update function (entry point called by Flask) ---
def run_database_update(days_back=2): # Original file had days_back=2 default here
    print_debug(f"--- run_database_update called (days_back={days_back}) ---")
    logger.info("Starting database update process via run_database_update")
    try:
        logger.info(f"Performing incremental update for past {days_back} days...")
        data = fetch_data(days_back=days_back)
        if data:
            restaurants_updated, violations_inserted = update_database_batch(data)
            logger.info(f"run_database_update: Batch update processed. Restaurants: {restaurants_updated}, Violations: {violations_inserted}")
        else:
            logger.warning("run_database_update: No data fetched from API")

    except Exception as e:
        print_debug(f"FATAL: Uncaught exception in run_database_update: {e}")
        logger.critical(f"Uncaught exception in run_database_update: {e}", exc_info=True)
        # import sentry_sdk; sentry_sdk.capture_exception(e) # If Sentry is used
    finally:
        logger.info("Database update process via run_database_update finished.")
        print_debug("--- run_database_update finished ---")

# --- Main Execution Block (from your original file for direct execution) ---
# if __name__ == '__main__':
#    (Your original __main__ block for direct execution, if any, would go here)
#    (The version you uploaded earlier had this removed, so I'm keeping it that way)
#    pass

