# update_database.py - Updated with more granular logging

import os
import requests
import logging
import argparse
import traceback
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
from dateutil.parser import parse as date_parse
from db_manager import DatabaseConnection # Assuming DatabaseConnection handles pool init/get/return
from config import APIConfig

# Get logger instance (will inherit config from app_search.py)
logger = logging.getLogger(__name__)

# --- Helper Print Function (Optional but kept for debug visibility) ---
def print_debug(message):
    """Helper function to print debug messages clearly."""
    # Using logger.info directly for consistency with other logs
    logger.info(f"---> SCRIPT DEBUG: {message}") # Changed prefix for clarity

# --- convert_date function (no changes needed) ---
def convert_date(date_str):
    """Convert date string to date object"""
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

# --- fetch_data function (no changes needed, uses logger) ---
def fetch_data(days_back=2, max_retries=3):
    """Fetch data from NYC API with pagination"""
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
        data = None
        for attempt in range(max_retries):
            print_debug(f"API fetch attempt {attempt + 1}/{max_retries}...")
            try:
                response = requests.get(base_url, headers=headers, params=params, timeout=60) # request timeout
                print_debug(f"API response status code: {response.status_code}")
                response.raise_for_status()
                data = response.json()
                if not data:
                    print_debug("API returned no data for this offset.")
                    logger.info("No more data to fetch for this offset.")
                    break 
                print_debug(f"API fetch successful, got {len(data)} records.")
                results.extend(data)
                total_fetched += len(data)
                logger.info(f"Fetched {len(data)} records, total: {total_fetched}")
                if len(data) < limit:
                    print_debug("Fetched less than limit, assuming end of data.")
                    break 
                break # Success, exit retry loop
            except requests.exceptions.Timeout:
                 logger.error(f"Network timeout on attempt {attempt + 1}/{max_retries}")
                 print_debug(f"Network timeout on attempt {attempt + 1}/{max_retries}")
                 if attempt < max_retries - 1: logger.info(f"Retrying in 5 seconds..."); import time; time.sleep(5)
                 else: logger.error("Max retries reached after timeout"); break
            except requests.exceptions.HTTPError as http_err:
                logger.error(f"HTTP error on attempt {attempt + 1}/{max_retries}: {http_err}")
                print_debug(f"HTTP error on attempt {attempt + 1}/{max_retries}: {http_err}")
                break
            except requests.exceptions.RequestException as req_err:
                logger.error(f"Network error on attempt {attempt + 1}/{max_retries}: {req_err}")
                print_debug(f"Network error on attempt {attempt + 1}/{max_retries}: {req_err}")
                if attempt < max_retries - 1: logger.info(f"Retrying in 5 seconds..."); import time; time.sleep(5)
                else: logger.error("Max retries reached after network error"); break
            except Exception as e:
                logger.error(f"Unexpected error during fetch attempt {attempt + 1}/{max_retries}: {e}", exc_info=True) # <<< ADDED exc_info
                print_debug(f"Unexpected error during fetch attempt {attempt + 1}/{max_retries}: {e}")
                # logger.error(traceback.format_exc()) # Already logged with exc_info
                if attempt < max_retries - 1: logger.info(f"Retrying in 5 seconds..."); import time; time.sleep(5)
                else: logger.error("Max retries reached after unexpected error"); break
        if data is None or not data: # Check if loop should break
            print_debug("Breaking outer fetch loop (no data or fetch failed).")
            break
        offset += limit
    logger.info(f"Total records fetched: {total_fetched}")
    print_debug(f"Exiting fetch_data. Total fetched: {total_fetched}")
    return results

# --- fetch_all_data function (no changes needed, uses logger) ---
def fetch_all_data(max_retries=3):
    print_debug("Entering fetch_all_data...")
    logger.info("Fetching ALL data from the NYC API...")
    results = []
    limit = APIConfig.API_REQUEST_LIMIT 
    offset = 0
    total_fetched = 0
    while True:
        base_url = APIConfig.NYC_API_URL
        params = {"$limit": limit, "$offset": offset}
        print_debug(f"Fetching URL: {base_url} with params: {params}")
        headers = {}
        if APIConfig.NYC_API_APP_TOKEN: headers["X-App-Token"] = APIConfig.NYC_API_APP_TOKEN
        data = None
        for attempt in range(max_retries):
            print_debug(f"API fetch attempt {attempt + 1}/{max_retries}...")
            try:
                response = requests.get(base_url, headers=headers, params=params, timeout=120) # Longer timeout for full sync
                print_debug(f"API response status code: {response.status_code}")
                response.raise_for_status()
                data = response.json()
                if not data: print_debug("API returned no data for this offset."); logger.info("No more data to fetch"); break
                print_debug(f"API fetch successful, got {len(data)} records.")
                results.extend(data)
                total_fetched += len(data)
                logger.info(f"Fetched {len(data)} records, total: {total_fetched}")
                if len(data) < limit: print_debug("Fetched less than limit, assuming end of data."); break
                break
            except requests.exceptions.Timeout:
                 logger.error(f"Network timeout on attempt {attempt + 1}/{max_retries}"); print_debug(f"Network timeout attempt {attempt + 1}/{max_retries}")
                 if attempt < max_retries - 1: logger.info(f"Retrying in 10 seconds..."); import time; time.sleep(10)
                 else: logger.error("Max retries reached after timeout"); break
            except requests.exceptions.HTTPError as http_err:
                 logger.error(f"HTTP error on attempt {attempt + 1}/{max_retries}: {http_err}"); print_debug(f"HTTP error attempt {attempt + 1}/{max_retries}: {http_err}"); break
            except requests.exceptions.RequestException as e:
                 logger.error(f"Network error on attempt {attempt + 1}/{max_retries}: {e}"); print_debug(f"Network error attempt {attempt + 1}/{max_retries}: {e}")
                 if attempt < max_retries - 1: logger.info(f"Retrying in 10 seconds..."); import time; time.sleep(10)
                 else: logger.error("Max retries reached after network error"); break
            except Exception as e:
                 logger.error(f"Unexpected error during fetch attempt {attempt + 1}/{max_retries}: {e}", exc_info=True); print_debug(f"Unexpected error attempt {attempt + 1}/{max_retries}: {e}") # <<< ADDED exc_info
                 # logger.error(traceback.format_exc()) # Already logged with exc_info
                 if attempt < max_retries - 1: logger.info(f"Retrying in 10 seconds..."); import time; time.sleep(10)
                 else: logger.error("Max retries reached after unexpected error"); break
        if data is None or not data: print_debug("Breaking outer fetch loop (no data or fetch failed)."); break
        offset += limit
    logger.info(f"Total records fetched: {total_fetched}")
    print_debug(f"Exiting fetch_all_data. Total fetched: {total_fetched}")
    return results


# --- fetch_restaurant_by_camis function (no changes needed, uses logger) ---
def fetch_restaurant_by_camis(camis, max_retries=3):
    print_debug(f"Entering fetch_restaurant_by_camis for CAMIS: {camis}")
    logger.info(f"Fetching all inspections for restaurant CAMIS: {camis}")
    # ... (rest of the function is fine, uses logger and print_debug) ...
    # Ensure any Exception logging uses exc_info=True if not already
    # Example from original: logger.error(f"Error fetching restaurant {camis} on attempt {attempt + 1}/{max_retries}: {e}"); print_debug(f"Error CAMIS {camis} attempt {attempt + 1}/{max_retries}: {e}"); logger.error(traceback.format_exc())
    # Should be: logger.error(f"Error fetching restaurant {camis} on attempt {attempt + 1}/{max_retries}: {e}", exc_info=True); print_debug(f"Error CAMIS {camis} attempt {attempt + 1}/{max_retries}: {e}")
    # (This pattern should be applied to all generic exception handlers)
    results = [] # Placeholder for brevity, keep your original logic
    limit = 1000
    base_url = APIConfig.NYC_API_URL
    params = {"$limit": limit, "$where": f"camis='{camis}'"}
    print_debug(f"Fetching URL: {base_url} with params: {params}")
    headers = {}
    if APIConfig.NYC_API_APP_TOKEN: headers["X-App-Token"] = APIConfig.NYC_API_APP_TOKEN
    for attempt in range(max_retries):
        print_debug(f"API fetch attempt {attempt + 1}/{max_retries}...")
        try:
            response = requests.get(base_url, headers=headers, params=params, timeout=30)
            print_debug(f"API response status code: {response.status_code}")
            response.raise_for_status()
            data = response.json()
            logger.info(f"Fetched {len(data)} inspections for restaurant CAMIS: {camis}")
            print_debug(f"Exiting fetch_restaurant_by_camis successfully.")
            return data
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error fetching CAMIS {camis} on attempt {attempt + 1}/{max_retries}: {http_err}"); print_debug(f"HTTP error CAMIS {camis} attempt {attempt + 1}/{max_retries}: {http_err}")
            if response.status_code == 404: logger.warning(f"CAMIS {camis} not found (404)."); return []
            if attempt < max_retries - 1: logger.info(f"Retrying attempt {attempt + 2}/{max_retries}..."); import time; time.sleep(5)
            else: logger.error(f"Max retries reached for CAMIS {camis} after HTTP error."); return []
        except Exception as e: # Generic exception
            logger.error(f"Error fetching restaurant {camis} on attempt {attempt + 1}/{max_retries}: {e}", exc_info=True); print_debug(f"Error CAMIS {camis} attempt {attempt + 1}/{max_retries}: {e}") # <<< MODIFIED LOGGING
            if attempt < max_retries - 1: logger.info(f"Retrying in 5 seconds..."); import time; time.sleep(5)
            else: logger.error("Max retries reached, giving up"); print_debug(f"Exiting fetch_restaurant_by_camis after max retries."); return []
    print_debug(f"Exiting fetch_restaurant_by_camis - loop finished unexpectedly.")
    return []


# --- update_database_batch function (MODIFIED WITH MORE LOGGING) ---
def update_database_batch(data):
    """Update database with fetched data using batch operations"""
    print_debug("Entering update_database_batch function...")
    if not data: 
        logger.info("No data provided to update_database_batch.")
        print_debug("No data, exiting update_database_batch.")
        return 0, 0
    
    logger.info(f"Preparing batch update for {len(data)} fetched records...")
    restaurants_to_upsert = []
    violations_to_insert = []
    processed_restaurant_keys = set()
    print_debug("Processing fetched data into lists for batch execution...")
    
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

            if camis and inspection_date and restaurant_key not in processed_restaurant_keys:
                restaurant_tuple = (
                    camis, item.get("dba"), item.get("boro"), item.get("building"), item.get("street"),
                    item.get("zipcode"), item.get("phone"),
                    float(latitude_val) if latitude_val and latitude_val != 'N/A' else None,
                    float(longitude_val) if longitude_val and longitude_val != 'N/A' else None,
                    item.get("grade"), inspection_date, item.get("critical_flag"), item.get("inspection_type"),
                    item.get("cuisine_description"), grade_date 
                )
                restaurants_to_upsert.append(restaurant_tuple)
                processed_restaurant_keys.add(restaurant_key)

            violation_code = item.get("violation_code")
            if camis and inspection_date and violation_code:
                 violation_tuple = ( camis, inspection_date, violation_code, item.get("violation_description") )
                 violations_to_insert.append(violation_tuple)
        except Exception as e:
            logger.error(f"Error preparing record CAMIS={item.get('camis')}, InspDate={item.get('inspection_date')} for batch: {e}", exc_info=True) # <<< ADDED exc_info
            print_debug(f"ERROR preparing record CAMIS={item.get('camis')} for batch: {e}")
            continue
            
    print_debug(f"Prepared {len(restaurants_to_upsert)} unique restaurant records for upsert.")
    print_debug(f"Prepared {len(violations_to_insert)} violation records for insert.")
    
    conn = None
    success = False
    cursor = None # <<< ADDED: Initialize cursor to None for finally block
    
    try:
        print_debug("Attempting to get DB connection for batch operations...")
        # Assuming DatabaseConnection() is a context manager that handles get/release
        with DatabaseConnection() as conn: 
            print_debug("DB connection acquired successfully.")
            with conn.cursor() as cursor:
                print_debug("DB cursor acquired.")
                
                if restaurants_to_upsert:
                    logger.info(f"--- SCRIPT DEBUG: Executing batch upsert for {len(restaurants_to_upsert)} restaurants... ---") # <<< MODIFIED LOGGING (was print_debug)
                    upsert_sql = """
                        INSERT INTO restaurants ( camis, dba, boro, building, street, zipcode, phone,
                            latitude, longitude, grade, inspection_date, critical_flag,
                            inspection_type, cuisine_description, grade_date )
                        VALUES %s
                        ON CONFLICT (camis, inspection_date) DO UPDATE SET
                            dba = EXCLUDED.dba, boro = EXCLUDED.boro, building = EXCLUDED.building, street = EXCLUDED.street,
                            zipcode = EXCLUDED.zipcode, phone = EXCLUDED.phone, latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude,
                            grade = EXCLUDED.grade, critical_flag = EXCLUDED.critical_flag, inspection_type = EXCLUDED.inspection_type,
                            cuisine_description = EXCLUDED.cuisine_description, grade_date = EXCLUDED.grade_date; """
                    psycopg2.extras.execute_values( cursor, upsert_sql, restaurants_to_upsert, template=None, page_size=100)
                    logger.info(f"--- SCRIPT DEBUG: Batch restaurant upsert executed. ---") # <<< MODIFIED LOGGING (was print_debug)

                if violations_to_insert:
                    logger.info(f"--- SCRIPT DEBUG: Preparing to insert {len(violations_to_insert)} violation records... ---") # <<< ADDED LOGGING
                    logger.info(f"--- SCRIPT DEBUG: Executing batch insert for {len(violations_to_insert)} violations... ---") # <<< MODIFIED LOGGING (was print_debug)
                    insert_sql = """ INSERT INTO violations ( camis, inspection_date, violation_code, violation_description ) VALUES %s ON CONFLICT DO NOTHING; """
                    psycopg2.extras.execute_values( cursor, insert_sql, violations_to_insert, template=None, page_size=100 )
                    logger.info(f"--- SCRIPT DEBUG: Batch violation insert executed. ---") # <<< MODIFIED LOGGING (was print_debug)
                
                logger.info("--- SCRIPT DEBUG: Attempting to commit batch transaction... ---") # <<< MODIFIED LOGGING (was print_debug)
                conn.commit()
                logger.info("--- SCRIPT DEBUG: Database transaction committed successfully! ---") # <<< ADDED LOGGING
                success = True
                
    except psycopg2.Error as db_err:
        logger.error(f"Database Error during batch update: {db_err}", exc_info=True) # <<< ADDED exc_info
        print_debug(f"FATAL: Database Error during batch update: {db_err}")
        # logger.error(traceback.format_exc()) # Already logged with exc_info
        if conn: 
            try:
                conn.rollback()
                print_debug("Database transaction rolled back due to psycopg2.Error.")
                logger.info("--- SCRIPT DEBUG: Database transaction rolled back due to psycopg2.Error. ---") # <<< ADDED LOGGING
            except Exception as rb_e:
                logger.error(f"--- SCRIPT DEBUG: Error during rollback after psycopg2.Error: {rb_e} ---", exc_info=True) # <<< ADDED LOGGING
    except Exception as e:
        logger.error(f"Unexpected error during batch database update: {e}", exc_info=True) # <<< ADDED exc_info
        print_debug(f"FATAL: Unexpected error during batch database update: {e}")
        # logger.error(traceback.format_exc()) # Already logged with exc_info
        if conn: 
            try:
                conn.rollback()
                print_debug("Database transaction rolled back due to unexpected error.")
                logger.info("--- SCRIPT DEBUG: Database transaction rolled back due to unexpected error. ---") # <<< ADDED LOGGING
            except Exception as rb_e:
                logger.error(f"--- SCRIPT DEBUG: Error during rollback after unexpected error: {rb_e} ---", exc_info=True) # <<< ADDED LOGGING
    finally:
        # Cursor is managed by 'with conn.cursor() as cursor:', so it's closed automatically.
        # Connection is managed by 'with DatabaseConnection() as conn:', so it's released/closed automatically.
        # Adding logs to confirm this understanding or if explicit close is needed by your DatabaseConnection context manager
        if cursor and not cursor.closed:
             logger.info("--- SCRIPT DEBUG: Cursor was not automatically closed in finally, attempting close. ---") # Should not happen with 'with'
             # cursor.close() # Typically not needed if 'with' statement for cursor is used correctly
        if conn: # Check if conn was assigned (it should be if 'with' was entered)
             logger.info("--- SCRIPT DEBUG: DatabaseConnection context manager should handle connection release/close. ---")

        print_debug("Exiting update_database_batch function (finally block).")

    if success:
        logger.info(f"Batch database update finished. Processed approx {len(restaurants_to_upsert)} restaurants and {len(violations_to_insert)} violations.")
        print_debug(f"Batch database update finished successfully.")
        return len(restaurants_to_upsert), len(violations_to_insert)
    else:
        logger.error("Batch database update failed.")
        print_debug("Batch database update failed.")
        return 0, 0

# --- update_specific_restaurants function (no changes needed, uses logger) ---
def update_specific_restaurants(camis_list):
    print_debug(f"Entering update_specific_restaurants for {len(camis_list)} CAMIS IDs.")
    logger.info(f"Updating {len(camis_list)} specific restaurants...")
    all_inspection_data = []
    fetch_errors = 0
    for i, camis in enumerate(camis_list):
        print_debug(f"Processing CAMIS {i+1}/{len(camis_list)}: {camis}")
        data = fetch_restaurant_by_camis(camis)
        if data: all_inspection_data.extend(data)
        else: fetch_errors += 1; print_debug(f"No data fetched for CAMIS {camis}")
    logger.info(f"Finished fetching. Total records: {len(all_inspection_data)}. Fetch errors: {fetch_errors}")
    if all_inspection_data: 
        update_database_batch(all_inspection_data)
    else: 
        logger.warning("No data collected to update for specific restaurants.")
    logger.info(f"Specific restaurant update process complete. Processed {len(camis_list)} IDs.")
    print_debug(f"Exiting update_specific_restaurants.")
    return -1, -1 # Consider returning actual counts or success status


# --- run_database_update function (entry point called by Flask) ---
def run_database_update(days_back=2):
    """Main entry point for running the update logic, called from Flask."""
    print_debug(f"--- run_database_update called (days_back={days_back}) ---")
    logger.info("Starting database update process via run_database_update")
    try:
        logger.info(f"Performing incremental update for past {days_back} days...")
        data = fetch_data(days_back=days_back)
        if data:
            restaurants_updated, violations_inserted = update_database_batch(data) # Use batch update
            logger.info(f"run_database_update: Batch update processed. Restaurants: {restaurants_updated}, Violations: {violations_inserted}")
        else:
            logger.warning("run_database_update: No data fetched from API")

    except Exception as e:
        print_debug(f"FATAL: Uncaught exception in run_database_update: {e}")
        logger.critical(f"Uncaught exception in run_database_update: {e}", exc_info=True) # <<< ADDED exc_info
        # logger.critical(traceback.format_exc()) # Already logged with exc_info
        # Optionally notify Sentry if not automatically captured from critical log
        # import sentry_sdk
        # sentry_sdk.capture_exception(e)
    finally:
        logger.info("Database update process via run_database_update finished.")
        print_debug("--- run_database_update finished ---")

# --- REMOVED: if __name__ == '__main__' block and old main() function ---
