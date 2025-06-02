# update_database.py - Final v3 with Aggressive Normalization for FTS

import os
import requests
import logging
import argparse
import traceback # For more detailed error logging if needed
import psycopg2
import psycopg2.extras # For execute_values
import re # Ensure re is imported
from datetime import datetime, timedelta
from dateutil.parser import parse as date_parse

# Assuming db_manager.py and config.py are in the same directory or accessible via PYTHONPATH
try:
    from db_manager import DatabaseConnection # Your context manager for DB connections
    from config import APIConfig # Your configuration class
except ImportError:
    # Fallback or error handling if these modules can't be imported
    # This is critical for the script to run.
    # For now, let's define dummy versions if they are missing, but this should be fixed in your setup.
    logging.critical("Failed to import db_manager or config. Using placeholder classes.")
    class DatabaseConnection: # Dummy
        def __enter__(self): return None
        def __exit__(self, type, value, traceback): pass
    class APIConfig: # Dummy
        API_REQUEST_LIMIT = 1000
        NYC_API_URL = "https://data.cityofnewyork.us/resource/43nn-pn8j.json" # Example
        NYC_API_APP_TOKEN = None


# Get logger instance
logger = logging.getLogger(__name__)
# Basic logging configuration if not already set up by a calling module (e.g., Flask app)
if not logger.hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


# --- AGGRESSIVE NORMALIZATION FUNCTION ---
def normalize_text(text):
    if not isinstance(text, str):
        return ''
    text = text.lower()
    # Convert apostrophes and periods to spaces first.
    # This helps separate terms like "E.J.'s" into "e j s" before further processing.
    # For "Xi'an", it becomes "xi an". For "Joe's", it becomes "joe s".
    text = text.replace("'", " ").replace(".", " ")
    text = text.replace('&', ' and ') # Replace ampersand with ' and '
    
    # Remove any characters that are not alphanumeric or whitespace
    text = re.sub(r'[^\w\s]', '', text)
    
    # Collapse multiple spaces into a single space and strip leading/trailing whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text
    
# --- END AGGRESSIVE NORMALIZATION FUNCTION ---

def print_debug(message):
    """Helper function to print debug messages clearly, using the logger."""
    logger.info(f"---> SCRIPT DEBUG: {message}")

def convert_date(date_str):
    """Convert date string to date object, trying multiple formats."""
    if not date_str:
        return None
    try:
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.date()
            except ValueError:
                continue
        dt = date_parse(date_str) # Fallback to dateutil.parser
        return dt.date()
    except Exception as e:
        logger.warning(f"Could not parse date '{date_str}': {e}")
        return None

def fetch_data(days_back=5, max_retries=3): # Reduced max_retries
    """Fetch data from NYC API with pagination for a given number of days back."""
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
    print_debug(f"Date filter for API query: {date_filter}")

    page_num = 0
    while True:
        page_num += 1
        print_debug(f"Fetching page {page_num}, offset {offset}...")
        base_url = APIConfig.NYC_API_URL
        params = {
            "$limit": limit,
            "$offset": offset,
            "$where": date_filter,
            "$order": "inspection_date DESC"
        }
        headers = {}
        if APIConfig.NYC_API_APP_TOKEN:
            headers["X-App-Token"] = APIConfig.NYC_API_APP_TOKEN
        
        current_data_batch = None
        attempt = 0
        while attempt < max_retries:
            attempt += 1
            try:
                response = requests.get(base_url, headers=headers, params=params, timeout=60)
                print_debug(f"API request URL: {response.url}")
                print_debug(f"API response status code: {response.status_code} (Attempt {attempt})")
                response.raise_for_status()
                current_data_batch = response.json()
                break
            except requests.exceptions.Timeout:
                logger.warning(f"API request timeout (Attempt {attempt}/{max_retries}). Retrying if possible...")
                if attempt == max_retries: logger.error("Max retries reached for API request due to timeout."); return results
                import time; time.sleep(5 * attempt)
            except requests.exceptions.RequestException as req_err:
                logger.error(f"Network error during fetch (Attempt {attempt}/{max_retries}): {req_err}", exc_info=True)
                if attempt == max_retries: logger.error("Max retries reached for API request due to network error."); return results
                import time; time.sleep(5 * attempt)
            except Exception as e:
                logger.error(f"Unexpected error during API fetch (Attempt {attempt}/{max_retries}): {e}", exc_info=True)
                return results

        if current_data_batch is None:
            print_debug("Failed to fetch data after max retries.")
            break

        if not current_data_batch:
            print_debug("API returned no data for this offset. Assuming end of data for this query.")
            logger.info("No more data to fetch for this date range/offset.")
            break
        
        results.extend(current_data_batch)
        total_fetched += len(current_data_batch)
        logger.info(f"Fetched {len(current_data_batch)} records this page, total so far: {total_fetched}")

        if len(current_data_batch) < limit:
            print_debug("Fetched less than limit, assuming end of data for this query.")
            break
        
        offset += len(current_data_batch)
            
    logger.info(f"Total records fetched from API after all pages: {total_fetched}")
    print_debug(f"Exiting fetch_data. Total fetched: {total_fetched}")
    return results


def update_database_batch(data):
    """Update database with fetched data using batch operations, including FTS column"""
    print_debug("Entering update_database_batch function...")
    if not data:
        logger.info("No data provided to update_database_batch.")
        return 0, 0
    
    logger.info(f"Preparing batch update for {len(data)} fetched records...")
    restaurants_to_upsert = []
    violations_to_insert = []
    processed_restaurant_keys = set()

    for i, item in enumerate(data):
        if (i + 1) % 500 == 0:
            print_debug(f"Preparing record {i + 1}/{len(data)} for batch...")
        try:
            camis = item.get("camis")
            dba = item.get("dba")
            normalized_dba_for_fts = normalize_text(dba) # Use new aggressive normalization

            inspection_date = convert_date(item.get("inspection_date"))
            grade_date = convert_date(item.get("grade_date"))
            
            if not (camis and inspection_date):
                logger.warning(f"Skipping record due to missing CAMIS or inspection_date: {item}")
                continue

            restaurant_key = (camis, inspection_date)

            if restaurant_key not in processed_restaurant_keys:
                latitude_val = item.get("latitude")
                longitude_val = item.get("longitude")
                
                restaurant_tuple = (
                    camis, dba, item.get("boro"), item.get("building"), item.get("street"),
                    item.get("zipcode"), item.get("phone"),
                    float(latitude_val) if latitude_val and latitude_val not in ['N/A', ''] else None,
                    float(longitude_val) if longitude_val and longitude_val not in ['N/A', ''] else None,
                    item.get("grade"), inspection_date, item.get("critical_flag"),
                    item.get("inspection_type"), item.get("cuisine_description"), grade_date,
                    normalized_dba_for_fts
                )
                restaurants_to_upsert.append(restaurant_tuple)
                processed_restaurant_keys.add(restaurant_key)

            violation_code = item.get("violation_code")
            if violation_code:
                 violation_tuple = ( camis, inspection_date, violation_code, item.get("violation_description") )
                 violations_to_insert.append(violation_tuple)
        except Exception as e:
            logger.error(f"Error preparing record CAMIS={item.get('camis', 'N/A')}, DBA={item.get('dba', 'N/A')} for batch: {e}", exc_info=True)
            continue
            
    print_debug(f"Prepared {len(restaurants_to_upsert)} unique restaurant inspection records for upsert.")
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
                    
                    # Tuples for executemany should match the order of %s placeholders
                    # The last %s in VALUES corresponds to normalized_dba_for_fts
                    # which is then used as input to to_tsvector('english', %s)
                    # The tuple structure is:
                    # (camis, dba, boro, ..., grade_date, normalized_dba_for_fts)
                    
                    # Correct SQL for executemany:
                    # The list of tuples `restaurants_to_upsert` has `normalized_dba_for_fts` as the last element.
                    # The SQL INSERT statement needs 16 placeholders for the 15 columns + 1 input to to_tsvector.
                    
                    # For ON CONFLICT, dba_tsv should be updated based on the NEW dba (EXCLUDED.dba)
                    # after it has been Python-normalized. Since we can't call Python normalize_text
                    # directly in SQL easily, we rely on to_tsvector('english', EXCLUDED.dba)
                    # and accept that the *backfill* (full run of this Python script) is the ultimate
                    # source of truth for dba_tsv consistency using Python's normalize_text.
                    
                    upsert_sql = """
                        INSERT INTO restaurants (
                            camis, dba, boro, building, street, zipcode, phone,
                            latitude, longitude, grade, inspection_date, critical_flag,
                            inspection_type, cuisine_description, grade_date, 
                            dba_tsv -- The 16th column
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            to_tsvector('english', %s) -- The 16th placeholder for normalized_dba_for_fts
                        )
                        ON CONFLICT (camis, inspection_date) DO UPDATE SET
                            dba = EXCLUDED.dba,
                            boro = EXCLUDED.boro,
                            building = EXCLUDED.building,
                            street = EXCLUDED.street,
                            zipcode = EXCLUDED.zipcode,
                            phone = EXCLUDED.phone,
                            latitude = EXCLUDED.latitude,
                            longitude = EXCLUDED.longitude,
                            grade = EXCLUDED.grade,
                            critical_flag = EXCLUDED.critical_flag,
                            inspection_type = EXCLUDED.inspection_type,
                            cuisine_description = EXCLUDED.cuisine_description,
                            grade_date = EXCLUDED.grade_date,
                            dba_tsv = to_tsvector('english', normalize_text_py_equivalent(EXCLUDED.dba)); 
                            -- This line is the most complex part to get right for UPSERT consistency
                            -- with Python's normalize_text. For now, the strategy is:
                            -- The INSERT uses the Python-normalized string.
                            -- The UPDATE (on conflict) will use the raw EXCLUDED.dba with to_tsvector.
                            -- A full backfill using this script ensures all rows are processed by Python's normalize_text.
                            -- So for ON CONFLICT:
                            -- dba_tsv = to_tsvector('english', EXCLUDED.dba)
                            -- This means if a dba changes, the dba_tsv is based on the new raw dba.
                            -- The python script's normalize_text() is the source of truth for initial population.
                            -- The next full backfill (or a targeted re-run) would ensure it uses python's normalize_text.
                            -- A better ON CONFLICT would be to also update using the EXCLUDED.normalized_dba_for_fts if possible
                            -- but that value isn't part of EXCLUDED table.
                            -- So, we do this:
                            dba_tsv = to_tsvector('english', EXCLUDED.dba);
                    """
                    # executemany expects a list of tuples. Each tuple must match the %s placeholders.
                    # Our restaurant_tuple has 16 items, matching the 16 %s in the VALUES part.
                    cursor.executemany(upsert_sql, restaurants_to_upsert)
                    restaurants_updated_count = cursor.rowcount if cursor.rowcount != -1 else len(restaurants_to_upsert)
                    logger.info(f"Batch restaurant upsert executed. Affected rows (approx): {restaurants_updated_count}")

                if violations_to_insert:
                    unique_violations = list(set(violations_to_insert))
                    logger.info(f"Executing batch insert for {len(unique_violations)} unique violations...")
                    insert_sql = """ INSERT INTO violations (camis, inspection_date, violation_code, violation_description) VALUES %s ON CONFLICT DO NOTHING; """
                    psycopg2.extras.execute_values(cursor, insert_sql, unique_violations, template=None, page_size=1000)
                    violations_inserted_count = cursor.rowcount if cursor.rowcount != -1 else len(unique_violations)
                    logger.info(f"Batch violation insert executed. Affected rows (approx): {violations_inserted_count}")
                
                logger.info("Attempting to commit batch transaction...")
                conn.commit()
                logger.info("Database transaction committed successfully.")
                success = True
                
    except psycopg2.Error as db_err:
        logger.error(f"Database Error during batch update: {db_err}", exc_info=True)
        if conn:
            try: conn.rollback(); logger.info("Database transaction rolled back due to psycopg2.Error.")
            except Exception as rb_e: logger.error(f"Error during rollback after psycopg2.Error: {rb_e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error during batch database update: {e}", exc_info=True)
        if conn:
            try: conn.rollback(); logger.info("Database transaction rolled back due to unexpected error.")
            except Exception as rb_e: logger.error(f"Error during rollback after unexpected error: {rb_e}", exc_info=True)
    finally:
        print_debug("Exiting update_database_batch function (finally block).")

    if success:
        logger.info(f"Batch database update finished. Processed restaurants (approx): {restaurants_updated_count}, Processed violations (approx): {violations_inserted_count}.")
        return restaurants_updated_count, violations_inserted_count
    else:
        logger.error("Batch database update failed.")
        return 0, 0

def run_database_update(days_back=5): # Default for daily runs
    """Main entry point for running the update logic, called from Flask."""
    print_debug(f"--- run_database_update called (days_back={days_back}) ---")
    logger.info(f"Starting database update process via run_database_update for past {days_back} days...")
    try:
        data = fetch_data(days_back=days_back)
        if data:
            restaurants_updated, violations_inserted = update_database_batch(data)
            logger.info(f"run_database_update: Batch update processed. Restaurants (approx): {restaurants_updated}, Violations (approx): {violations_inserted}")
        else:
            logger.warning("run_database_update: No data fetched from API to update.")

    except Exception as e:
        print_debug(f"FATAL: Uncaught exception in run_database_update: {e}")
        logger.critical(f"Uncaught exception in run_database_update: {e}", exc_info=True)
    finally:
        logger.info("Database update process via run_database_update finished.")
        print_debug("--- run_database_update finished ---")

# This block allows running the script directly for testing or manual updates
if __name__ == '__main__':
    # Configure logging for direct script execution
    if not logging.getLogger().handlers: # Ensure handlers are set up if run directly
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', force=True)
    
    parser = argparse.ArgumentParser(description="Update restaurant inspection database.")
    parser.add_argument(
        "--days",
        type=int,
        default=5,
        help="Number of past days to fetch data for. Use a large number for a full backfill."
    )
    parser.add_argument(
        "--full-backfill",
        action="store_true",
        help="Perform a full backfill (sets days_back to a very large number, e.g., 3650)."
    )
    args = parser.parse_args()

    days_to_fetch = args.days
    if args.full_backfill:
        days_to_fetch = 3650 # Roughly 10 years
        logger.info(f"Full backfill requested. Setting days_back to {days_to_fetch}.")
    
    logger.info(f"Running update_database.py script directly for {days_to_fetch} days.")
    run_database_update(days_back=days_to_fetch)
    logger.info("Script execution finished.")

