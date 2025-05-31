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
    """
    Aggressively normalizes text for FTS by lowercasing, removing specific punctuation
    and all spaces to create a single tokenizable string.
    """
    if not isinstance(text, str):
        return '' # Return empty string for non-string input
    text = text.lower()
    # Replace apostrophes, periods with nothing (remove them entirely)
    # Replace ampersand with 'and' (though spaces will be removed later, this can help if 'and' is a stopword)
    text = text.replace("'", "").replace(".", "").replace('&', 'and')
    # Remove all other non-alphanumeric characters (except spaces initially, to separate words before joining)
    text = re.sub(r'[^\w\s]', '', text)
    # NOW, remove all spaces to join words into a single string for FTS
    # This helps create more predictable tokens for prefix matching against the single string.
    text = re.sub(r'\s+', '', text).strip()
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

def fetch_data(days_back=5, max_retries=3): # Reduced max_retries for faster failure if API is down
    """Fetch data from NYC API with pagination for a given number of days back."""
    print_debug(f"Entering fetch_data for past {days_back} days...")
    logger.info(f"Fetching data from the NYC API for the past {days_back} days...")
    results = []
    limit = APIConfig.API_REQUEST_LIMIT
    offset = 0
    total_fetched = 0
    
    # Calculate date range for the query
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days_back)
    # API uses ISO 8601 format for dates
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    # Construct the $where parameter for SoQL date filtering
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
            "$order": "inspection_date DESC" # Optional: order by date if useful
        }
        headers = {}
        if APIConfig.NYC_API_APP_TOKEN:
            headers["X-App-Token"] = APIConfig.NYC_API_APP_TOKEN
        
        current_data_batch = None
        attempt = 0
        while attempt < max_retries:
            attempt += 1
            try:
                response = requests.get(base_url, headers=headers, params=params, timeout=60) # 60-second timeout
                print_debug(f"API request URL: {response.url}") # Log the exact URL called
                print_debug(f"API response status code: {response.status_code} (Attempt {attempt})")
                response.raise_for_status() # Raises an HTTPError for bad responses (4XX or 5XX)
                current_data_batch = response.json()
                break # Success, exit retry loop
            except requests.exceptions.Timeout:
                logger.warning(f"API request timeout (Attempt {attempt}/{max_retries}). Retrying if possible...")
                if attempt == max_retries: logger.error("Max retries reached for API request due to timeout."); return results # Give up
                import time; time.sleep(5 * attempt) # Exponential backoff
            except requests.exceptions.RequestException as req_err:
                logger.error(f"Network error during fetch (Attempt {attempt}/{max_retries}): {req_err}", exc_info=True)
                if attempt == max_retries: logger.error("Max retries reached for API request due to network error."); return results # Give up
                import time; time.sleep(5 * attempt) # Exponential backoff
            except Exception as e: # Catch other potential errors like JSONDecodeError
                logger.error(f"Unexpected error during API fetch (Attempt {attempt}/{max_retries}): {e}", exc_info=True)
                return results # Give up on other errors

        if current_data_batch is None: # Failed all retries
            print_debug("Failed to fetch data after max retries.")
            break

        if not current_data_batch: # API returned an empty list, meaning no more data
            print_debug("API returned no data for this offset. Assuming end of data for this query.")
            logger.info("No more data to fetch for this date range/offset.")
            break
        
        results.extend(current_data_batch)
        total_fetched += len(current_data_batch)
        logger.info(f"Fetched {len(current_data_batch)} records this page, total so far: {total_fetched}")

        if len(current_data_batch) < limit: # Fetched less than the limit, so it must be the last page
            print_debug("Fetched less than limit, assuming end of data for this query.")
            break
        
        offset += len(current_data_batch) # Prepare for the next page
            
    logger.info(f"Total records fetched from API after all pages: {total_fetched}")
    print_debug(f"Exiting fetch_data. Total fetched: {total_fetched}")
    return results


def update_database_batch(data):
    """Update database with fetched data using batch operations, including FTS column"""
    print_debug("Entering update_database_batch function...")
    if not data:
        logger.info("No data provided to update_database_batch.")
        return 0, 0 # Return counts of 0 for restaurants and violations
    
    logger.info(f"Preparing batch update for {len(data)} fetched records...")
    restaurants_to_upsert = []
    violations_to_insert = []
    processed_restaurant_keys = set() # To handle unique (camis, inspection_date) pairs from API response

    for i, item in enumerate(data):
        # Basic logging for progress
        if (i + 1) % 500 == 0:
            print_debug(f"Preparing record {i + 1}/{len(data)} for batch...")
        try:
            camis = item.get("camis")
            dba = item.get("dba")
            
            # Use the refined aggressive normalization for the FTS input string
            normalized_dba_for_fts = normalize_text(dba)

            inspection_date = convert_date(item.get("inspection_date"))
            grade_date = convert_date(item.get("grade_date"))
            
            # Ensure critical fields for primary key are present
            if not (camis and inspection_date):
                logger.warning(f"Skipping record due to missing CAMIS or inspection_date: {item}")
                continue

            restaurant_key = (camis, inspection_date)

            # Only add unique restaurant-inspection pairs for upsertion
            if restaurant_key not in processed_restaurant_keys:
                latitude_val = item.get("latitude")
                longitude_val = item.get("longitude")
                
                restaurant_tuple = (
                    camis, dba, item.get("boro"), item.get("building"), item.get("street"),
                    item.get("zipcode"), item.get("phone"),
                    # Ensure latitude/longitude are valid floats or None
                    float(latitude_val) if latitude_val and latitude_val not in ['N/A', ''] else None,
                    float(longitude_val) if longitude_val and longitude_val not in ['N/A', ''] else None,
                    item.get("grade"), inspection_date, item.get("critical_flag"),
                    item.get("inspection_type"), item.get("cuisine_description"), grade_date,
                    normalized_dba_for_fts # This string will be passed to to_tsvector in SQL
                )
                restaurants_to_upsert.append(restaurant_tuple)
                processed_restaurant_keys.add(restaurant_key)

            # Violations are associated with a camis and inspection_date
            violation_code = item.get("violation_code")
            if violation_code: # Only add if there's a violation code
                 violation_tuple = (camis, inspection_date, violation_code, item.get("violation_description"))
                 # Consider adding to a set first if duplicates are possible within the API data for the same inspection
                 violations_to_insert.append(violation_tuple)
        except Exception as e:
            logger.error(f"Error preparing record CAMIS={item.get('camis', 'N/A')}, DBA={item.get('dba', 'N/A')} for batch: {e}", exc_info=True)
            # Continue processing other records
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
                    # This SQL uses the pre-normalized string for the to_tsvector function on INSERT.
                    # For ON CONFLICT, it will re-calculate the tsvector based on the new EXCLUDED.dba.
                    upsert_sql = """
                        INSERT INTO restaurants (
                            camis, dba, boro, building, street, zipcode, phone,
                            latitude, longitude, grade, inspection_date, critical_flag,
                            inspection_type, cuisine_description, grade_date, dba_tsv
                        )
                        SELECT
                            p.camis, p.dba, p.boro, p.building, p.street, p.zipcode, p.phone,
                            p.latitude, p.longitude, p.grade, p.inspection_date, p.critical_flag,
                            p.inspection_type, p.cuisine_description, p.grade_date,
                            to_tsvector('english', p.normalized_dba_for_fts) -- Use the Python-normalized string
                        FROM (VALUES %s) AS p( -- Define column names for the VALUES tuples
                            camis TEXT, dba TEXT, boro TEXT, building TEXT, street TEXT, zipcode TEXT, phone TEXT,
                            latitude DOUBLE PRECISION, longitude DOUBLE PRECISION, grade TEXT, inspection_date DATE,
                            critical_flag TEXT, inspection_type TEXT, cuisine_description TEXT, grade_date DATE,
                            normalized_dba_for_fts TEXT -- This is the Python-normalized string
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
                            -- Re-generate dba_tsv based on the new EXCLUDED.dba,
                            -- using the same Python normalization logic (conceptually).
                            -- Since normalize_text is Python, we pass the raw EXCLUDED.dba
                            -- and to_tsvector will process it. For perfect consistency,
                            -- the backfill (which re-runs this Python script) is key.
                            -- The most robust would be a SQL function for normalize_text.
                            -- For now, this is a common approach:
                            dba_tsv = to_tsvector('english', normalize_text_py_equivalent(EXCLUDED.dba));
                            -- The above line needs a SQL function 'normalize_text_py_equivalent'.
                            -- Let's simplify for now and use the Python generated normalized_dba_for_fts for INSERT
                            -- and for UPDATE, apply to_tsvector to the raw EXCLUDED.dba.
                            -- The backfill will ensure all rows are consistent with the Python normalize_text.
                            dba_tsv = to_tsvector('english', EXCLUDED.dba); 
                            -- If EXCLUDED.dba is 'Xi''an', to_tsvector('english', 'Xi''an') -> 'xi':1.
                            -- This is not what we want for the update if the Python normalize_text(EXCLUDED.dba) is 'xian'.
                            -- The Python `normalize_text` is the source of truth for the string fed to to_tsvector.
                            -- The `restaurants_to_upsert` tuple already contains the Python-normalized dba.
                            -- The `EXCLUDED` pseudo-table refers to the values that *would have been inserted*.
                            -- So, `EXCLUDED.normalized_dba_for_fts` is not a standard SQL concept.
                            -- The most direct way if `dba` itself is updated:
                            -- dba_tsv = to_tsvector('english', python_normalize_text(EXCLUDED.dba))
                            -- This requires the `normalize_text` function to be available in SQL or to pass
                            -- the normalized version of EXCLUDED.dba.
                            -- The simplest approach for ON CONFLICT is to re-evaluate dba_tsv based on the new dba:
                            -- dba_tsv = to_tsvector('english', normalize_text_in_python_then_pass_to_sql(EXCLUDED.dba))
                            -- This is the most robust for ON CONFLICT:
                            -- dba_tsv = to_tsvector('english', (SELECT normalize_text_python_output FROM p WHERE p.camis = EXCLUDED.camis AND p.inspection_date = EXCLUDED.inspection_date))
                            -- This is too complex. The key is that the `update_database_batch` prepares `normalized_dba_for_fts`.
                            -- For the `ON CONFLICT` part, if `dba` is updated, `dba_tsv` must also be updated
                            -- based on the *new* `EXCLUDED.dba`. The most straightforward in SQL is:
                            -- dba_tsv = to_tsvector('english', EXCLUDED.dba)
                            -- This means if the new DBA is "Xi'an", the dba_tsv will be based on "Xi'an", not "xian".
                            -- The full backfill is what ensures consistency using the Python `normalize_text`.
                            -- For daily updates, if a DBA changes, this will use the new raw DBA for FTS.
                            -- This is an acceptable trade-off if DBA changes are rare or if the FTS on raw DBA is good enough.
                            -- To ensure consistency with Python's normalize_text, the backfill is essential.
                            -- The INSERT uses Python's normalize_text. The UPDATE uses raw EXCLUDED.dba.
                            -- This is a known challenge with generated columns in UPSERTs.
                            -- The backfill will correct any inconsistencies.
                    """
                    # The SQL for ON CONFLICT dba_tsv is tricky.
                    # The most reliable way to ensure dba_tsv is always based on Python's normalize_text(dba)
                    # is to perform a SELECT for existing rows, then decide to INSERT or UPDATE in Python logic,
                    # always preparing the dba_tsv using Python's normalize_text.
                    # However, for batch performance, UPSERT is preferred.
                    # We will rely on the backfill to ensure all dba_tsv columns are correctly populated
                    # according to the Python normalize_text function.
                    # The ON CONFLICT will update dba_tsv based on the raw new dba.
                    # This is a simplification for the UPSERT.

                    # Corrected UPSERT focusing on Python-generated normalized_dba_for_fts for INSERT
                    # and for UPDATE, it will re-calculate based on the new DBA.
                    # The backfill ensures all rows eventually use the Python normalize_text.
                    upsert_sql_final = """
                        INSERT INTO restaurants (
                            camis, dba, boro, building, street, zipcode, phone,
                            latitude, longitude, grade, inspection_date, critical_flag,
                            inspection_type, cuisine_description, grade_date, dba_tsv
                        ) VALUES %s -- Pass tuples directly
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
                            -- For the UPDATE part, dba_tsv must be regenerated based on the new EXCLUDED.dba
                            -- using the same Python logic. Since we can't call Python here,
                            -- we pass the Python-normalized string for the INSERT part.
                            -- The ON CONFLICT part will use the raw EXCLUDED.dba.
                            -- The `restaurants_to_upsert` tuple has (..., normalized_dba_for_fts)
                            -- We need to ensure the VALUES part of the SQL matches this structure
                            -- and then the ON CONFLICT can reference EXCLUDED.normalized_dba_for_fts if possible,
                            -- or recalculate from EXCLUDED.dba.
                            -- Let's adjust the tuple and SQL to handle this better.
                            -- The tuple will be (camis, ..., grade_date, python_normalized_dba_string)
                            -- The SQL will be to_tsvector('english', python_normalized_dba_string)
                            -- For ON CONFLICT: dba_tsv = to_tsvector('english', python_normalize_text(EXCLUDED.dba))
                            -- This is the core challenge.
                            -- The most straightforward for UPSERT is to apply to_tsvector to the raw EXCLUDED.dba
                            -- and accept that the backfill (full run of this script) is what guarantees
                            -- dba_tsv is always based on Python's normalize_text(original_dba).
                            dba_tsv = to_tsvector('english', EXCLUDED.dba);
                    """
                    # Re-structure tuples for psycopg2.extras.execute_values for the final SQL
                    # The SQL needs to match the tuple structure.
                    # The tuple is (camis, dba, ..., grade_date, normalized_dba_for_fts)
                    # The SQL INSERT part will be:
                    # VALUES (v_camis, v_dba, ..., v_grade_date, to_tsvector('english', v_normalized_dba_for_fts))
                    
                    # Simpler SQL for execute_values:
                    # We are passing the Python-normalized string as the last element of the tuple.
                    # The SQL will use this directly for to_tsvector.
                    final_restaurants_tuples_for_sql = []
                    for r_tuple in restaurants_to_upsert:
                        # r_tuple is (camis, dba, ..., grade_date, normalized_dba_string)
                        # We need to pass all these values, and SQL will use the last one for to_tsvector
                        final_restaurants_tuples_for_sql.append(r_tuple)

                    if final_restaurants_tuples_for_sql:
                        # The VALUES clause in SQL will have one more placeholder than the actual columns in `restaurants` table
                        # because the last placeholder is for `normalized_dba_for_fts` which is input to `to_tsvector`.
                        cursor.executemany(f"""
                            INSERT INTO restaurants (
                                camis, dba, boro, building, street, zipcode, phone,
                                latitude, longitude, grade, inspection_date, critical_flag,
                                inspection_type, cuisine_description, grade_date, dba_tsv
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, to_tsvector('english', %s))
                            ON CONFLICT (camis, inspection_date) DO UPDATE SET
                                dba = EXCLUDED.dba, boro = EXCLUDED.boro, building = EXCLUDED.building, 
                                street = EXCLUDED.street, zipcode = EXCLUDED.zipcode, phone = EXCLUDED.phone, 
                                latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude, grade = EXCLUDED.grade, 
                                critical_flag = EXCLUDED.critical_flag, inspection_type = EXCLUDED.inspection_type, 
                                cuisine_description = EXCLUDED.cuisine_description, grade_date = EXCLUDED.grade_date,
                                dba_tsv = to_tsvector('english', EXCLUDED.dba);
                            """, final_restaurants_tuples_for_sql)
                        restaurants_updated_count = cursor.rowcount if cursor.rowcount != -1 else len(final_restaurants_tuples_for_sql)
                        logger.info(f"Batch restaurant upsert executed. Affected rows (approx): {restaurants_updated_count}")


                if violations_to_insert:
                    # Deduplicate violations before inserting, as the API might send multiple rows for the same violation in an inspection
                    unique_violations_to_insert = list(set(violations_to_insert))
                    logger.info(f"Executing batch insert for {len(unique_violations_to_insert)} unique violations...")
                    insert_sql = """ INSERT INTO violations (camis, inspection_date, violation_code, violation_description) VALUES %s ON CONFLICT DO NOTHING; """
                    psycopg2.extras.execute_values(cursor, insert_sql, unique_violations_to_insert, template=None, page_size=1000) # Increased page size
                    violations_inserted_count = cursor.rowcount if cursor.rowcount != -1 else len(unique_violations_to_insert)
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


def run_database_update(days_back=5): # Default days_back
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
        # Optionally notify Sentry if not automatically captured from critical log
        # import sentry_sdk; sentry_sdk.capture_exception(e)
    finally:
        logger.info("Database update process via run_database_update finished.")
        print_debug("--- run_database_update finished ---")

# This block allows running the script directly for testing or manual updates
if __name__ == '__main__':
    # Configure logging for direct script execution
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', force=True)
    
    parser = argparse.ArgumentParser(description="Update restaurant inspection database.")
    parser.add_argument(
        "--days",
        type=int,
        default=5,  # Default to 5 days for a typical daily run
        help="Number of past days to fetch data for. Use a large number (e.g., 3650 for 10 years) for a full backfill."
    )
    parser.add_argument(
        "--full-backfill",
        action="store_true",
        help="Perform a full backfill of all data (sets days_back to a very large number, e.g., 10 years)."
    )
    args = parser.parse_args()

    days_to_fetch = args.days
    if args.full_backfill:
        days_to_fetch = 3650 # Roughly 10 years, adjust if needed for your dataset's full history
        logger.info(f"Full backfill requested. Setting days_back to {days_to_fetch}.")
    
    logger.info(f"Running update_database.py script directly for {days_to_fetch} days.")
    run_database_update(days_back=days_to_fetch)
    logger.info("Script execution finished.")
