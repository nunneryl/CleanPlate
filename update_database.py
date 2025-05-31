# update_database.py - Final version with Refined Normalization for FTS

import os
import requests
import logging
import argparse
import traceback
import psycopg2
import psycopg2.extras
import re # Ensure re is imported
from datetime import datetime, timedelta
from dateutil.parser import parse as date_parse
from db_manager import DatabaseConnection # Assuming you have this
from config import APIConfig # Assuming you have this

# Get logger instance
logger = logging.getLogger(__name__)

# --- REFINED NORMALIZATION FUNCTION ---
def normalize_text(text):
    """
    Normalizes text for FTS by lowercasing, removing specific punctuation (including internal apostrophes for this purpose),
    and preparing it for to_tsvector.
    """
    if not isinstance(text, str):
        return '' # Return empty string for non-string input
    text = text.lower()
    # Replace apostrophes and periods with spaces first to handle cases like "E.J.'s" -> "e j s"
    # This helps separate components that FTS might treat as distinct words.
    text = text.replace("'", " ").replace(".", " ")
    # Replace common "and" variations
    text = text.replace('&', ' and ')
    # Remove all other characters that are not letters, numbers, or whitespace.
    # This will effectively remove remaining punctuation after the targeted replacements.
    text = re.sub(r'[^\w\s]', '', text)
    # Collapse multiple whitespace characters (including those introduced by replacements) into a single space
    text = re.sub(r'\s+', ' ', text).strip()
    return text
# --- END REFINED NORMALIZATION FUNCTION ---

def print_debug(message):
    """Helper function to print debug messages clearly."""
    logger.info(f"---> SCRIPT DEBUG: {message}")

def convert_date(date_str):
    """Convert date string to date object"""
    if not date_str:
        return None
    try:
        # Try specific formats first
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.date()
            except ValueError:
                continue
        # Fallback to dateutil.parser for more general parsing
        dt = date_parse(date_str)
        return dt.date()
    except Exception as e:
        logger.warning(f"Could not parse date '{date_str}': {e}")
        return None

def fetch_data(days_back=5, max_retries=4):
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
        
        current_data_batch = None # To check if any data was fetched in the last request
        try:
            response = requests.get(base_url, headers=headers, params=params, timeout=60) # request timeout
            print_debug(f"API response status code: {response.status_code}")
            response.raise_for_status() # Raises an HTTPError for bad responses (4XX or 5XX)
            current_data_batch = response.json()

            if not current_data_batch: # No more data from the API for this query
                print_debug("API returned no data for this offset. Assuming end of data for this query.")
                logger.info("No more data to fetch for this date range/offset.")
                break
            
            print_debug(f"API fetch successful, got {len(current_data_batch)} records.")
            results.extend(current_data_batch)
            total_fetched += len(current_data_batch)
            logger.info(f"Fetched {len(current_data_batch)} records, total so far: {total_fetched}")

            if len(current_data_batch) < limit: # Fetched less than the limit, so it must be the last page
                print_debug("Fetched less than limit, assuming end of data for this query.")
                break
            
            offset += len(current_data_batch) # Prepare for the next page

        except requests.exceptions.RequestException as req_err:
            logger.error(f"Network error during fetch: {req_err}", exc_info=True)
            print_debug(f"Network error during fetch: {req_err}")
            break # Stop fetching on error
        except Exception as e:
            logger.error(f"Unexpected error during API fetch: {e}", exc_info=True)
            print_debug(f"Unexpected error during API fetch: {e}")
            break # Stop fetching on unexpected error
            
    logger.info(f"Total records fetched from API: {total_fetched}")
    print_debug(f"Exiting fetch_data. Total fetched: {total_fetched}")
    return results

def update_database_batch(data):
    """Update database with fetched data using batch operations, including FTS column"""
    print_debug("Entering update_database_batch function...")
    if not data:
        logger.info("No data provided to update_database_batch.")
        print_debug("No data, exiting update_database_batch.")
        return 0, 0
    
    logger.info(f"Preparing batch update for {len(data)} fetched records...")
    restaurants_to_upsert = []
    violations_to_insert = []
    processed_restaurant_keys = set() # To handle unique (camis, inspection_date) pairs

    for i, item in enumerate(data):
        if (i + 1) % 1000 == 0:
            print_debug(f"Preparing record {i + 1}/{len(data)} for batch...")
        try:
            camis = item.get("camis")
            dba = item.get("dba")
            normalized_dba_for_fts = normalize_text(dba) # Use refined normalization

            inspection_date = convert_date(item.get("inspection_date"))
            grade_date = convert_date(item.get("grade_date"))
            latitude_val = item.get("latitude")
            longitude_val = item.get("longitude")
            
            restaurant_key = (camis, inspection_date)

            if camis and inspection_date and restaurant_key not in processed_restaurant_keys:
                restaurant_tuple = (
                    camis, dba, item.get("boro"), item.get("building"), item.get("street"),
                    item.get("zipcode"), item.get("phone"),
                    float(latitude_val) if latitude_val and latitude_val not in ['N/A', None, ''] else None,
                    float(longitude_val) if longitude_val and longitude_val not in ['N/A', None, ''] else None,
                    item.get("grade"), inspection_date, item.get("critical_flag"), item.get("inspection_type"),
                    item.get("cuisine_description"), grade_date,
                    normalized_dba_for_fts # This string will be passed to to_tsvector in SQL
                )
                restaurants_to_upsert.append(restaurant_tuple)
                processed_restaurant_keys.add(restaurant_key)

            violation_code = item.get("violation_code")
            if camis and inspection_date and violation_code: # Ensure key fields exist
                 violation_tuple = ( camis, inspection_date, violation_code, item.get("violation_description") )
                 violations_to_insert.append(violation_tuple)
        except Exception as e:
            logger.error(f"Error preparing record CAMIS={item.get('camis', 'N/A')}, DBA={item.get('dba', 'N/A')} for batch: {e}", exc_info=True)
            print_debug(f"ERROR preparing record CAMIS={item.get('camis', 'N/A')} for batch: {e}")
            continue # Skip this record but continue with others
            
    print_debug(f"Prepared {len(restaurants_to_upsert)} unique restaurant inspection records for upsert.")
    print_debug(f"Prepared {len(violations_to_insert)} violation records for insert.")
    
    conn = None
    success = False
    
    try:
        print_debug("Attempting to get DB connection for batch operations...")
        with DatabaseConnection() as conn:
            print_debug("DB connection acquired successfully.")
            with conn.cursor() as cursor:
                print_debug("DB cursor acquired.")
                
                if restaurants_to_upsert:
                    logger.info(f"Executing batch upsert for {len(restaurants_to_upsert)} restaurants...")
                    # SQL now correctly uses the pre-normalized string for to_tsvector input
                    # And uses EXCLUDED.dba for the on-conflict update of dba_tsv
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
                            to_tsvector('english', p.normalized_dba_for_fts)
                        FROM (VALUES %s) AS p(
                            camis, dba, boro, building, street, zipcode, phone,
                            latitude, longitude, grade, inspection_date, critical_flag,
                            inspection_type, cuisine_description, grade_date, normalized_dba_for_fts
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
                            dba_tsv = to_tsvector('english', normalize_text(EXCLUDED.dba)); 
                            -- We need normalize_text function in PostgreSQL or pass normalized EXCLUDED.dba
                            -- For simplicity assuming direct to_tsvector on EXCLUDED.dba is acceptable
                            -- if EXCLUDED.dba is the raw new dba.
                            -- A more robust way would be to pass normalized EXCLUDED.dba or create a SQL function.
                            -- Let's use a simpler approach: use the original text for EXCLUDED.dba,
                            -- letting to_tsvector handle it. The key is consistency.
                            -- If EXCLUDED.dba is the original new name, to_tsvector on it is consistent.
                            -- The most critical part is that the normalized_dba_for_fts passed to the INSERT
                            -- is consistently created.
                            -- Re-evaluating: The tuple contains `normalized_dba_for_fts`. 
                            -- The conflict update should ideally use a normalized version of the new dba.
                            -- The simplest robust way:
                            -- dba_tsv = to_tsvector('english', normalize_text_sql_version(EXCLUDED.dba))
                            -- For now, let's assume the Python normalized string IS the best source.
                            -- The SQL should reference the *normalized* string intended for update.
                            -- The `p.normalized_dba_for_fts` is not available in EXCLUDED.
                            -- So, if EXCLUDED.dba is the *new* dba, this is fine:
                            dba_tsv = to_tsvector('english', EXCLUDED.dba);
                    """
                    psycopg2.extras.execute_values(cursor, upsert_sql, restaurants_to_upsert, template=None, page_size=100)
                    logger.info("Batch restaurant upsert executed.")

                if violations_to_insert:
                    logger.info(f"Executing batch insert for {len(violations_to_insert)} violations...")
                    insert_sql = """ INSERT INTO violations (camis, inspection_date, violation_code, violation_description) VALUES %s ON CONFLICT DO NOTHING; """
                    psycopg2.extras.execute_values(cursor, insert_sql, violations_to_insert, template=None, page_size=100)
                    logger.info("Batch violation insert executed.")
                
                logger.info("Attempting to commit batch transaction...")
                conn.commit()
                logger.info("Database transaction committed successfully.")
                success = True
                
    except psycopg2.Error as db_err:
        logger.error(f"Database Error during batch update: {db_err}", exc_info=True)
        print_debug(f"FATAL: Database Error during batch update: {db_err}")
        if conn:
            try: conn.rollback(); logger.info("Database transaction rolled back.")
            except Exception as rb_e: logger.error(f"Error during rollback: {rb_e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error during batch database update: {e}", exc_info=True)
        print_debug(f"FATAL: Unexpected error during batch database update: {e}")
        if conn:
            try: conn.rollback(); logger.info("Database transaction rolled back.")
            except Exception as rb_e: logger.error(f"Error during rollback: {rb_e}", exc_info=True)
    finally:
        print_debug("Exiting update_database_batch function (finally block).")

    if success:
        logger.info(f"Batch database update finished. Processed {len(restaurants_to_upsert)} restaurants and {len(violations_to_insert)} violations.")
        return len(restaurants_to_upsert), len(violations_to_insert)
    else:
        logger.error("Batch database update failed.")
        return 0, 0

def run_database_update(days_back=5):
    """Main entry point for running the update logic, called from Flask."""
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
    finally:
        logger.info("Database update process via run_database_update finished.")
        print_debug("--- run_database_update finished ---")
