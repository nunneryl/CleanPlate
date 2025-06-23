# apostrophe_backfill.py
#
# ONE-TIME SCRIPT to fix apostrophe handling in the database.
#
# What it does:
# 1. Fetches all unique restaurant names from your live database.
# 2. Re-calculates the normalized search term for each name using the NEW logic
#    (removing apostrophes, e.g., "Joe's" becomes "joes").
# 3. Updates the `dba_normalized_search` column in your database with the corrected terms.
# 4. Clears your Redis cache to ensure users see the new, correct search results immediately.
#
# HOW TO RUN:
# 1. Make sure your local environment is connected to your live Railway database
#    (i.e., your .env file has the production database credentials).
# 2. Run this script from your terminal: `python apostrophe_backfill.py`

import os
import re
import logging
import psycopg2
import psycopg2.extras
import redis

# Local application imports
try:
    from db_manager import DatabaseConnection, get_redis_client
    from config import RedisConfig
except ImportError as e:
    print(f"CRITICAL ERROR: Could not import necessary modules. Make sure this script is in the same directory as your other backend files. Error: {e}")
    exit(1)

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)

# --- THE CORRECTED NORMALIZATION FUNCTION ---
# This MUST be identical to the new function in your deployed `app_search.py`
def normalize_search_term_for_hybrid(text):
    if not isinstance(text, str): return ''
    normalized_text = text.lower()
    normalized_text = normalized_text.replace('&', ' and ')
    accent_map = {
        'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e', 'á': 'a', 'à': 'a', 'â': 'a', 'ä': 'a',
        'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i', 'ó': 'o', 'ò': 'o', 'ô': 'o', 'ö': 'o',
        'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u', 'ç': 'c', 'ñ': 'n'
    }
    for accented, unaccented in accent_map.items():
        normalized_text = normalized_text.replace(accented, unaccented)
    
    # This is the critical change: remove apostrophes entirely.
    normalized_text = re.sub(r"[']", "", normalized_text)
    # Now, replace other specific punctuation with a space.
    normalized_text = re.sub(r"[./-]", " ", normalized_text)
    # Clean up any other unwanted characters and extra spaces.
    normalized_text = re.sub(r"[^a-z0-9\s]", "", normalized_text)
    normalized_text = re.sub(r"\s+", " ", normalized_text)
    
    return normalized_text.strip()

def run_backfill():
    """
    Fetches, re-normalizes, and updates all restaurant search terms.
    """
    logger.info("--- Starting Database Backfill for Apostrophe Fix ---")
    logger.warning("!!! IMPORTANT: It is recommended to back up your database before running this script. !!!")

    records_to_update = []
    
    # Step 1: Fetch all unique restaurant records (camis and dba)
    try:
        with DatabaseConnection() as conn, conn.cursor() as cursor:
            logger.info("Fetching all restaurant records from the database...")
            # We need camis and inspection_date to uniquely identify each row.
            cursor.execute("SELECT camis, inspection_date, dba FROM restaurants;")
            all_records = cursor.fetchall()
            logger.info(f"Found {len(all_records)} total records to process.")
    except Exception as e:
        logger.critical(f"Failed to fetch records from the database. Aborting. Error: {e}", exc_info=True)
        return

    # Step 2: Process each record and re-normalize the 'dba'
    for record in all_records:
        camis, inspection_date, dba = record
        if not dba:
            continue
        
        new_normalized_dba = normalize_search_term_for_hybrid(dba)
        # We will update the row identified by its composite primary key.
        records_to_update.append((new_normalized_dba, camis, inspection_date))

    logger.info(f"Re-normalization complete. Preparing to update {len(records_to_update)} records in the database.")

    # Step 3: Perform a bulk update
    if not records_to_update:
        logger.info("No records needed updating. Exiting.")
        return

    try:
        with DatabaseConnection() as conn, conn.cursor() as cursor:
            logger.info("Executing bulk update... This may take a few minutes.")
            
            # This SQL statement efficiently updates multiple rows at once.
            update_query = """
                UPDATE restaurants AS r SET
                    dba_normalized_search = v.dba_normalized_search
                FROM (VALUES %s) AS v(dba_normalized_search, camis, inspection_date)
                WHERE r.camis = v.camis AND r.inspection_date = v.inspection_date;
            """
            
            psycopg2.extras.execute_values(
                cursor,
                update_query,
                records_to_update,
                page_size=1000  # Process in chunks of 1000 for efficiency
            )
            
            conn.commit()
            logger.info(f"Successfully updated {cursor.rowcount} rows in the database.")
            
    except Exception as e:
        logger.critical(f"DATABASE UPDATE FAILED. The transaction was rolled back. Error: {e}", exc_info=True)
        return

    # Step 4: Clear the Redis cache
    try:
        logger.info("Clearing Redis cache to ensure new search results are served...")
        redis_conn = get_redis_client()
        if redis_conn:
            redis_conn.flushdb()
            logger.info("Redis cache successfully cleared.")
        else:
            logger.warning("Could not connect to Redis. Please clear the cache manually if possible.")
    except Exception as e:
        logger.error(f"An error occurred while clearing the Redis cache: {e}", exc_info=True)
        logger.warning("You may need to manually clear the Redis cache.")

    logger.info("--- Backfill script finished successfully! ---")


if __name__ == "__main__":
    # This block runs when you execute `python apostrophe_backfill.py`
    run_backfill()

