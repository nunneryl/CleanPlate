# update_normalized_terms.py

import os
import re
import logging
import psycopg2
import psycopg2.extras

# Make sure this script can import your database connection manager
try:
    from db_manager import DatabaseConnection
except ImportError as e:
    print(f"CRITICAL ERROR: Could not import DatabaseConnection. Make sure this script is in your project's root directory. Error: {e}")
    exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)

# --- THE NEW, CORRECTED NORMALIZATION FUNCTION ---
# This MUST be identical to the new function in your deployed `app_search.py`
def normalize_search_term_for_hybrid(text):
    if not isinstance(text, str): return ''
    normalized_text = text.lower().replace('&', ' and ')
    accent_map = { 'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e', 'á': 'a', 'à': 'a', 'â': 'a', 'ä': 'a', 'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i', 'ó': 'o', 'ò': 'o', 'ô': 'o', 'ö': 'o', 'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u', 'ç': 'c', 'ñ': 'n' }
    for accented, unaccented in accent_map.items():
        normalized_text = normalized_text.replace(accented, unaccented)
    normalized_text = re.sub(r"[']", "", normalized_text)
    normalized_text = re.sub(r"[./-]", " ", normalized_text)
    normalized_text = re.sub(r"[^a-z0-9\s]", "", normalized_text)
    # Remove whitespace
    normalized_text = re.sub(r"\s+", "", normalized_text)
    return normalized_text.strip()

def run_backfill():
    logger.info("--- Starting Database Backfill for New Normalization Logic ---")
    records_to_update = []
    
    try:
        with DatabaseConnection() as conn, conn.cursor() as cursor:
            logger.info("Fetching all restaurant records (camis, inspection_date, dba)...")
            cursor.execute("SELECT camis, inspection_date, dba FROM restaurants;")
            all_records = cursor.fetchall()
            logger.info(f"Found {len(all_records)} total records to process.")
    except Exception as e:
        logger.critical(f"Failed to fetch records. Aborting. Error: {e}", exc_info=True)
        return

    for record in all_records:
        camis, inspection_date, dba = record
        if not dba:
            continue
        
        new_normalized_dba = normalize_search_term_for_hybrid(dba)
        records_to_update.append((new_normalized_dba, camis, inspection_date))

    logger.info(f"Re-normalization complete. Preparing to update {len(records_to_update)} records.")

    if not records_to_update:
        logger.info("No records needed updating. Exiting.")
        return

    try:
        with DatabaseConnection() as conn, conn.cursor() as cursor:
            logger.info("Executing bulk update... This may take a few minutes.")
            
            update_query = """
                UPDATE restaurants AS r SET
                    dba_normalized_search = v.dba_normalized_search
                FROM (VALUES %s) AS v(dba_normalized_search, camis, inspection_date)
                WHERE r.camis = v.camis AND r.inspection_date = v.inspection_date;
            """
            
            psycopg2.extras.execute_values(cursor, update_query, records_to_update, page_size=1000)
            conn.commit()
            logger.info(f"Successfully updated {cursor.rowcount} rows in the database.")
            
    except Exception as e:
        logger.critical(f"DATABASE UPDATE FAILED. The transaction was rolled back. Error: {e}", exc_info=True)
        return

    logger.info("--- Backfill script finished successfully! ---")

if __name__ == "__main__":
    run_backfill()
