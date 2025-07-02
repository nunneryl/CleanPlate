# repair_database.py - Final, Automated Data Repair Script

import os
import re
import logging
import psycopg2
import psycopg2.extras
import sys

# This block ensures that Python can find your other project files
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(SCRIPT_DIR)

try:
    from db_manager import DatabaseConnection
except ImportError as e:
    print(f"CRITICAL ERROR: Could not import from db_manager.py. Error: {e}")
    exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)

# --- FINAL NORMALIZATION FUNCTION ---
def normalize_search_term_for_hybrid(text):
    if not isinstance(text, str): return ''
    normalized_text = text.lower().replace('&', ' and ')
    accent_map = { 'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e', 'á': 'a', 'à': 'a', 'â': 'a', 'ä': 'a', 'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i', 'ó': 'o', 'ò': 'o', 'ô': 'o', 'ö': 'o', 'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u', 'ç': 'c', 'ñ': 'n' }
    for accented, unaccented in accent_map.items():
        normalized_text = normalized_text.replace(accented, unaccented)
    normalized_text = re.sub(r"['.]", "", normalized_text) # Removes periods and apostrophes
    normalized_text = re.sub(r"[-/]", " ", normalized_text)
    normalized_text = re.sub(r"[^a-z0-9\s]", "", normalized_text)
    normalized_text = re.sub(r"\s+", " ", normalized_text)
    return normalized_text.strip()

def run_database_repair():
    logger.info("--- Starting Database Repair Script ---")
    
    try:
        with DatabaseConnection() as conn, conn.cursor() as cursor:
            logger.info("Database connection successful. Fetching all records...")
            cursor.execute("SELECT camis, inspection_date, dba FROM restaurants;")
            all_records = cursor.fetchall()
            logger.info(f"Found {len(all_records)} records to process.")

            records_to_update = []
            for record in all_records:
                if record[2]:
                    new_normalized_dba = normalize_search_term_for_hybrid(record[2])
                    records_to_update.append((new_normalized_dba, record[0], record[1]))
            
            logger.info("Re-normalization complete. Preparing to update database.")
            update_query = """
                UPDATE restaurants AS r SET dba_normalized_search = v.dba_normalized_search
                FROM (VALUES %s) AS v(dba_normalized_search, camis, inspection_date)
                WHERE r.camis = v.camis AND r.inspection_date = v.inspection_date;
            """
            psycopg2.extras.execute_values(cursor, update_query, records_to_update, page_size=1000)
            
            updated_rows = cursor.rowcount
            conn.commit()
            logger.info(f"Successfully updated {updated_rows} rows in the database's final batch.")
            
    except Exception as e:
        logger.critical(f"DATABASE REPAIR FAILED. Error: {e}", exc_info=True)
        return

    logger.info("--- Database repair script finished successfully! ---")

if __name__ == "__main__":
    run_database_repair()
