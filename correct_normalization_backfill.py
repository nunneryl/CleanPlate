# correct_normalization_backfill.py
import psycopg2
import psycopg2.extras
import re
import os
import logging
from datetime import datetime

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("correct_normalization_backfill.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- CANONICAL NORMALIZATION FUNCTION (Copied from app_search.py) ---
def normalize_text_final(text):
    if not isinstance(text, str):
        return ''
    normalized_text = text.lower()
    normalized_text = normalized_text.replace('&', ' and ')
    accent_map = {
        'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e', 'á': 'a', 'à': 'a', 'â': 'a', 'ä': 'a',
        'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i', 'ó': 'o', 'ò': 'o', 'ô': 'o', 'ö': 'o',
        'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u', 'ç': 'c', 'ñ': 'n'
    }
    for accented, unaccented in accent_map.items():
        normalized_text = normalized_text.replace(accented, unaccented)
    normalized_text = re.sub(r"['./-]", " ", normalized_text)
    normalized_text = re.sub(r"[^a-z0-9\s]", "", normalized_text)
    normalized_text = re.sub(r"\s+", " ", normalized_text).strip()
    return normalized_text

def get_db_connection_string():
    conn_str = os.environ.get('DATABASE_URL') # Assumes Railway's standard DATABASE_URL
    if not conn_str:
        logger.error("DATABASE_URL environment variable not set.")
        conn_str = input("Please paste your full PostgreSQL connection URL: ")
    return conn_str

def run_corrective_backfill(batch_size=500):
    conn_string = get_db_connection_string()
    if not conn_string:
        logger.error("No database connection string provided. Exiting.")
        return

    updated_count = 0
    processed_count = 0
    
    try:
        logger.info("Connecting to the database...")
        conn = psycopg2.connect(conn_string)
        
        with conn.cursor() as cursor_count:
            cursor_count.execute("SELECT COUNT(*) FROM restaurants;")
            total_rows = cursor_count.fetchone()[0]
            logger.info(f"Total restaurant records to process: {total_rows}")

        offset = 0
        while processed_count < total_rows:
            logger.info(f"Fetching batch of records (offset: {offset}, limit: {batch_size})...")
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor_select:
                cursor_select.execute(
                    "SELECT camis, inspection_date, dba FROM restaurants ORDER BY camis, inspection_date LIMIT %s OFFSET %s;",
                    (batch_size, offset)
                )
                rows = cursor_select.fetchall()

            if not rows:
                break

            updates_to_execute = []
            for row in rows:
                if not row['dba']:
                    continue
                
                normalized_dba = normalize_text_final(row['dba'])
                updates_to_execute.append((normalized_dba, row['camis'], row['inspection_date']))

            if updates_to_execute:
                with conn.cursor() as cursor_update:
                    update_query = """
                        UPDATE restaurants
                        SET 
                            dba_normalized_search = %s,
                            dba_tsv = to_tsvector('public.restaurant_search_config', %s)
                        WHERE camis = %s AND inspection_date = %s
                    """
                    # We need to pass the normalized_dba twice for the two %s placeholders
                    update_params = [(norm_dba, norm_dba, camis, insp_date) for norm_dba, camis, insp_date in updates_to_execute]
                    psycopg2.extras.execute_batch(cursor_update, update_query, update_params)
                    updated_count += cursor_update.rowcount

            processed_count += len(rows)
            offset += batch_size
            conn.commit()
            logger.info(f"Batch committed. Processed: {processed_count}/{total_rows}. Updated so far: {updated_count}")
            
        logger.info("Corrective backfill process completed.")

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    start_time = datetime.now()
    logger.info("Starting the corrective backfill to fix normalization in the database...")
    run_corrective_backfill()
    end_time = datetime.now()
    logger.info(f"Script finished in {end_time - start_time}.")
