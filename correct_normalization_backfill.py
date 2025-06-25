# correct_normalization_backfill.py
import psycopg
from psycopg.rows import dict_row
import re
import os
import logging
from utils import normalize_search_term_for_hybrid as normalize_text_final
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

def get_db_connection_string():
    conn_str = os.environ.get('DATABASE_URL')
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
    conn = None
    
    try:
        logger.info("Connecting to the database...")
        conn = psycopg.connect(conn_string, row_factory=dict_row, autocommit=True)
        
        with conn.cursor() as cursor_count:
            cursor_count.execute("SELECT COUNT(*) FROM restaurants;")
            total_rows = cursor_count.fetchone()['count']
            logger.info(f"Total restaurant records to process: {total_rows}")

        offset = 0
        while processed_count < total_rows:
            logger.info(f"Fetching batch of records (offset: {offset}, limit: {batch_size})...")
            with conn.cursor() as cursor_select:
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
                update_params = (normalized_dba, row['camis'], row['inspection_date'])
                updates_to_execute.append(update_params)

            if updates_to_execute:
                with conn.cursor() as cursor_update:
                    # UPDATED: Removed the dba_tsv column from the query
                    update_query = """
                        UPDATE restaurants
                        SET 
                            dba_normalized_search = %s
                        WHERE camis = %s AND inspection_date = %s
                    """
                    cursor_update.executemany(update_query, updates_to_execute)
                    updated_count += cursor_update.rowcount
            
            processed_count += len(rows)
            offset += batch_size
            # No conn.commit() needed due to autocommit=True
            logger.info(f"Batch processed. Processed: {processed_count}/{total_rows}. Updated so far: {updated_count}")
            
        logger.info("Corrective backfill process completed.")

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    start_time = datetime.now()
    logger.info("Starting the corrective backfill to fix normalization in the database...")
    run_corrective_backfill()
    end_time = datetime.now()
    logger.info(f"Script finished in {end_time - start_time}.")
