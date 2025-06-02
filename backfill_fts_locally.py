import psycopg2
import psycopg2.extras # For DictCursor
import re
import os
import logging
from datetime import datetime # For logging timestamp

# --- Logging Setup ---
logging.basicConfig(
    level=logging.DEBUG, # Changed to DEBUG to get more verbose output
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("backfill_fts_locally_v2_debug.log"), # Log to a new file
        logging.StreamHandler() # Also log to console
    ]
)
logger = logging.getLogger(__name__)

# --- AGGRESSIVE NORMALIZATION FUNCTION (Must match update_database.py and app_search.py) ---
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

def get_db_connection_string():
    conn_str = os.environ.get('DATABASE_URL_FOR_BACKFILL')
    if not conn_str:
        logger.warning("DATABASE_URL_FOR_BACKFILL environment variable not set.")
        conn_str = input("Please paste your full PostgreSQL connection URL from Railway: ")
    return conn_str

def backfill_dba_tsv(batch_size=500): # Process in batches
    conn_string = get_db_connection_string()
    if not conn_string:
        logger.error("No database connection string provided. Exiting.")
        return

    conn = None
    updated_count = 0
    processed_count = 0
    failed_to_normalize_count = 0
    skipped_due_to_empty_normalized_dba = 0

    try:
        logger.info(f"Connecting to database...")
        conn = psycopg2.connect(conn_string)
        logger.info("Successfully connected to the database.")
        
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor_count:
            cursor_count.execute("SELECT COUNT(*) FROM restaurants WHERE dba IS NOT NULL;")
            total_rows = cursor_count.fetchone()[0]
            logger.info(f"Total restaurants with a DBA to potentially process: {total_rows}")
            if total_rows == 0:
                logger.info("No restaurants to backfill. Exiting.")
                return

        offset = 0
        while True:
            logger.info(f"Fetching batch of restaurants (offset: {offset}, limit: {batch_size})...")
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor_select:
                cursor_select.execute(
                    "SELECT camis, dba, inspection_date FROM restaurants WHERE dba IS NOT NULL ORDER BY camis, inspection_date LIMIT %s OFFSET %s;",
                    (batch_size, offset)
                )
                rows = cursor_select.fetchall()

            if not rows:
                logger.info("No more rows to process in database.")
                break

            logger.info(f"Fetched {len(rows)} rows for this batch.")
            updates_to_execute = []
            for row_num, row in enumerate(rows):
                processed_count += 1
                try:
                    camis = row['camis']
                    dba = row['dba'] # This should not be None due to WHERE clause
                    inspection_date = row['inspection_date']

                    if not isinstance(dba, str):
                        logger.warning(f"Row {processed_count} (CAMIS {camis}, InspDate {inspection_date}): DBA is not a string (type: {type(dba)}). Skipping.")
                        failed_to_normalize_count +=1
                        continue

                    normalized_dba = normalize_text(dba) # normalize_text now logs its input/output
                    
                    if not normalized_dba:
                        logger.warning(f"Row {processed_count} (CAMIS {camis}, InspDate {inspection_date}): Normalized DBA is empty for original DBA: '{dba[:100]}'. Skipping.")
                        skipped_due_to_empty_normalized_dba += 1
                        continue
                    
                    updates_to_execute.append((normalized_dba, camis, inspection_date))
                    if row_num < 5: # Log first 5 prepared updates in this batch
                        logger.debug(f"Prepared for update: CAMIS={camis}, InspDate={inspection_date}, NormDBA='{normalized_dba}'")

                except Exception as e:
                    logger.error(f"Error processing row {processed_count} (CAMIS {row.get('camis')}, DBA {row.get('dba')[:100]}): {e}", exc_info=True)
                    failed_to_normalize_count +=1 # Count as a failure to normalize/prepare
                    continue
            
            logger.info(f"Prepared {len(updates_to_execute)} updates for this batch.")

            if updates_to_execute:
                batch_update_successful_rows = 0
                try:
                    with conn.cursor() as cursor_update: # New cursor for update
                        psycopg2.extras.execute_batch(
                            cursor_update,
                            """
                            UPDATE restaurants 
                            SET dba_tsv = to_tsvector('english', %s) 
                            WHERE camis = %s AND inspection_date = %s;
                            """,
                            updates_to_execute,
                            page_size=100
                        )
                        # rowcount for execute_batch with psycopg2 might not be reliable for actual updated rows.
                        # It often returns the number of statements in the last batch executed.
                        # We assume if no exception, all in updates_to_execute were attempted.
                        batch_update_successful_rows = len(updates_to_execute)
                        conn.commit() # Commit after each successful batch
                        updated_count += batch_update_successful_rows
                        logger.info(f"Committed batch. {batch_update_successful_rows} update statements executed. Total updated so far: {updated_count}")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Error executing update batch. Rolled back. Error: {e}", exc_info=True)
                    # All in this batch are now considered failed for counting purposes
                    failed_to_normalize_count += len(updates_to_execute)
            else:
                logger.info("No updates to execute for this batch (all DBAs might have normalized to empty or had issues).")
            
            if len(rows) < batch_size: # Last page
                logger.info("Processed the last page of rows from database.")
                break
            offset += batch_size # Move to next page

        logger.info("Backfill process completed iteration through database rows.")

    except psycopg2.Error as db_err:
        logger.error(f"Database connection error or query error: {db_err}", exc_info=True)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")
    
    logger.info(f"--- Backfill Summary ---")
    logger.info(f"Total records iterated through (approx): {processed_count}")
    logger.info(f"Successfully executed UPDATE statements for: {updated_count} records")
    logger.info(f"Skipped due to empty normalized DBA: {skipped_due_to_empty_normalized_dba} records")
    logger.info(f"Failed during processing/preparation (before update attempt) or during update batch: {failed_to_normalize_count} records")

if __name__ == "__main__":
    logger.info("Starting local FTS backfill script (v2 with enhanced debugging)...")
    start_time = datetime.now()
    
    # Ensure DATABASE_URL_FOR_BACKFILL is set or script will prompt
    backfill_dba_tsv(batch_size=100) # Smaller batch size for initial debug run
    
    end_time = datetime.now()
    logger.info(f"Script finished in {end_time - start_time}.")

