# In file: reconcile_pending_grades.py (Corrected for modern psycopg library)

import logging
from datetime import datetime
import requests
import psycopg
from psycopg.rows import dict_row

from db_manager import DatabaseConnection, DatabaseManager
from config import APIConfig

# --- Constants ---
PENDING_GRADES = {'P', 'Z', 'N', None, ''}
FINAL_GRADES = {'A', 'B', 'C'}
BATCH_SIZE = 400 # Number of records to check in a single API call

# --- Logger Setup ---
logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

def fetch_live_inspection_data_batch(stale_records_batch):
    """Fetches live data for a batch of records using a single API call."""
    if not stale_records_batch:
        return {}

    where_clauses = []
    for record in stale_records_batch:
        camis = record['camis']
        inspection_date_iso = record['inspection_date'].isoformat()
        where_clauses.append(f"(camis='{camis}' AND inspection_date='{inspection_date_iso}')")
    
    soql_filter = " OR ".join(where_clauses)
    
    api_params = {
        "$where": soql_filter,
        "$limit": len(stale_records_batch) * 2
    }
    
    try:
        response = requests.get(APIConfig.NYC_API_URL, params=api_params, timeout=180)
        response.raise_for_status()
        live_data = response.json()
        
        live_grades = {}
        for item in live_data:
            camis = item.get('camis')
            insp_date_str = item.get('inspection_date')
            grade = item.get('grade')
            if camis and insp_date_str and grade:
                insp_date = datetime.fromisoformat(insp_date_str).date()
                live_grades[(camis, insp_date)] = grade
        return live_grades
        
    except requests.exceptions.RequestException as e:
        logger.warning(f"API batch request failed: {e}")
        return {}

def run_reconciliation():
    logger.info("Starting reconciliation of stale 'Pending' grades with batching...")
    
    try:
        with DatabaseConnection() as conn, conn.cursor(row_factory=dict_row) as cursor:
            logger.info("Fetching all 'Pending' or 'Blank' grade records from local database...")
            
            cursor.execute("""
                SELECT camis, inspection_date, grade 
                FROM restaurants 
                WHERE grade IS NULL OR grade IN ('P', 'Z', 'N');
            """)
            stale_records = cursor.fetchall()
            
            if not stale_records:
                logger.info("No stale records found. Reconciliation complete.")
                return

            logger.info(f"Found {len(stale_records)} potentially stale records to check in batches of {BATCH_SIZE}.")

            records_to_update = []
            grade_updates_to_log = []

            for i in range(0, len(stale_records), BATCH_SIZE):
                batch = stale_records[i:i + BATCH_SIZE]
                logger.info(f"Processing batch {i//BATCH_SIZE + 1}/{(len(stale_records) + BATCH_SIZE - 1)//BATCH_SIZE}...")
                
                live_grades_batch = fetch_live_inspection_data_batch(batch)
                
                for record in batch:
                    camis = record['camis']
                    inspection_date = record['inspection_date'].date()
                    previous_grade = record['grade']
                    
                    live_grade = live_grades_batch.get((camis, inspection_date))

                    if live_grade and live_grade in FINAL_GRADES and previous_grade in PENDING_GRADES:
                        logger.info(f"  -> Update Found for CAMIS {camis} on {inspection_date}: {previous_grade or 'NULL'} -> {live_grade}")
                        records_to_update.append((live_grade, camis, inspection_date))
                        grade_updates_to_log.append((camis, previous_grade, live_grade, 'finalized', inspection_date))

            if not records_to_update:
                logger.info("No stale records needed updating after checking the live API.")
                return
            
            logger.info(f"Updating {len(records_to_update)} records in the 'restaurants' table...")
            update_restaurants_sql = "UPDATE restaurants SET grade = %s WHERE camis = %s AND inspection_date::date = %s;"
            cursor.executemany(update_restaurants_sql, records_to_update)
            logger.info(f"Successfully updated {len(records_to_update)} restaurant records.")

            logger.info(f"Logging {len(grade_updates_to_log)} events in the 'grade_updates' table...")
            update_log_sql = "INSERT INTO grade_updates (restaurant_camis, previous_grade, new_grade, update_type, inspection_date) VALUES (%s, %s, %s, %s, %s);"
            cursor.executemany(update_log_sql, grade_updates_to_log)
            logger.info(f"Successfully logged {len(grade_updates_to_log)} grade update events.")
            
            conn.commit()

    except psycopg.Error as e:
        logger.error(f"A database error occurred: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")

    logger.info("Reconciliation process complete.")


if __name__ == '__main__':
    DatabaseManager.initialize_pool()
    run_reconciliation()
    DatabaseManager.close_all_connections()
