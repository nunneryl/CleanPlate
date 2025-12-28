# In file: reconcile_pending_grades.py (Corrected with Historical Dates)

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
BATCH_SIZE = 50

# --- Logger Setup ---
logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

def convert_api_date(date_str):
    """Safely converts an ISO date string from the API to a date object."""
    if not date_str: return None
    try:
        return datetime.fromisoformat(date_str).date()
    except (ValueError, TypeError):
        return None

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
        
        live_details = {}
        for item in live_data:
            camis = item.get('camis')
            insp_date = convert_api_date(item.get('inspection_date'))
            grade = item.get('grade')
            grade_date = convert_api_date(item.get('grade_date'))
            
            if camis and insp_date and grade:
                live_details[(camis, insp_date)] = (grade, grade_date)
        return live_details
        
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
                
                live_details_batch = fetch_live_inspection_data_batch(batch)
                
                for record in batch:
                    camis = record['camis']
                    inspection_date = record['inspection_date'].date()
                    previous_grade = record['grade']
                    
                    live_data = live_details_batch.get((camis, inspection_date))

                    if live_data:
                        live_grade, live_grade_date = live_data
                        if live_grade in FINAL_GRADES and previous_grade in PENDING_GRADES:
                            logger.info(f"  -> Update Found for CAMIS {camis} on {inspection_date}: {previous_grade or 'NULL'} -> {live_grade}")
                            records_to_update.append((live_grade, live_grade_date, camis, inspection_date))
                            
                            # Use the official grade_date as the update_date for the log
                            update_date_for_log = live_grade_date if live_grade_date else inspection_date
                            grade_updates_to_log.append((camis, previous_grade, live_grade, 'finalized', update_date_for_log, inspection_date))

            if not records_to_update:
                logger.info("No stale records needed updating after checking the live API.")
                return
            
            logger.info(f"Updating {len(records_to_update)} records in the 'restaurants' table...")
            update_restaurants_sql = "UPDATE restaurants SET grade = %s, grade_date = %s WHERE camis = %s AND inspection_date::date = %s;"
            cursor.executemany(update_restaurants_sql, records_to_update)
            logger.info(f"Successfully updated {len(records_to_update)} restaurant records.")

            logger.info(f"Logging {len(grade_updates_to_log)} events in the 'grade_updates' table...")
            update_log_sql = "INSERT INTO grade_updates (restaurant_camis, previous_grade, new_grade, update_type, update_date, inspection_date) VALUES (%s, %s, %s, %s, %s, %s);"
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
