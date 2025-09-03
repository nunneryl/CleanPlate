# In a new file named: reconcile_pending_grades.py

import logging
import requests
from datetime import datetime
from db_manager import DatabaseConnection

# --- Logger Setup ---
logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

NYC_API_BASE_URL = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"
PENDING_GRADES = {'P', 'Z', 'N', None, ''}
FINAL_GRADES = {'A', 'B', 'C'}

def fetch_live_inspection_data(camis, inspection_date):
    """Makes a targeted API call for a single inspection."""
    formatted_date = inspection_date.strftime('%Y-%m-%d')
    api_params = {
        "camis": camis,
        "inspection_date": f"{formatted_date}T00:00:00.000"
    }
    try:
        response = requests.get(NYC_API_BASE_URL, params=api_params, timeout=30)
        response.raise_for_status()
        data = response.json()
        # The API might return multiple rows for one inspection (one per violation)
        # We just need the grade from the first one.
        if data and data[0].get("grade"):
            return data[0]["grade"]
        return None
    except requests.exceptions.RequestException as e:
        logger.warning(f"API call failed for CAMIS {camis} on {formatted_date}: {e}")
        return None

def run_reconciliation():
    logger.info("Starting reconciliation of stale 'Pending' grades...")
    
    records_to_update = []
    grade_updates_to_log = []

    with DatabaseConnection() as conn, conn.cursor() as cursor:
        # 1. Get all stale records from our database
        logger.info("Fetching all 'Pending' or 'Blank' grade records from local database...")
        cursor.execute("SELECT camis, inspection_date, grade FROM restaurants WHERE grade IS NULL OR grade IN ('P', 'Z', 'N');")
        stale_records = cursor.fetchall()
        logger.info(f"Found {len(stale_records)} potentially stale records to check.")

        # 2. For each stale record, check the live API
        for i, record in enumerate(stale_records):
            camis, inspection_date, previous_grade = record
            
            if (i + 1) % 100 == 0:
                logger.info(f"Checking record {i + 1}/{len(stale_records)}...")

            live_grade = fetch_live_inspection_data(camis, inspection_date)

            # 3. If the live grade is a final letter grade, we've found an update
            if live_grade in FINAL_GRADES:
                logger.info(f"  -> Update Found for CAMIS {camis} on {inspection_date}: {previous_grade or 'NULL'} -> {live_grade}")
                # Add to list to update our main `restaurants` table
                records_to_update.append((live_grade, camis, inspection_date))
                # Add to list to log in our `grade_updates` table
                grade_updates_to_log.append((camis, previous_grade, live_grade, 'finalized'))

        # 4. Perform the updates in our database
        if records_to_update:
            logger.info(f"Updating {len(records_to_update)} records in the 'restaurants' table...")
            update_restaurants_sql = "UPDATE restaurants SET grade = %s WHERE camis = %s AND inspection_date = %s;"
            cursor.executemany(update_restaurants_sql, records_to_update)
            logger.info(f"Successfully updated {cursor.rowcount} restaurant records.")

            logger.info(f"Logging {len(grade_updates_to_log)} events in the 'grade_updates' table...")
            # We assign them a date of 15 days ago so they don't flood the live list
            update_log_sql = "INSERT INTO grade_updates (restaurant_camis, previous_grade, new_grade, update_type, update_date) VALUES (%s, %s, %s, %s, NOW() - INTERVAL '15 days');"
            cursor.executemany(update_log_sql, grade_updates_to_log)
            logger.info(f"Successfully logged {cursor.rowcount} grade update events.")
        else:
            logger.info("No stale records needed updating.")

        conn.commit()
    logger.info("Reconciliation process complete.")

if __name__ == '__main__':
    run_reconciliation()
