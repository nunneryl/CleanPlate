# In a new file named: backfill_grade_updates.py

import logging
from db_manager import DatabaseConnection

# --- Logger Setup ---
logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

PENDING_GRADES = {'P', 'Z', 'N'}
FINAL_GRADES = {'A', 'B', 'C'}

def run_backfill():
    """
    Scans the entire restaurant history to find and log instances where a grade
    was updated from a 'Pending' state to a final letter grade.
    """
    logger.info("Starting historical grade update backfill process...")
    
    grade_updates_to_insert = []

    with DatabaseConnection() as conn, conn.cursor() as cursor:
        logger.info("Fetching all unique restaurant CAMIS from the database...")
        cursor.execute("SELECT DISTINCT camis FROM restaurants;")
        all_camis = [row[0] for row in cursor.fetchall()]
        logger.info(f"Found {len(all_camis)} unique restaurants to process.")

        for i, camis in enumerate(all_camis):
            if (i + 1) % 500 == 0:
                logger.info(f"Processing restaurant {i + 1}/{len(all_camis)}...")
            
            cursor.execute(
                "SELECT grade FROM restaurants WHERE camis = %s ORDER BY inspection_date ASC;",
                (camis,)
            )
            inspections = cursor.fetchall()

            for j in range(1, len(inspections)):
                previous_grade = inspections[j-1][0]
                current_grade = inspections[j][0]
                
                if previous_grade in PENDING_GRADES and current_grade in FINAL_GRADES:
                    logger.info(f"  -> Found historical update for CAMIS {camis}: {previous_grade} -> {current_grade}")
                    grade_updates_to_insert.append((camis, previous_grade, current_grade))
                    break

        if grade_updates_to_insert:
            logger.info(f"Found a total of {len(grade_updates_to_insert)} historical grade updates. Inserting into the database...")
            update_sql = "INSERT INTO grade_updates (restaurant_camis, previous_grade, new_grade, update_date) VALUES (%s, %s, %s, NOW() - INTERVAL '21 days');"
            cursor.executemany(update_sql, grade_updates_to_insert)
            logger.info(f"Successfully inserted {cursor.rowcount} records.")
        else:
            logger.info("No historical grade updates were found.")

        conn.commit()

    logger.info("Backfill process complete.")

if __name__ == '__main__':
    run_backfill()
