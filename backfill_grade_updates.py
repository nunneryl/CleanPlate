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
        logger.info("Finding historical grade transitions with a single query...")
        cursor.execute("""
            WITH grade_sequences AS (
                SELECT camis, grade,
                       LAG(grade) OVER (PARTITION BY camis ORDER BY inspection_date) AS prev_grade
                FROM restaurants
            )
            SELECT DISTINCT ON (camis) camis, prev_grade, grade
            FROM grade_sequences
            WHERE prev_grade IN ('P', 'Z', 'N') AND grade IN ('A', 'B', 'C')
            ORDER BY camis;
        """)
        rows = cursor.fetchall()
        logger.info(f"Found {len(rows)} historical grade transitions.")

        for camis, prev_grade, current_grade in rows:
            logger.info(f"  -> Found historical update for CAMIS {camis}: {prev_grade} -> {current_grade}")
            grade_updates_to_insert.append((camis, prev_grade, current_grade))

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
