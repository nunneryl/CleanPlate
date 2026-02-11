import logging
from db_manager import DatabaseConnection, DatabaseManager

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
    One-time script: scans the entire restaurant history to find and log
    instances where a grade transitioned from Pending to a final letter grade.
    Skips any transitions already captured by the live pipeline.
    """
    logger.info("Starting historical grade update backfill process...")

    with DatabaseConnection() as conn, conn.cursor() as cursor:
        logger.info("Finding historical grade transitions with a single query...")
        cursor.execute("""
            WITH grade_sequences AS (
                SELECT camis, grade, inspection_date,
                       LAG(grade) OVER (PARTITION BY camis ORDER BY inspection_date) AS prev_grade
                FROM restaurants
            )
            SELECT camis, prev_grade, grade, inspection_date
            FROM grade_sequences
            WHERE prev_grade IN ('P', 'Z', 'N') AND grade IN ('A', 'B', 'C')
              AND NOT EXISTS (
                  SELECT 1 FROM grade_updates gu
                  WHERE gu.restaurant_camis = grade_sequences.camis
                    AND gu.inspection_date = grade_sequences.inspection_date
              )
            ORDER BY camis, inspection_date;
        """)
        rows = cursor.fetchall()
        logger.info(f"Found {len(rows)} historical grade transitions not yet in grade_updates.")

        if not rows:
            logger.info("No historical grade updates to backfill.")
            conn.commit()
            return

        grade_updates_to_insert = []
        for camis, prev_grade, current_grade, inspection_date in rows:
            logger.info(f"  -> CAMIS {camis} on {inspection_date}: {prev_grade} -> {current_grade}")
            grade_updates_to_insert.append((camis, prev_grade, current_grade, inspection_date, inspection_date))

        logger.info(f"Inserting {len(grade_updates_to_insert)} historical grade updates...")
        update_sql = """
            INSERT INTO grade_updates (restaurant_camis, previous_grade, new_grade, update_type, update_date, inspection_date)
            VALUES (%s, %s, %s, 'backfill', %s, %s)
            ON CONFLICT DO NOTHING;
        """
        cursor.executemany(update_sql, grade_updates_to_insert)
        logger.info(f"Successfully inserted {cursor.rowcount} records.")

        conn.commit()

    logger.info("Backfill process complete.")

if __name__ == '__main__':
    DatabaseManager.initialize_pool()
    run_backfill()
    DatabaseManager.close_all_connections()
