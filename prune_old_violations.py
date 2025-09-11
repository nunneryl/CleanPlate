import logging
import argparse
from datetime import datetime, timedelta
from db_manager import DatabaseConnection, DatabaseManager

# --- Logger Setup ---
logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

def prune_violations(years_to_keep: int):
    """
    Deletes violation records from the database that are older than a specified
    number of years, based on their inspection_date.
    """
    if years_to_keep <= 0:
        logger.error("Retention period must be a positive number of years.")
        return

    # Calculate the cutoff date. Any inspection before this date will have its violations pruned.
    cutoff_date = datetime.now() - timedelta(days=years_to_keep * 365.25)
    logger.info(f"Preparing to prune violation records from inspections older than {years_to_keep} years (before {cutoff_date.strftime('%Y-%m-%d')}).")

    delete_query = "DELETE FROM violations WHERE inspection_date < %s;"

    try:
        with DatabaseConnection() as conn:
            with conn.cursor() as cursor:
                # First, run a SELECT COUNT to see how many records will be deleted (a "dry run")
                count_query = "SELECT COUNT(*) FROM violations WHERE inspection_date < %s;"
                cursor.execute(count_query, (cutoff_date,))
                count_result = cursor.fetchone()
                record_count = count_result[0] if count_result else 0
                
                if record_count == 0:
                    logger.info("No old violation records found to prune. The database is already clean.")
                    return

                logger.info(f"Found {record_count:,} violation records to be deleted. Proceeding with deletion...")

                # Now, execute the DELETE operation
                cursor.execute(delete_query, (cutoff_date,))
                deleted_count = cursor.rowcount
                
                logger.info("Committing changes to the database...")
                conn.commit()
                
                logger.info(f"Successfully deleted {deleted_count:,} old violation records.")
                logger.info("The database may take some time to reclaim the disk space.")

    except Exception as e:
        logger.error(f"An error occurred during the pruning process: {e}", exc_info=True)
    finally:
        DatabaseManager.close_all_connections()
        logger.info("Database connection pool closed.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Prune old violation records from the database.")
    parser.add_argument(
        "--years",
        type=int,
        default=5,
        help="The number of years of violation data to keep. Records older than this will be deleted."
    )
    args = parser.parse_args()
    
    prune_violations(years_to_keep=args.years)
