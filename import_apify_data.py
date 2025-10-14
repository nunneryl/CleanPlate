# In file: import_apify_data.py (Final Version for Place ID Matching)

import json
import logging
from db_manager import DatabaseConnection, DatabaseManager

# --- Logger Setup ---
logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# --- IMPORTANT: Change this to the new JSON file name ---
JSON_FILE_NAME = 'dataset_crawler-google-places_2025-10-14_17-10-10-953.json'

def run_import():
    """
    Reads the Apify JSON output and updates the database with the scraped data
    by matching on the unique 'placeId'.
    """
    try:
        with open(JSON_FILE_NAME, 'r') as f:
            scraped_data = json.load(f)
        logger.info(f"Successfully loaded {len(scraped_data)} records from '{JSON_FILE_NAME}'.")
    except FileNotFoundError:
        logger.error(f"ERROR: The file '{JSON_FILE_NAME}' was not found.")
        return
    except json.JSONDecodeError:
        logger.error("ERROR: The JSON file is not formatted correctly.")
        return

    update_records = []
    for item in scraped_data:
        google_place_id = item.get('placeId')
        if not google_place_id:
            continue

        update_records.append((
            item.get('totalScore'),
            item.get('reviewsCount'),
            item.get('website'),
            json.dumps(item.get('openingHours')),
            item.get('url'),
            item.get('price'),
            google_place_id
        ))

    if not update_records:
        logger.warning("No records with a 'placeId' found in the JSON file. Nothing to import.")
        return

    logger.info(f"Prepared {len(update_records)} records to be updated in the database.")

    try:
        with DatabaseConnection() as conn, conn.cursor() as cursor:
            # This query is much more efficient as it updates all rows for a given place ID at once.
            update_sql = """
                UPDATE restaurants
                SET
                    google_rating = %s,
                    google_review_count = %s,
                    website = %s,
                    hours = %s::jsonb,
                    google_maps_url = %s,
                    price_level = %s
                WHERE google_place_id = %s;
            """
            cursor.executemany(update_sql, update_records)
            conn.commit()
            logger.info(f"Successfully updated {cursor.rowcount} restaurant rows in the database.")
    except Exception as e:
        logger.error(f"A database error occurred during the update: {e}")

if __name__ == '__main__':
    DatabaseManager.initialize_pool()
    run_import()
    DatabaseManager.close_all_connections()
