# In a new file: import_apify_data.py

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

def run_import():
    """
    Reads the Apify JSON output and updates the database with the scraped data.
    """
    try:
        with open('dataset_crawler-google-places_2025-10-13_20-43-05-582.json', 'r') as f:
            scraped_data = json.load(f)
        logger.info(f"Successfully loaded {len(scraped_data)} records from the JSON file.")
    except FileNotFoundError:
        logger.error("ERROR: The JSON file 'dataset_crawler-google-places_2025-10-13_20-43-05-582.json' was not found.")
        logger.error("Please make sure the file is in the same directory as this script.")
        return
    except json.JSONDecodeError:
        logger.error("ERROR: The JSON file is not formatted correctly.")
        return

    update_records = []
    for item in scraped_data:
        # We need the google_place_id to find the correct restaurant to update
        google_place_id = item.get('placeId')
        if not google_place_id:
            continue

        # Prepare the data for the UPDATE statement
        update_records.append((
            item.get('totalScore'),
            item.get('reviewsCount'),
            item.get('website'),
            json.dumps(item.get('openingHours')), # Convert hours to a JSON string
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
            logger.info(f"Successfully updated {cursor.rowcount} restaurant records in the database.")
    except Exception as e:
        logger.error(f"A database error occurred during the update: {e}")

if __name__ == '__main__':
    DatabaseManager.initialize_pool()
    run_import()
    DatabaseManager.close_all_connections()
