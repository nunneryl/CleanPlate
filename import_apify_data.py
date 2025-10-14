# In file: import_apify_data.py (Corrected for Name/Location Matching)

import json
import logging
from db_manager import DatabaseConnection, DatabaseManager
from psycopg.rows import dict_row

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
    Reads the Apify JSON output and updates the database by matching on name and location.
    """
    try:
        with open('dataset_crawler-google-places_2025-10-13_20-43-05-582.json', 'r') as f:
            scraped_data = json.load(f)
        logger.info(f"Successfully loaded {len(scraped_data)} records from the JSON file.")
    except FileNotFoundError:
        logger.error("ERROR: The JSON file was not found.")
        return
    except json.JSONDecodeError:
        logger.error("ERROR: The JSON file is not formatted correctly.")
        return

    update_records = []
    
    with DatabaseConnection() as conn, conn.cursor(row_factory=dict_row) as cursor:
        logger.info("Matching scraped data to database records...")
        match_count = 0
        
        for item in scraped_data:
            scraped_title = item.get('title')
            # The 'city' in the JSON can sometimes be the neighborhood, so we check both.
            scraped_location = item.get('city') or item.get('neighborhood')

            if not scraped_title or not scraped_location:
                continue

            # Find a matching restaurant in the database based on name and boro/city
            cursor.execute("""
                SELECT camis FROM restaurants 
                WHERE dba ILIKE %s AND boro ILIKE %s
                LIMIT 1;
            """, (f'%{scraped_title}%', f'%{scraped_location}%'))
            
            match = cursor.fetchone()

            if match:
                match_count += 1
                camis_to_update = match['camis']
                
                # Prepare the data for the UPDATE statement
                update_records.append((
                    item.get('totalScore'),
                    item.get('reviewsCount'),
                    item.get('website'),
                    json.dumps(item.get('openingHours')),
                    item.get('url'),
                    item.get('price'),
                    camis_to_update
                ))

    if not update_records:
        logger.warning("Could not match any of the scraped records to the database. Nothing to import.")
        return

    logger.info(f"Successfully matched {match_count} records. Preparing to update database...")

    try:
        with DatabaseConnection() as conn, conn.cursor() as cursor:
            # We now use the CAMIS to update all rows for that restaurant
            update_sql = """
                UPDATE restaurants
                SET
                    google_rating = %s,
                    google_review_count = %s,
                    website = %s,
                    hours = %s::jsonb,
                    google_maps_url = %s,
                    price_level = %s
                WHERE camis = %s;
            """
            cursor.executemany(update_sql, update_records)
            conn.commit()
            logger.info(f"Successfully updated data for {cursor.rowcount} restaurant rows in the database.")
    except Exception as e:
        logger.error(f"A database error occurred during the update: {e}")

if __name__ == '__main__':
    DatabaseManager.initialize_pool()
    run_import()
    DatabaseManager.close_all_connections()
