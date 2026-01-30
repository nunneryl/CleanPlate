# One-time migration script to add dine_in, takeout, delivery to existing restaurants
# This only updates the new fields without touching existing enrichment data

import time
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_manager import DatabaseConnection, DatabaseManager
from google_provider import GoogleProvider

# --- Logger Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_restaurants_needing_service_options(conn, limit=500):
    """Fetch restaurants that have google_place_id but no service options yet."""
    query = """
        SELECT DISTINCT ON (camis) camis, google_place_id
        FROM restaurants
        WHERE google_place_id IS NOT NULL
        AND (dine_in IS NULL OR takeout IS NULL OR delivery IS NULL)
        ORDER BY camis
        LIMIT %s;
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (limit,))
            results = cursor.fetchall()
            logger.info(f"Found {len(results)} restaurants needing service options.")
            return [{'camis': row[0], 'place_id': row[1]} for row in results]
    except Exception as e:
        logger.error(f"Database error: {e}")
        return []

def update_service_options(conn, camis, dine_in, takeout, delivery):
    """Update only the service option fields."""
    update_sql = """
        UPDATE restaurants
        SET dine_in = %s, takeout = %s, delivery = %s
        WHERE camis = %s;
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(update_sql, (dine_in, takeout, delivery, camis))
        return True
    except Exception as e:
        logger.error(f"Failed to update CAMIS {camis}: {e}")
        return False

def main():
    DatabaseManager.initialize_pool()

    try:
        google = GoogleProvider()
    except ValueError as e:
        logger.error(f"Failed to initialize Google Provider: {e}")
        DatabaseManager.close_all_connections()
        return

    try:
        with DatabaseConnection() as conn:
            # Process in batches
            total_updated = 0
            batch_num = 0

            while True:
                batch_num += 1
                restaurants = fetch_restaurants_needing_service_options(conn, limit=500)

                if not restaurants:
                    logger.info("No more restaurants to process.")
                    break

                logger.info(f"--- Batch {batch_num}: Processing {len(restaurants)} restaurants ---")

                for index, restaurant in enumerate(restaurants):
                    camis = restaurant['camis']
                    place_id = restaurant['place_id']

                    # Only request the fields we need
                    details, error = google.get_place_details(place_id)

                    if details and not error:
                        dine_in = details.get('dineIn')
                        takeout = details.get('takeout')
                        delivery = details.get('delivery')

                        if update_service_options(conn, camis, dine_in, takeout, delivery):
                            total_updated += 1
                            if (index + 1) % 50 == 0:
                                logger.info(f"  Progress: {index + 1}/{len(restaurants)}")
                    else:
                        logger.warning(f"  Failed to get details for CAMIS {camis}")

                    conn.commit()
                    time.sleep(0.1)  # Small delay to avoid rate limiting

                logger.info(f"Batch {batch_num} complete. Total updated so far: {total_updated}")

    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
    finally:
        logger.info(f"--- Migration Complete. Total restaurants updated: {total_updated} ---")
        DatabaseManager.close_all_connections()

if __name__ == '__main__':
    main()
