# Weekly backfill script for dine_in, takeout, delivery fields
# Designed to run as part of cron job with cost cap of ~$10/week
# At $0.02-0.035 per Places API request, 300 requests keeps us under $10

import time
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_manager import DatabaseConnection, DatabaseManager
from google_provider import GoogleProvider

# --- Configuration ---
MAX_REQUESTS_PER_RUN = 300  # ~$10 at $0.02-0.035 per request

# --- Logger Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_restaurants_needing_service_options(conn, limit):
    """Fetch restaurants that have google_place_id but no service options yet."""
    query = """
        SELECT DISTINCT ON (camis) camis, google_place_id
        FROM restaurants
        WHERE google_place_id IS NOT NULL
        AND dine_in IS NULL
        ORDER BY camis
        LIMIT %s;
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (limit,))
            results = cursor.fetchall()
            logger.info(f"Found {len(results)} restaurants needing service options (capped at {limit}).")
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
            restaurants = fetch_restaurants_needing_service_options(conn, limit=MAX_REQUESTS_PER_RUN)

            if not restaurants:
                logger.info("No restaurants needing service options. Done.")
                return

            logger.info(f"Processing {len(restaurants)} restaurants (max {MAX_REQUESTS_PER_RUN}/week)")

            total_updated = 0
            for index, restaurant in enumerate(restaurants):
                camis = restaurant['camis']
                place_id = restaurant['place_id']

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
                time.sleep(0.1)

            logger.info(f"Service options backfill complete. Updated: {total_updated}")

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
    finally:
        DatabaseManager.close_all_connections()

if __name__ == '__main__':
    main()
