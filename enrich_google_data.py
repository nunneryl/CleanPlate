# In file: enrich_google_data.py

import time
import json
import logging
from db_manager import DatabaseConnection, DatabaseManager
from google_provider import GoogleProvider

# --- Logger Setup ---
logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

def fetch_restaurants_to_enrich(limit=500):
    """
    Fetches restaurants that have a google_place_id but haven't been enriched yet.
    """
    logger.info(f"Fetching up to {limit} restaurants needing enrichment...")
    query = """
        SELECT camis, google_place_id
        FROM restaurants
        WHERE google_place_id IS NOT NULL
        AND google_rating IS NULL
        GROUP BY camis, google_place_id
        ORDER BY MAX(inspection_date) DESC
        LIMIT %s;
    """
    try:
        with DatabaseConnection() as conn, conn.cursor() as cursor:
            cursor.execute(query, (limit,))
            results = cursor.fetchall()
            logger.info(f"Found {len(results)} restaurants to enrich.")
            return results
    except Exception as e:
        logger.error(f"Database error while fetching restaurants: {e}")
        return []

def update_restaurant_in_db(conn, camis, details):
    """
    Updates all rows for a given restaurant with the new Google Places data.
    """
    update_sql = """
        UPDATE restaurants
        SET
            google_rating = %s,
            google_review_count = %s,
            website = %s,
            hours = %s::jsonb,
            price_level = %s
        WHERE camis = %s;
    """
    try:
        # Safely get values from the response, providing defaults for missing keys
        rating = details.get('rating')
        review_count = details.get('userRatingCount')
        website = details.get('websiteUri')
        hours = json.dumps(details.get('regularOpeningHours')) # Convert hours object to JSON string
        price_level = details.get('priceLevel') # e.g., 'PRICE_LEVEL_MODERATE'
        
        with conn.cursor() as cursor:
            cursor.execute(update_sql, (rating, review_count, website, hours, price_level, camis))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Database update failed for CAMIS {camis}: {e}")
        conn.rollback()
        return False

def main():
    """Main function to run the enrichment process."""
    DatabaseManager.initialize_pool()
    
    try:
        google = GoogleProvider()
    except ValueError as e:
        logger.error(f"Failed to initialize Google Provider: {e}")
        return

    restaurants_to_process = fetch_restaurants_to_enrich(limit=500)
    total_count = len(restaurants_to_process)
    success_count = 0
    fail_count = 0

    if total_count == 0:
        logger.info("No restaurants found that need enrichment.")
    else:
        logger.info(f"--- Starting enrichment for {total_count} restaurants ---")

        with DatabaseConnection() as conn:
            for index, (camis, place_id) in enumerate(restaurants_to_process):
                logger.info(f"Processing {index + 1}/{total_count}: CAMIS {camis}")
                
                details, error = google.get_place_details(place_id)
                
                if details and not error:
                    if update_restaurant_in_db(conn, camis, details):
                        success_count += 1
                        logger.info(f"  -> SUCCESS: Database updated for CAMIS {camis}.")
                    else:
                        fail_count += 1 # DB update failed
                else:
                    fail_count += 1 # API call failed
                    logger.warning(f"  -> FAILED: Could not retrieve data for CAMIS {camis}. Error: {error}")

                time.sleep(0.1) # Small delay to avoid hitting rate limits

    logger.info("--- Enrichment Batch Complete ---")
    logger.info(f"Successfully updated: {success_count}")
    logger.info(f"Failed or skipped: {fail_count}")
    
    DatabaseManager.close_all_connections()
    logger.info("Database connection pool closed.")

if __name__ == '__main__':
    main()
