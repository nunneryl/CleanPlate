# In file: enrich_google_data.py (Optimized with Cooldown and Weekly Limit)

import time
import json
import logging
from db_manager import DatabaseConnection, DatabaseManager
from google_provider import GoogleProvider # Assuming google_provider.py is updated

# --- Logger Setup ---
logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

def fetch_restaurants_to_enrich(conn, limit=500): # Default limit for weekly runs
    """
    Fetches restaurants that need enrichment, prioritizing those never attempted
    or whose last attempt was more than 90 days ago.
    """
    logger.info(f"Fetching up to {limit} restaurants needing enrichment (with cooldown)...")
    query = """
        SELECT camis, google_place_id
        FROM restaurants
        WHERE google_place_id IS NOT NULL
        AND google_rating IS NULL
        AND (enrichment_last_attempted IS NULL OR enrichment_last_attempted < NOW() - INTERVAL '90 days')
        GROUP BY camis, google_place_id, enrichment_last_attempted -- <<< ADDED enrichment_last_attempted HERE
        ORDER BY enrichment_last_attempted ASC NULLS FIRST, MAX(inspection_date) DESC
        LIMIT %s;
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (limit,))
            results = cursor.fetchall()
            logger.info(f"Found {len(results)} restaurants to attempt enrichment for.")
            # Convert list of tuples to list of dictionaries for easier access
            return [{'camis': row[0], 'place_id': row[1]} for row in results]
    except Exception as e:
        logger.error(f"Database error while fetching restaurants for enrichment: {e}")
        return []

def update_restaurant_in_db(conn, camis, details):
    """
    Updates all rows for a given restaurant with the new Google Places data.
    (This function remains the same as before)
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
        rating = details.get('rating')
        review_count = details.get('userRatingCount')
        website = details.get('websiteUri')
        hours = json.dumps(details.get('regularOpeningHours'))
        price_level = details.get('priceLevel')

        with conn.cursor() as cursor:
            cursor.execute(update_sql, (rating, review_count, website, hours, price_level, camis))
        # No commit here, let the main function handle transactions per restaurant
        return True
    except Exception as e:
        logger.error(f"Database update failed for CAMIS {camis}: {e}")
        # No rollback here, let the main function handle transactions
        return False

def update_enrichment_timestamp(conn, camis):
    """
    Updates the enrichment_last_attempted timestamp for a given restaurant.
    Called after every enrichment attempt (success or fail).
    """
    query = "UPDATE restaurants SET enrichment_last_attempted = NOW() WHERE camis = %s;"
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (camis,))
        # No commit here, let the main function handle transactions per restaurant
        return True
    except Exception as e:
        logger.error(f"Failed to update enrichment timestamp for CAMIS {camis}: {e}")
        return False

def main():
    """Main function to run the enrichment process."""
    DatabaseManager.initialize_pool()

    try:
        google = GoogleProvider()
    except ValueError as e:
        logger.error(f"Failed to initialize Google Provider: {e}")
        DatabaseManager.close_all_connections()
        return

    try:
        with DatabaseConnection() as conn: # Use a single connection for the batch
            restaurants_to_process = fetch_restaurants_to_enrich(conn, limit=500) # Use the weekly limit
            total_count = len(restaurants_to_process)
            success_count = 0
            fail_count = 0
            skipped_due_error = 0 # Count API/DB errors separately

            if total_count == 0:
                logger.info("No restaurants found that need enrichment at this time.")
            else:
                logger.info(f"--- Starting enrichment for {total_count} restaurants ---")

                for index, restaurant in enumerate(restaurants_to_process):
                    camis = restaurant['camis']
                    place_id = restaurant['place_id']
                    logger.info(f"Processing {index + 1}/{total_count}: CAMIS {camis}")

                    enrichment_succeeded = False
                    details, error = google.get_place_details(place_id)

                    if details and not error:
                        if update_restaurant_in_db(conn, camis, details):
                            success_count += 1
                            enrichment_succeeded = True
                            logger.info(f"  -> SUCCESS: Database updated for CAMIS {camis}.")
                        else:
                            skipped_due_error += 1 # DB update failed
                            conn.rollback() # Rollback only this restaurant's transaction
                    else:
                        fail_count += 1 # API call failed or returned no details
                        logger.warning(f"  -> FAILED: Could not retrieve/process data for CAMIS {camis}. Error: {error}")
                        # No rollback needed as nothing was updated

                    # Update timestamp regardless of success/fail, unless DB error prevented it
                    if not enrichment_succeeded and skipped_due_error > 0 and index+1 == skipped_due_error+success_count+fail_count:
                         logger.warning(f"  -> SKIPPED timestamp update for CAMIS {camis} due to prior DB error.")
                    elif update_enrichment_timestamp(conn, camis):
                         logger.debug(f"  -> Timestamp updated for CAMIS {camis}.")
                    else:
                         skipped_due_error += 1 # Timestamp update failed
                         conn.rollback() # Rollback only this restaurant's transaction

                    # Commit after each restaurant's processing (API call + DB update + timestamp update)
                    conn.commit()
                    time.sleep(0.1) # Small delay

    except Exception as e:
        logger.error(f"An unexpected error occurred during the main process: {e}", exc_info=True)
        # Attempt to rollback if connection is still active
        try:
             if conn and not conn.closed:
                 conn.rollback()
        except Exception as rb_e:
             logger.error(f"Failed during rollback attempt: {rb_e}")
    finally:
        logger.info("--- Enrichment Batch Complete ---")
        if 'total_count' in locals(): # Ensure variables are defined
             logger.info(f"Successfully updated: {success_count}")
             logger.info(f"Failed (API error/No Data): {fail_count}")
             logger.info(f"Skipped (DB error): {skipped_due_error}")
        DatabaseManager.close_all_connections()
        logger.info("Database connection pool closed.")

if __name__ == '__main__':
    main()
