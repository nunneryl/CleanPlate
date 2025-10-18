# In file: backfill.py (Optimized with Cooldown)

import os
import time
import psycopg
import logging
from dotenv import load_dotenv
from foursquare_provider import FoursquareProvider
from google_provider import GoogleProvider

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_unmatched_restaurants(conn, limit=1000):
    """
    Fetches a batch of unique restaurants that need a Google Place ID,
    prioritizing those that have never been checked, then those whose last check has expired.
    """
    logging.info(f"Fetching a batch of up to {limit} restaurants to check for Google Place IDs...")

    # This updated query implements the "cooldown" logic.
    query = """
        WITH latest_inspections AS (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY camis ORDER BY inspection_date DESC) as rn
            FROM public.restaurants
        ),
        latest_unique_restaurants AS (
            SELECT camis, dba, building, street, latitude, longitude, google_id_last_checked, google_place_id, inspection_date -- <<< ADDED inspection_date HERE
            FROM latest_inspections
            WHERE rn = 1
        )
        SELECT
            camis, dba, building, street, latitude, longitude
        FROM
            latest_unique_restaurants
        WHERE
            google_place_id IS NULL AND
            (google_id_last_checked IS NULL OR google_id_last_checked < NOW() - INTERVAL '90 days')
        ORDER BY
            google_id_last_checked ASC NULLS FIRST, -- Prioritize ones we've never checked
            inspection_date DESC -- <<< This line can now correctly reference the column
        LIMIT %s;
    """
    with conn.cursor() as cur:
        cur.execute(query, (limit,))
        return cur.fetchall()

def update_restaurant_ids(conn, camis, fsq_id, google_id):
    """
    Updates all inspection rows for a given restaurant with the matched IDs.
    """
    query = """
        UPDATE public.restaurants
        SET 
            foursquare_fsq_id = %s,
            google_place_id = %s
        WHERE 
            camis = %s;
    """
    with conn.cursor() as cur:
        cur.execute(query, (fsq_id, google_id, camis))

def update_last_checked_timestamp(conn, camis):
    """
    Updates the google_id_last_checked timestamp for a given restaurant.
    This is called after every attempt, success or fail.
    """
    query = "UPDATE public.restaurants SET google_id_last_checked = NOW() WHERE camis = %s;"
    with conn.cursor() as cur:
        cur.execute(query, (camis,))

def main():
    """Main function to run the backfill process."""
    load_dotenv()
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        logging.error("DATABASE_URL not found.")
        return

    try:
        foursquare = FoursquareProvider()
        google = GoogleProvider()
    except ValueError as e:
        logging.error(f"Failed to initialize API providers: {e}")
        return

    stats = {"succeeded": 0, "no_match": 0, "missing_data": 0, "failed": 0}
    
    try:
        with psycopg.connect(db_url) as conn:
            restaurants_to_process = get_unmatched_restaurants(conn)
            total_restaurants = len(restaurants_to_process)
            logging.info(f"Found {total_restaurants} restaurants to process in this batch.")

            if total_restaurants > 0:
                for index, restaurant in enumerate(restaurants_to_process):
                    camis, dba, building, street, latitude, longitude = restaurant
                    logging.info(f"Processing {index + 1}/{total_restaurants}: {dba} (CAMIS: {camis})")
                    
                    fsq_status, fsq_data = foursquare.find_match(name=dba, latitude=latitude, longitude=longitude)
                    
                    if fsq_status == "success":
                        fsq_id = fsq_data.get("fsq_place_id")
                        full_address = f"{building} {street}"
                        
                        google_status, google_id = google.find_place_id(name=dba, address=full_address)
                        
                        if google_status == "success":
                            update_restaurant_ids(conn, camis, fsq_id, google_id)
                            logging.info(f"  -> SUCCESS: Found and updated IDs.")
                            stats["succeeded"] += 1
                        else:
                            update_restaurant_ids(conn, camis, fsq_id, None) # Still save Foursquare ID
                            logging.warning(f"  -> PARTIAL: Found Foursquare ID but no Google ID.")
                            stats["no_match"] += 1
                    
                    elif fsq_status == "no_match":
                        stats["no_match"] += 1
                    elif fsq_status == "missing_data":
                        stats["missing_data"] += 1
                    else: # fsq_status == "failed"
                        stats["failed"] += 1
                    
                    # CRITICAL: Update the timestamp regardless of the outcome
                    update_last_checked_timestamp(conn, camis)
                    conn.commit()
                    
                    time.sleep(0.5)

    except psycopg.Error as e:
        logging.error(f"Database connection error: {e}")
    finally:
        logging.info("--- BATCH COMPLETE ---")
        logging.info(f"Total Processed: {sum(stats.values())}, Succeeded: {stats['succeeded']}, No Match: {stats['no_match']}")
        logging.info("-------------------------")

if __name__ == "__main__":
    main()
