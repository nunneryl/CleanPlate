# In file: backfill.py

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
    Fetches a limited batch of unique restaurants that have not yet been matched,
    prioritizing the most recently inspected restaurants first.
    """
    logging.info(f"Fetching a batch of up to {limit} unmatched restaurants (prioritizing newest)...")
    
    # UPDATED: This query now prioritizes restaurants by their most recent inspection date.
    query = """
        WITH latest_inspections AS (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY camis ORDER BY inspection_date DESC) as rn
            FROM public.restaurants
        ),
        latest_unique_restaurants AS (
            SELECT * FROM latest_inspections WHERE rn = 1
        )
        SELECT 
            camis, dba, building, street, latitude, longitude
        FROM 
            latest_unique_restaurants
        WHERE 
            foursquare_fsq_id IS NULL
        ORDER BY 
            inspection_date DESC
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
    conn.commit()

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
            logging.info(f"Found {total_restaurants} unique restaurants to process in this batch.")

            if total_restaurants == 0:
                logging.info("No restaurants to process. Backfill may be complete.")
                # We still want to print the final summary, even if it's all zeros.
            else:
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
                            logging.info(f"  -> SUCCESS: Updated DB with FSQ_ID: {fsq_id}, GOOGLE_ID: {google_id}")
                            stats["succeeded"] += 1
                        else:
                            update_restaurant_ids(conn, camis, fsq_id, None)
                            logging.warning(f"  -> PARTIAL SUCCESS: Updated DB with FSQ_ID: {fsq_id}, Google failed.")
                            stats["succeeded"] += 1
                    
                    elif fsq_status == "no_match":
                        stats["no_match"] += 1
                    elif fsq_status == "missing_data":
                        logging.warning(f"  -> SKIPPED: Missing location data in database.")
                        stats["missing_data"] += 1
                    else: # fsq_status == "failed"
                        stats["failed"] += 1
                    
                    time.sleep(0.5)

    except psycopg.Error as e:
        logging.error(f"Database connection error: {e}")
    finally:
        logging.info("--- BATCH COMPLETE ---")
        logging.info(f"Total Restaurants Processed in this run: {sum(stats.values())}")
        logging.info(f"Succeeded: {stats['succeeded']}")
        logging.info(f"No Match Found: {stats['no_match']}")
        logging.info(f"Missing Location Data: {stats['missing_data']}")
        logging.info(f"Failed (Network/API Errors): {stats['failed']}")
        logging.info("-------------------------")

if __name__ == "__main__":
    main()
