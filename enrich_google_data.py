# In file: enrich_google_data.py

import os
import time
import json
import logging
import requests
from db_manager import DatabaseConnection, DatabaseManager

# --- Logger Setup ---
logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# --- Configuration ---
# IMPORTANT: Replace these with your actual Apify details
APIFY_API_TOKEN = 'apify_api_TRJ8FvvJyrS1A1W01GiMzsc25kyGN04zROiD' # Find in your Apify account settings -> Integrations
# This is the ID of your 'Google Maps Scraper' actor.
# You can find it in the Apify console URL (e.g., .../actors/XYZ/...)
ACTOR_ID = 'apify/google-maps-scraper'

def fetch_restaurants_to_enrich(limit=500):
    """
    Fetches restaurants that have a google_place_id but haven't been enriched yet.
    """
    logger.info("Fetching restaurants needing enrichment from the database...")
    query = """
        SELECT camis, google_place_id
        FROM restaurants
        WHERE google_place_id IS NOT NULL
        AND google_rating IS NULL -- This is our check to see if it's been enriched
        GROUP BY camis, google_place_id -- Ensures we only process each restaurant once
        ORDER BY MAX(inspection_date) DESC -- Prioritize most recently inspected
        LIMIT %s;
    """
    try:
        with DatabaseConnection() as conn, conn.cursor() as cursor:
            cursor.execute(query, (limit,))
            results = cursor.fetchall()
            logger.info(f"Found {len(results)} restaurants to enrich in this batch.")
            return results
    except Exception as e:
        logger.error(f"Database error while fetching restaurants: {e}")
        return []

def run_apify_scrape(place_id):
    """
    Triggers an Apify actor run for a single Google Place ID and retrieves the result.
    """
    if not APIFY_API_TOKEN or 'YOUR' in APIFY_API_TOKEN:
        logger.error("APIFY_API_TOKEN is not configured. Please set it at the top of the script.")
        return None

    if not ACTOR_ID or 'YOUR' in ACTOR_ID:
        logger.error("ACTOR_ID is not configured. Please set it at the top of the script.")
        return None

    run_url = f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs?token={APIFY_API_TOKEN}"
    
    # Input for the Google Maps Scraper actor
    run_input = {
        "startUrls": [],
        "placeIds": [place_id], # We provide just the one ID
        "maxCrawledPlaces": 1
    }

    try:
        logger.info(f"Starting Apify scrape for placeId: {place_id}")
        response = requests.post(run_url, json=run_input, timeout=30)
        response.raise_for_status()
        
        run_data = response.json().get('data', {})
        dataset_id = run_data.get('defaultDatasetId')
        run_id = run_data.get('id')
        
        if not dataset_id or not run_id:
            logger.error("Failed to get datasetId or runId from Apify run start.")
            return None

        # Poll for run completion
        status_url = f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs/{run_id}?token={APIFY_API_TOKEN}"
        while True:
            status_response = requests.get(status_url, timeout=30)
            status_response.raise_for_status()
            status_data = status_response.json().get('data', {})
            if status_data.get('status') in ['SUCCEEDED', 'FAILED', 'TIMED_OUT']:
                break
            time.sleep(5) # Wait 5 seconds before checking again

        if status_data.get('status') != 'SUCCEEDED':
            logger.error(f"Apify run for {place_id} did not succeed. Status: {status_data.get('status')}")
            return None
        
        # Fetch results from the dataset
        items_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={APIFY_API_TOKEN}"
        items_response = requests.get(items_url, timeout=30)
        items_response.raise_for_status()
        
        items = items_response.json()
        if items and len(items) > 0:
            logger.info(f"Successfully scraped data for {place_id}.")
            return items[0] # Return the first (and only) result
        else:
            logger.warning(f"Scrape succeeded but returned no items for {place_id}.")
            return None

    except requests.RequestException as e:
        logger.error(f"API call to Apify failed: {e}")
        return None

def update_restaurant_in_db(conn, camis, scraped_data):
    """
    Updates all rows for a given restaurant with the new scraped data.
    """
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
    try:
        with conn.cursor() as cursor:
            cursor.execute(update_sql, (
                scraped_data.get('totalScore'),
                scraped_data.get('reviewsCount'),
                scraped_data.get('website'),
                json.dumps(scraped_data.get('openingHours')),
                scraped_data.get('url'),
                scraped_data.get('price'),
                camis
            ))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Database update failed for CAMIS {camis}: {e}")
        conn.rollback()
        return False

def main():
    """Main function to run the enrichment process."""
    DatabaseManager.initialize_pool()
    
    restaurants_to_process = fetch_restaurants_to_enrich(limit=500) # Process in batches of 500
    total_count = len(restaurants_to_process)
    success_count = 0
    fail_count = 0

    if total_count == 0:
        logger.info("No restaurants found that need enrichment. The database is up to date.")
    else:
        logger.info(f"--- Starting enrichment process for {total_count} restaurants ---")

        with DatabaseConnection() as conn:
            for index, (camis, place_id) in enumerate(restaurants_to_process):
                logger.info(f"Processing {index + 1}/{total_count}: CAMIS {camis}, Place ID {place_id}")
                
                scraped_data = run_apify_scrape(place_id)
                
                if scraped_data:
                    if update_restaurant_in_db(conn, camis, scraped_data):
                        success_count += 1
                        logger.info(f"  -> SUCCESS: Database updated for CAMIS {camis}.")
                    else:
                        fail_count += 1
                else:
                    fail_count += 1
                    logger.warning(f"  -> FAILED: Could not retrieve data for CAMIS {camis}.")

                time.sleep(2) # Be a good citizen and don't hammer the API

    logger.info("--- Enrichment Batch Complete ---")
    logger.info(f"Successfully updated: {success_count}")
    logger.info(f"Failed or skipped: {fail_count}")
    
    DatabaseManager.close_all_connections()
    logger.info("Database connection pool closed.")

if __name__ == '__main__':
    main()
