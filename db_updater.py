# In file: db_updater.py

import os
import csv
import psycopg
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    load_dotenv()
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        logging.error("DATABASE_URL not found in .env file. Please set it to your PRODUCTION database URL.")
        return

    update_filename = 'update_data.csv'

    try:
        with open(update_filename, 'r') as f:
            reader = csv.DictReader(f)
            updates = list(reader)
    except FileNotFoundError:
        logging.error(f"Error: {update_filename} not found. Please run the export step first.")
        return

    logging.info(f"Found {len(updates)} records in {update_filename} to update.")

    try:
        with psycopg.connect(db_url) as conn:
            for index, row in enumerate(updates):
                camis = row.get('camis')
                fsq_id = row.get('foursquare_fsq_id')
                google_id = row.get('google_place_id')

                if not camis:
                    continue

                logging.info(f"Updating {index + 1}/{len(updates)}: CAMIS {camis}")

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
            logging.info("Database update complete.")

    except psycopg.Error as e:
        logging.error(f"Database connection error: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
