import os
import requests
import logging
import argparse
import traceback # Import traceback for detailed error logging
from datetime import datetime, timedelta
from dateutil.parser import parse as date_parse
from db_manager import DatabaseConnection # Assuming DatabaseConnection handles pool init/get/return
from config import APIConfig

# Suppress LibreSSL warnings (Keep if needed, but often not necessary)
# import warnings
# warnings.filterwarnings("ignore", category=Warning)

# Setup logging (Keep existing setup)
log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "update_database.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()  # This will also print to console
    ]
)
logger = logging.getLogger(__name__)

# --- Added Print Function for Clarity ---
def print_debug(message):
    """Helper function to print debug messages clearly."""
    print(f"---> DEBUG: {message}")
    logger.info(f"---> DEBUG: {message}") # Also log to file

def convert_date(date_str):
    """Convert date string to date object"""
    if not date_str:
        return None
    try:
        dt = date_parse(date_str)
        return dt.date()
    except Exception as e:
        logger.error(f"Error parsing date {date_str}: {e}")
        return None

def fetch_data(days_back=30, max_retries=3):
    """Fetch data from NYC API with pagination"""
    print_debug(f"Entering fetch_data for past {days_back} days...") # DEBUG PRINT
    logger.info(f"Fetching data from the NYC API for the past {days_back} days...")

    results = []
    limit = APIConfig.API_REQUEST_LIMIT
    offset = 0
    total_fetched = 0

    # Calculate date range
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days_back)
    date_filter = f"inspection_date between '{start_date}' and '{end_date}'"
    print_debug(f"Date filter: {date_filter}") # DEBUG PRINT

    while True:
        url = f"{APIConfig.NYC_API_URL}?$limit={limit}&$offset={offset}&$where={date_filter}"
        print_debug(f"Fetching URL: {url}") # DEBUG PRINT

        headers = {}
        if APIConfig.NYC_API_APP_TOKEN:
            headers["X-App-Token"] = APIConfig.NYC_API_APP_TOKEN

        data = None # Initialize data for the loop check
        for attempt in range(max_retries):
            print_debug(f"API fetch attempt {attempt + 1}/{max_retries}...") # DEBUG PRINT
            try:
                response = requests.get(url, headers=headers, timeout=60) # Increased timeout slightly

                print_debug(f"API response status code: {response.status_code}") # DEBUG PRINT
                if response.status_code != 200:
                    logger.error(f"API request failed with status {response.status_code}: {response.text}")
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying attempt {attempt + 2}/{max_retries}...")
                        continue
                    break # Exit retry loop if status != 200 after retries

                data = response.json()

                if not data:
                    print_debug("API returned no data for this offset.") # DEBUG PRINT
                    logger.info("No more data to fetch for this offset.")
                    break # Exit retry loop, outer loop will check 'not data'

                print_debug(f"API fetch successful, got {len(data)} records.") # DEBUG PRINT
                results.extend(data)
                total_fetched += len(data)
                logger.info(f"Fetched {len(data)} records, total: {total_fetched}")

                if len(data) < limit:
                    print_debug("Fetched less than limit, assuming end of data.") # DEBUG PRINT
                    break # Exit retry loop, outer loop will check 'not data'

                # Success, exit retry loop
                break

            except requests.exceptions.Timeout:
                 logger.error(f"Network timeout on attempt {attempt + 1}/{max_retries}")
                 print_debug(f"Network timeout on attempt {attempt + 1}/{max_retries}") # DEBUG PRINT
                 if attempt < max_retries - 1:
                     logger.info(f"Retrying in 5 seconds...")
                     import time
                     time.sleep(5)
                 else:
                     logger.error("Max retries reached after timeout, giving up on this batch")
                     break # Exit retry loop
            except requests.exceptions.RequestException as e:
                logger.error(f"Network error on attempt {attempt + 1}/{max_retries}: {e}")
                print_debug(f"Network error on attempt {attempt + 1}/{max_retries}: {e}") # DEBUG PRINT
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in 5 seconds...")
                    import time
                    time.sleep(5)
                else:
                    logger.error("Max retries reached after network error, giving up on this batch")
                    break # Exit retry loop
            except Exception as e:
                logger.error(f"Unexpected error during fetch attempt {attempt + 1}/{max_retries}: {e}")
                print_debug(f"Unexpected error during fetch attempt {attempt + 1}/{max_retries}: {e}") # DEBUG PRINT
                # Optionally log traceback
                # logger.error(traceback.format_exc())
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in 5 seconds...")
                    import time
                    time.sleep(5)
                else:
                    logger.error("Max retries reached after unexpected error, giving up on this batch")
                    break # Exit retry loop

        # Break the outer loop if the last attempt failed to get data
        if data is None or not data: # Check if data is None (error) or empty (end of results)
            print_debug("Breaking outer fetch loop.") # DEBUG PRINT
            break

        # Successful fetch, increment offset for pagination
        offset += limit

    logger.info(f"Total records fetched: {total_fetched}")
    print_debug(f"Exiting fetch_data. Total fetched: {total_fetched}") # DEBUG PRINT
    return results

# --- Keep fetch_all_data and fetch_restaurant_by_camis as they were ---
# (Or add similar print_debug statements if needed later)
def fetch_all_data(max_retries=3):
    """Fetch all data from NYC API without date filtering"""
    print_debug("Entering fetch_all_data...") # DEBUG PRINT
    logger.info("Fetching ALL data from the NYC API (this may take a while)...")

    results = []
    limit = APIConfig.API_REQUEST_LIMIT # Use a smaller limit for full sync? e.g., 10000
    offset = 0
    total_fetched = 0

    while True:
        url = f"{APIConfig.NYC_API_URL}?$limit={limit}&$offset={offset}"
        print_debug(f"Fetching URL: {url}") # DEBUG PRINT

        headers = {}
        if APIConfig.NYC_API_APP_TOKEN:
            headers["X-App-Token"] = APIConfig.NYC_API_APP_TOKEN

        data = None
        for attempt in range(max_retries):
            print_debug(f"API fetch attempt {attempt + 1}/{max_retries}...") # DEBUG PRINT
            try:
                # Increase timeout for potentially larger full sync requests
                response = requests.get(url, headers=headers, timeout=120)

                print_debug(f"API response status code: {response.status_code}") # DEBUG PRINT
                if response.status_code != 200:
                    logger.error(f"API request failed with status {response.status_code}: {response.text}")
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying attempt {attempt + 2}/{max_retries}...")
                        continue
                    break

                data = response.json()

                if not data:
                    print_debug("API returned no data for this offset.") # DEBUG PRINT
                    logger.info("No more data to fetch")
                    break

                print_debug(f"API fetch successful, got {len(data)} records.") # DEBUG PRINT
                results.extend(data)
                total_fetched += len(data)
                logger.info(f"Fetched {len(data)} records, total: {total_fetched}")

                if len(data) < limit:
                    print_debug("Fetched less than limit, assuming end of data.") # DEBUG PRINT
                    break

                # Success, exit retry loop
                break

            except requests.exceptions.Timeout:
                 logger.error(f"Network timeout on attempt {attempt + 1}/{max_retries}")
                 print_debug(f"Network timeout on attempt {attempt + 1}/{max_retries}") # DEBUG PRINT
                 if attempt < max_retries - 1:
                     logger.info(f"Retrying in 10 seconds...") # Longer wait for full sync
                     import time
                     time.sleep(10)
                 else:
                     logger.error("Max retries reached after timeout, giving up on this batch")
                     break # Exit retry loop
            except requests.exceptions.RequestException as e:
                logger.error(f"Network error on attempt {attempt + 1}/{max_retries}: {e}")
                print_debug(f"Network error on attempt {attempt + 1}/{max_retries}: {e}") # DEBUG PRINT
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in 10 seconds...")
                    import time
                    time.sleep(10)
                else:
                    logger.error("Max retries reached after network error, giving up on this batch")
                    break # Exit retry loop
            except Exception as e:
                logger.error(f"Unexpected error during fetch attempt {attempt + 1}/{max_retries}: {e}")
                print_debug(f"Unexpected error during fetch attempt {attempt + 1}/{max_retries}: {e}") # DEBUG PRINT
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in 10 seconds...")
                    import time
                    time.sleep(10)
                else:
                    logger.error("Max retries reached after unexpected error, giving up on this batch")
                    break # Exit retry loop

        if data is None or not data:
            print_debug("Breaking outer fetch loop.") # DEBUG PRINT
            break

        offset += limit

    logger.info(f"Total records fetched: {total_fetched}")
    print_debug(f"Exiting fetch_all_data. Total fetched: {total_fetched}") # DEBUG PRINT
    return results


def fetch_restaurant_by_camis(camis, max_retries=3):
    """Fetch data for a specific restaurant by CAMIS ID"""
    print_debug(f"Entering fetch_restaurant_by_camis for CAMIS: {camis}") # DEBUG PRINT
    logger.info(f"Fetching all inspections for restaurant CAMIS: {camis}")

    results = []
    limit = 1000  # Should be enough for a single restaurant

    url = f"{APIConfig.NYC_API_URL}?$limit={limit}&$where=camis='{camis}'"
    print_debug(f"Fetching URL: {url}") # DEBUG PRINT

    headers = {}
    if APIConfig.NYC_API_APP_TOKEN:
        headers["X-App-Token"] = APIConfig.NYC_API_APP_TOKEN

    for attempt in range(max_retries):
        print_debug(f"API fetch attempt {attempt + 1}/{max_retries}...") # DEBUG PRINT
        try:
            response = requests.get(url, headers=headers, timeout=30)

            print_debug(f"API response status code: {response.status_code}") # DEBUG PRINT
            if response.status_code != 200:
                logger.error(f"API request failed with status {response.status_code}: {response.text}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying attempt {attempt + 2}/{max_retries}...")
                    continue
                return [] # Return empty list on failure after retries

            data = response.json()
            logger.info(f"Fetched {len(data)} inspections for restaurant CAMIS: {camis}")
            print_debug(f"Exiting fetch_restaurant_by_camis successfully.") # DEBUG PRINT
            return data

        except Exception as e:
            logger.error(f"Error fetching restaurant {camis} on attempt {attempt + 1}/{max_retries}: {e}")
            print_debug(f"Error fetching restaurant {camis} on attempt {attempt + 1}/{max_retries}: {e}") # DEBUG PRINT
            if attempt < max_retries - 1:
                logger.info(f"Retrying in 5 seconds...")
                import time
                time.sleep(5)
            else:
                logger.error("Max retries reached, giving up")
                print_debug(f"Exiting fetch_restaurant_by_camis after max retries.") # DEBUG PRINT
                return [] # Return empty list on failure after retries

    print_debug(f"Exiting fetch_restaurant_by_camis - loop finished unexpectedly.") # DEBUG PRINT
    return [] # Should not be reached if logic is correct


def update_database(data):
    """Update database with fetched data"""
    print_debug("Entering update_database function...") # DEBUG PRINT
    logger.info(f"Updating database with {len(data)} fetched records...")

    restaurants_updated = 0
    violations_updated = 0
    errors = 0
    conn = None # Initialize conn outside try

    try:
        print_debug("Attempting to get DB connection...") # DEBUG PRINT
        # Use the context manager for connection handling
        with DatabaseConnection() as conn:
            print_debug("DB connection acquired successfully.") # DEBUG PRINT
            with conn.cursor() as cursor:
                print_debug("DB cursor acquired.") # DEBUG PRINT
                print_debug(f"Starting loop through {len(data)} items...") # DEBUG PRINT
                for i, item in enumerate(data):
                    # Print progress every 1000 records
                    if (i + 1) % 1000 == 0:
                         print_debug(f"Processing record {i + 1}/{len(data)} (CAMIS: {item.get('camis')})...")

                    try:
                        # --- Prepare Data ---
                        # (Ensure all necessary fields are extracted and converted)
                        camis = item.get("camis")
                        inspection_date = convert_date(item.get("inspection_date"))
                        latitude_val = item.get("latitude")
                        longitude_val = item.get("longitude")

                        # --- Update restaurants table ---
                        # print_debug(f"Executing restaurant upsert for CAMIS {camis}, Date {inspection_date}") # Too verbose
                        cursor.execute("""
                            INSERT INTO restaurants (
                                camis, dba, boro, building, street, zipcode, phone,
                                latitude, longitude, grade, inspection_date, critical_flag,
                                inspection_type, cuisine_description, grade_date
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (camis, inspection_date) DO UPDATE SET
                                dba = EXCLUDED.dba,
                                boro = EXCLUDED.boro,
                                building = EXCLUDED.building,
                                street = EXCLUDED.street,
                                zipcode = EXCLUDED.zipcode,
                                phone = EXCLUDED.phone,
                                latitude = EXCLUDED.latitude,
                                longitude = EXCLUDED.longitude,
                                grade = EXCLUDED.grade,
                                critical_flag = EXCLUDED.critical_flag,
                                inspection_type = EXCLUDED.inspection_type,
                                cuisine_description = EXCLUDED.cuisine_description,
                                grade_date = EXCLUDED.grade_date
                        """, (
                            camis,
                            item.get("dba"),
                            item.get("boro"),
                            item.get("building"),
                            item.get("street"),
                            item.get("zipcode"),
                            item.get("phone"),
                            float(latitude_val) if latitude_val else None,
                            float(longitude_val) if longitude_val else None,
                            item.get("grade"),
                            inspection_date, # Use converted date
                            item.get("critical_flag"),
                            item.get("inspection_type"),
                            item.get("cuisine_description"),
                            convert_date(item.get("grade_date"))
                        ))
                        # Don't count updates here, psycopg2 doesn't easily return upsert counts

                        # --- If there's a violation, update violations table ---
                        violation_code = item.get("violation_code")
                        if violation_code:
                            # print_debug(f"Executing violation insert for CAMIS {camis}, Date {inspection_date}, Code {violation_code}") # Too verbose
                            cursor.execute("""
                                INSERT INTO violations (
                                    camis, inspection_date, violation_code, violation_description
                                ) VALUES (%s, %s, %s, %s)
                                ON CONFLICT DO NOTHING
                            """, (
                                camis,
                                inspection_date, # Use converted date
                                violation_code,
                                item.get("violation_description")
                            ))
                            # Don't count updates here

                    except Exception as e:
                        logger.error(f"Error processing record CAMIS={item.get('camis')}, InspDate={item.get('inspection_date')}: {e}")
                        print_debug(f"ERROR processing record CAMIS={item.get('camis')}: {e}") # DEBUG PRINT
                        errors += 1
                        # Rollback the transaction for this specific item's failure?
                        # Or just log and continue? Current logic continues.
                        # Consider adding conn.rollback() here if one bad record should stop the batch.
                        # For now, we log and continue, attempting commit later.
                        continue # Skip to the next item

                # --- Commit after loop ---
                print_debug(f"Finished loop. Processed {i+1} items. Attempting to commit transaction...") # DEBUG PRINT
                conn.commit()
                print_debug("Transaction committed successfully.") # DEBUG PRINT
                # Note: We don't have accurate update counts with ON CONFLICT without more complex queries
                restaurants_updated = -1 # Indicate unknown update count
                violations_updated = -1 # Indicate unknown update count

        # Log results after commit
        logger.info(f"Database update attempt finished. Errors: {errors}. (Update counts not tracked precisely with ON CONFLICT)")
        print_debug(f"Database update attempt finished. Errors: {errors}. (Update counts N/A)") # DEBUG PRINT
        return restaurants_updated, violations_updated

    except psycopg2.OperationalError as db_op_err:
        # Specific handling for connection errors during the 'with' block itself
        logger.error(f"Database Operational Error during update: {db_op_err}")
        print_debug(f"FATAL: Database Operational Error during update: {db_op_err}") # DEBUG PRINT
        logger.error(traceback.format_exc()) # Log full traceback for DB errors
        # No need to rollback here, context manager handles it on error
        return 0, 0 # Indicate failure
    except Exception as e:
        logger.error(f"Unexpected error during database update: {e}")
        print_debug(f"FATAL: Unexpected error during database update: {e}") # DEBUG PRINT
        logger.error(traceback.format_exc()) # Log full traceback
        # Context manager handles rollback
        return 0, 0 # Indicate failure
    finally:
        # This block executes whether the try block succeeded or failed
        print_debug("Exiting update_database function (finally block).") # DEBUG PRINT
        # Connection is returned automatically by the context manager's __exit__


def update_specific_restaurants(camis_list):
    """Update specific restaurants by CAMIS ID"""
    print_debug(f"Entering update_specific_restaurants for {len(camis_list)} CAMIS IDs.") # DEBUG PRINT
    logger.info(f"Updating {len(camis_list)} specific restaurants...")

    total_restaurants = 0
    total_violations = 0

    for i, camis in enumerate(camis_list):
        print_debug(f"Processing CAMIS {i+1}/{len(camis_list)}: {camis}") # DEBUG PRINT
        data = fetch_restaurant_by_camis(camis)
        if data:
            # update_database returns -1 for counts now, so we can't sum them
            update_database(data)
            # Maybe just count successful updates?
            # total_restaurants += 1 # Count CAMIS IDs processed
        else:
            print_debug(f"No data fetched for CAMIS {camis}")

    logger.info(f"Specific restaurant update complete. Processed {len(camis_list)} CAMIS IDs.")
    print_debug(f"Exiting update_specific_restaurants.") # DEBUG PRINT
    # Return value may need adjustment as counts are not tracked
    return -1, -1

def main():
    print_debug("--- main() started ---") # DEBUG PRINT
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Update restaurant inspection database")
    parser.add_argument("--full-sync", action="store_true", help="Perform full data sync")
    parser.add_argument("--days", type=int, default=30, help="Number of days to look back")
    parser.add_argument("--restaurant", type=str, help="Update specific restaurant by CAMIS ID")
    parser.add_argument("--restaurants-file", type=str, help="File with list of CAMIS IDs to update")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    args = parser.parse_args()

    # Set logging level based on verbose flag
    if args.verbose:
        print_debug("Verbose logging enabled.") # DEBUG PRINT
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("Starting database update process")
    print_debug("Parsed arguments. Starting update logic.") # DEBUG PRINT

    # Update specific restaurants if requested
    if args.restaurant:
        print_debug(f"Mode: Update specific restaurant (CAMIS: {args.restaurant})") # DEBUG PRINT
        data = fetch_restaurant_by_camis(args.restaurant)
        if data:
            update_database(data)
            logger.info(f"Updated restaurant {args.restaurant}")
        else:
            logger.warning(f"No data found for restaurant {args.restaurant}")

    # Update restaurants from file if requested
    elif args.restaurants_file:
        print_debug(f"Mode: Update restaurants from file ({args.restaurants_file})") # DEBUG PRINT
        try:
            with open(args.restaurants_file, 'r') as f:
                camis_list = [line.strip() for line in f if line.strip()]
                print_debug(f"Found {len(camis_list)} CAMIS IDs in file.") # DEBUG PRINT
                update_specific_restaurants(camis_list)
        except Exception as e:
            logger.error(f"Error processing restaurants file: {e}")
            print_debug(f"ERROR processing restaurants file: {e}") # DEBUG PRINT

    # Full sync if requested
    elif args.full_sync:
        print_debug("Mode: Full sync") # DEBUG PRINT
        logger.info("Performing FULL data sync...")
        data = fetch_all_data()
        if data:
            update_database(data)
            logger.info(f"Full sync complete")
        else:
            logger.warning("No data fetched from API for full sync")

    # Otherwise do normal update with specified days
    else:
        print_debug(f"Mode: Incremental update (Days: {args.days})") # DEBUG PRINT
        logger.info(f"Performing incremental update for past {args.days} days...")
        data = fetch_data(days_back=args.days)
        if data:
            update_database(data)
            logger.info(f"Update complete")
        else:
            logger.warning("No data fetched from API")

    logger.info("Database update process completed")
    print_debug("--- main() finished ---") # DEBUG PRINT

if __name__ == "__main__":
    # This block executes when the script is run directly
    print_debug("Script execution started (__name__ == '__main__').") # DEBUG PRINT
    try:
        main()
    except Exception as e:
        # Catch any uncaught exceptions from main()
        print_debug(f"FATAL: Uncaught exception in main: {e}")
        logger.critical(f"Uncaught exception in main: {e}")
        logger.critical(traceback.format_exc())
    finally:
        print_debug("Script execution finished (__name__ == '__main__').") # DEBUG PRINT

