import os
import requests
import logging
import argparse
from datetime import datetime, timedelta
from dateutil.parser import parse as date_parse
from db_manager import DatabaseConnection
from config import APIConfig

# Suppress LibreSSL warnings
import warnings
warnings.filterwarnings("ignore", category=Warning)

# Setup logging
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
    logger.info(f"Fetching data from the NYC API for the past {days_back} days...")
    
    results = []
    limit = APIConfig.API_REQUEST_LIMIT
    offset = 0
    total_fetched = 0
    
    # Calculate date range
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days_back)
    date_filter = f"inspection_date between '{start_date}' and '{end_date}'"
    
    while True:
        url = f"{APIConfig.NYC_API_URL}?$limit={limit}&$offset={offset}&$where={date_filter}"
        
        headers = {}
        if APIConfig.NYC_API_APP_TOKEN:
            headers["X-App-Token"] = APIConfig.NYC_API_APP_TOKEN
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=headers, timeout=30)
                
                if response.status_code != 200:
                    logger.error(f"API request failed with status {response.status_code}: {response.text}")
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying attempt {attempt + 2}/{max_retries}...")
                        continue
                    break
                
                data = response.json()
                
                if not data:
                    logger.info("No more data to fetch")
                    break
                
                results.extend(data)
                total_fetched += len(data)
                logger.info(f"Fetched {len(data)} records, total: {total_fetched}")
                
                if len(data) < limit:
                    break
                
                # Success, no need to retry
                break
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Network error on attempt {attempt + 1}/{max_retries}: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in 5 seconds...")
                    import time
                    time.sleep(5)
                else:
                    logger.error("Max retries reached, giving up on this batch")
                    break
            except Exception as e:
                logger.error(f"Unexpected error on attempt {attempt + 1}/{max_retries}: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in 5 seconds...")
                    import time
                    time.sleep(5)
                else:
                    logger.error("Max retries reached, giving up on this batch")
                    break
        
        # Break the outer loop if we didn't get data
        if not data:
            break
            
        # Successful fetch, increment offset for pagination
        offset += limit
            
    logger.info(f"Total records fetched: {total_fetched}")
    return results

def fetch_all_data(max_retries=3):
    """Fetch all data from NYC API without date filtering"""
    logger.info("Fetching ALL data from the NYC API (this may take a while)...")
    
    results = []
    limit = APIConfig.API_REQUEST_LIMIT
    offset = 0
    total_fetched = 0
    
    while True:
        url = f"{APIConfig.NYC_API_URL}?$limit={limit}&$offset={offset}"
        
        headers = {}
        if APIConfig.NYC_API_APP_TOKEN:
            headers["X-App-Token"] = APIConfig.NYC_API_APP_TOKEN
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=headers, timeout=30)
                
                if response.status_code != 200:
                    logger.error(f"API request failed with status {response.status_code}: {response.text}")
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying attempt {attempt + 2}/{max_retries}...")
                        continue
                    break
                
                data = response.json()
                
                if not data:
                    logger.info("No more data to fetch")
                    break
                
                results.extend(data)
                total_fetched += len(data)
                logger.info(f"Fetched {len(data)} records, total: {total_fetched}")
                
                if len(data) < limit:
                    break
                
                # Success, no need to retry
                break
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Network error on attempt {attempt + 1}/{max_retries}: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in 5 seconds...")
                    import time
                    time.sleep(5)
                else:
                    logger.error("Max retries reached, giving up on this batch")
                    break
            except Exception as e:
                logger.error(f"Unexpected error on attempt {attempt + 1}/{max_retries}: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in 5 seconds...")
                    import time
                    time.sleep(5)
                else:
                    logger.error("Max retries reached, giving up on this batch")
                    break
        
        # Break the outer loop if we didn't get data
        if not data:
            break
            
        # Successful fetch, increment offset for pagination
        offset += limit
            
    logger.info(f"Total records fetched: {total_fetched}")
    return results

def fetch_restaurant_by_camis(camis, max_retries=3):
    """Fetch data for a specific restaurant by CAMIS ID"""
    logger.info(f"Fetching all inspections for restaurant CAMIS: {camis}")
    
    results = []
    limit = 1000  # Should be enough for a single restaurant
    
    url = f"{APIConfig.NYC_API_URL}?$limit={limit}&$where=camis='{camis}'"
    
    headers = {}
    if APIConfig.NYC_API_APP_TOKEN:
        headers["X-App-Token"] = APIConfig.NYC_API_APP_TOKEN
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code != 200:
                logger.error(f"API request failed with status {response.status_code}: {response.text}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying attempt {attempt + 2}/{max_retries}...")
                    continue
                return []
            
            data = response.json()
            logger.info(f"Fetched {len(data)} inspections for restaurant CAMIS: {camis}")
            return data
            
        except Exception as e:
            logger.error(f"Error fetching restaurant {camis} on attempt {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in 5 seconds...")
                import time
                time.sleep(5)
            else:
                logger.error("Max retries reached, giving up")
                return []
    
    return []

def update_database(data):
    """Update database with fetched data"""
    logger.info("Updating database with fetched data...")
    
    restaurants_updated = 0
    violations_updated = 0
    errors = 0
    
    try:
        with DatabaseConnection() as conn:
            with conn.cursor() as cursor:
                for item in data:
                    try:
                        # Update restaurants table
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
                            item.get("camis"),
                            item.get("dba"),
                            item.get("boro"),
                            item.get("building"),
                            item.get("street"),
                            item.get("zipcode"),
                            item.get("phone"),
                            float(item.get("latitude")) if item.get("latitude") else None,
                            float(item.get("longitude")) if item.get("longitude") else None,
                            item.get("grade"),
                            convert_date(item.get("inspection_date")),
                            item.get("critical_flag"),
                            item.get("inspection_type"),
                            item.get("cuisine_description"),
                            convert_date(item.get("grade_date"))
                        ))
                        restaurants_updated += 1
                        
                        # If there's a violation, update violations table
                        if item.get("violation_code"):
                            cursor.execute("""
                                INSERT INTO violations (
                                    camis, inspection_date, violation_code, violation_description
                                ) VALUES (%s, %s, %s, %s)
                                ON CONFLICT DO NOTHING
                            """, (
                                item.get("camis"),
                                convert_date(item.get("inspection_date")),
                                item.get("violation_code"),
                                item.get("violation_description")
                            ))
                            violations_updated += 1
                            
                    except Exception as e:
                        logger.error(f"Error processing record {item.get('camis')}: {e}")
                        errors += 1
                        # Try to commit what we have so far before continuing
                        try:
                            conn.commit()
                        except:
                            pass
                        continue
                
                conn.commit()
                
        logger.info(f"Database update complete. Restaurants: {restaurants_updated}, Violations: {violations_updated}, Errors: {errors}")
        return restaurants_updated, violations_updated
        
    except Exception as e:
        logger.error(f"Error updating database: {e}")
        return 0, 0

def update_specific_restaurants(camis_list):
    """Update specific restaurants by CAMIS ID"""
    logger.info(f"Updating {len(camis_list)} specific restaurants...")
    
    total_restaurants = 0
    total_violations = 0
    
    for camis in camis_list:
        data = fetch_restaurant_by_camis(camis)
        if data:
            restaurants, violations = update_database(data)
            total_restaurants += restaurants
            total_violations += violations
    
    logger.info(f"Specific restaurant update complete. Total restaurants: {total_restaurants}, Total violations: {total_violations}")
    return total_restaurants, total_violations

def main():
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
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info("Starting database update process")
    
    # Update specific restaurants if requested
    if args.restaurant:
        data = fetch_restaurant_by_camis(args.restaurant)
        if data:
            restaurants, violations = update_database(data)
            logger.info(f"Updated restaurant {args.restaurant}: {restaurants} records, {violations} violations")
        else:
            logger.warning(f"No data found for restaurant {args.restaurant}")
            
    # Update restaurants from file if requested
    elif args.restaurants_file:
        try:
            with open(args.restaurants_file, 'r') as f:
                camis_list = [line.strip() for line in f if line.strip()]
                update_specific_restaurants(camis_list)
        except Exception as e:
            logger.error(f"Error processing restaurants file: {e}")
            
    # Full sync if requested
    elif args.full_sync:
        logger.info("Performing FULL data sync...")
        data = fetch_all_data()
        if data:
            restaurants, violations = update_database(data)
            logger.info(f"Full sync complete: {restaurants} restaurants and {violations} violations processed")
        else:
            logger.warning("No data fetched from API for full sync")
            
    # Otherwise do normal update with specified days
    else:
        logger.info(f"Performing incremental update for past {args.days} days...")
        data = fetch_data(days_back=args.days)
        if data:
            restaurants, violations = update_database(data)
            logger.info(f"Update complete: {restaurants} restaurants and {violations} violations processed")
        else:
            logger.warning("No data fetched from API")
    
    logger.info("Database update process completed")

if __name__ == "__main__":
    main()
