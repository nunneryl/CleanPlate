# backfill_actions.py
import os
import logging
import argparse
from datetime import datetime
import requests
import psycopg

# We need to import the functions and classes from your existing files
from utils import normalize_search_term_for_hybrid
from db_manager import DatabaseManager
from config import APIConfig
# We will also import the batch update function to reuse it
from update_database import update_database_batch, convert_date


# Setup a detailed logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def fetch_data_for_range(start_date, end_date):
    """
    Fetches data from the NYC API for a specific date range.
    """
    logger.info(f"--> Fetching data from NYC API for range: {start_date} to {end_date}...")
    
    # This query is modified to use a specific date range
    query = f"https://data.cityofnewyork.us/resource/43nn-pn8j.json?$where=inspection_date >= '{start_date}T00:00:00.000' AND inspection_date <= '{end_date}T23:59:59.000'&$limit=500000"
    
    try:
        # Increased timeout for potentially larger queries
        response = requests.get(query, timeout=300)
        response.raise_for_status()
        data = response.json()
        logger.info(f"--> Successfully fetched {len(data)} records for this range.")
        return data
    except Exception as e:
        logger.error(f"--> API fetch error for range {start_date}-{end_date}: {e}")
        return []


def run_backfill(start_date, end_date):
    """
    Fetches data for a date range and runs the existing batch update logic on it.
    """
    logger.info(f"--- Starting HISTORICAL BACKFILL for range: {start_date} to {end_date} ---")
    data = fetch_data_for_range(start_date, end_date)
    if data:
        # We are re-using the robust batch update function from your main script
        update_database_batch(data)
    else:
        logger.warning("No data returned from API for this range.")
    logger.info(f"--- FINISHED BACKFILL for range: {start_date} to {end_date} ---")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Backfill 'action' data for a specific date range.")
    parser.add_argument("--start-date", required=True, help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", required=True, help="End date in YYYY-MM-DD format")
    args = parser.parse_args()
    
    # Initialize the DB Manager before running
    DatabaseManager.initialize_pool()
    run_backfill(start_date=args.start_date, end_date=args.end_date)
    DatabaseManager.close_all_connections()
