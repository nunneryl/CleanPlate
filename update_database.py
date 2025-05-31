# update_database.py - Final version with FTS logic

import os
import requests
import logging
import argparse
import traceback
import psycopg2
import psycopg2.extras
import re
from datetime import datetime, timedelta
from dateutil.parser import parse as date_parse
from db_manager import DatabaseConnection
from config import APIConfig

logger = logging.getLogger(__name__)

def normalize_text(text):
    """
    Normalizes text for FTS by lowercasing, removing specific punctuation,
    and preparing it for to_tsvector.
    """
    if not isinstance(text, str):
        return '' # Return empty string for non-string input
    text = text.lower()
    # Remove characters that would interfere with FTS parsing, but keep apostrophes for now
    text = text.replace('.', ' ').replace('&', ' and ')
    # Collapse multiple spaces and strip
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def print_debug(message):
    logger.info(f"---> SCRIPT DEBUG: {message}")

def convert_date(date_str):
    if not date_str: return None
    try:
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try: return datetime.strptime(date_str, fmt).date()
            except ValueError: continue
        return date_parse(date_str).date()
    except Exception as e:
        logger.warning(f"Could not parse date '{date_str}': {e}")
        return None

def fetch_data(days_back=5, max_retries=4):
    logger.info(f"Fetching data from the NYC API for the past {days_back} days...")
    results = []
    limit = APIConfig.API_REQUEST_LIMIT
    offset = 0
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days_back)
    date_filter = f"inspection_date between '{start_date.strftime('%Y-%m-%d')}T00:00:00.000' and '{end_date.strftime('%Y-%m-%d')}T23:59:59.999'"

    while True:
        base_url = APIConfig.NYC_API_URL
        params = {"$limit": limit, "$offset": offset, "$where": date_filter}
        headers = {"X-App-Token": APIConfig.NYC_API_APP_TOKEN} if APIConfig.NYC_API_APP_TOKEN else {}
        try:
            response = requests.get(base_url, headers=headers, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            if not data: break
            results.extend(data)
            logger.info(f"Fetched {len(data)} records, total so far: {len(results)}")
            if len(data) < limit: break
            offset += len(data)
        except requests.exceptions.RequestException as req_err:
            logger.error(f"Network error during fetch: {req_err}")
            break
    logger.info(f"Total records fetched: {len(results)}")
    return results

def update_database_batch(data):
    if not data:
        logger.info("No data provided to update_database_batch.")
        return 0, 0

    logger.info(f"Preparing batch update for {len(data)} fetched records...")
    restaurants_to_upsert = []
    violations_to_insert = []
    processed_restaurant_keys = set()

    for item in data:
        try:
            camis = item.get("camis")
            dba = item.get("dba")
            normalized_dba = normalize_text(dba) # Normalize for FTS
            inspection_date = convert_date(item.get("inspection_date"))
            restaurant_key = (camis, inspection_date)

            if camis and inspection_date and restaurant_key not in processed_restaurant_keys:
                restaurant_tuple = (
                    camis, dba, item.get("boro"), item.get("building"), item.get("street"),
                    item.get("zipcode"), item.get("phone"),
                    float(item.get("latitude", 0)), float(item.get("longitude", 0)),
                    item.get("grade"), inspection_date, item.get("critical_flag"),
                    item.get("inspection_type"), item.get("cuisine_description"),
                    convert_date(item.get("grade_date")),
                    normalized_dba # Pass the normalized string to be used by to_tsvector
                )
                restaurants_to_upsert.append(restaurant_tuple)
                processed_restaurant_keys.add(restaurant_key)

            if camis and inspection_date and item.get("violation_code"):
                 violations_to_insert.append((camis, inspection_date, item.get("violation_code"), item.get("violation_description")))
        except Exception as e:
            logger.error(f"Error preparing record CAMIS={item.get('camis')} for batch: {e}", exc_info=True)

    success = False
    try:
        with DatabaseConnection() as conn:
            with conn.cursor() as cursor:
                if restaurants_to_upsert:
                    logger.info(f"Executing batch upsert for {len(restaurants_to_upsert)} restaurants...")
                    # Note the use of to_tsvector on the last parameter
                    upsert_sql = """
                        INSERT INTO restaurants (
                            camis, dba, boro, building, street, zipcode, phone,
                            latitude, longitude, grade, inspection_date, critical_flag,
                            inspection_type, cuisine_description, grade_date, dba_tsv
                        )
                        VALUES %s
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
                            grade_date = EXCLUDED.grade_date,
                            dba_tsv = to_tsvector('english', EXCLUDED.dba);
                    """
                    # We modify the tuple to call the function inside SQL
                    # This is slightly complex with execute_values, so we'll do it a different way.
                    # The above SQL is slightly wrong for execute_values. Let's build it right.

                    final_upsert_sql = """
                        INSERT INTO restaurants (
                            camis, dba, boro, building, street, zipcode, phone,
                            latitude, longitude, grade, inspection_date, critical_flag,
                            inspection_type, cuisine_description, grade_date, dba_tsv
                        )
                        SELECT
                            p.camis, p.dba, p.boro, p.building, p.street, p.zipcode, p.phone,
                            p.latitude, p.longitude, p.grade, p.inspection_date, p.critical_flag,
                            p.inspection_type, p.cuisine_description, p.grade_date,
                            to_tsvector('english', p.normalized_dba)
                        FROM (VALUES %s) AS p(
                            camis, dba, boro, building, street, zipcode, phone,
                            latitude, longitude, grade, inspection_date, critical_flag,
                            inspection_type, cuisine_description, grade_date, normalized_dba
                        )
                        ON CONFLICT (camis, inspection_date) DO UPDATE SET
                            dba = EXCLUDED.dba,
                            boro = EXCLUDED.boro,
                            street = EXCLUDED.street,
                            zipcode = EXCLUDED.zipcode,
                            phone = EXCLUDED.phone,
                            grade = EXCLUDED.grade,
                            cuisine_description = EXCLUDED.cuisine_description,
                            dba_tsv = to_tsvector('english', EXCLUDED.dba);
                    """

                    psycopg2.extras.execute_values(cursor, final_upsert_sql, restaurants_to_upsert, template=None, page_size=100)
                    logger.info("Batch restaurant upsert executed.")

                if violations_to_insert:
                    logger.info(f"Executing batch insert for {len(violations_to_insert)} violations...")
                    insert_sql = "INSERT INTO violations (camis, inspection_date, violation_code, violation_description) VALUES %s ON CONFLICT DO NOTHING;"
                    psycopg2.extras.execute_values(cursor, insert_sql, violations_to_insert, template=None, page_size=100)
                    logger.info("Batch violation insert executed.")

                conn.commit()
                success = True
                logger.info("Database transaction committed successfully.")
    except Exception as e:
        logger.error(f"Unexpected error during batch database update: {e}", exc_info=True)
        if conn: conn.rollback()

    return len(restaurants_to_upsert) if success else 0, len(violations_to_insert) if success else 0

def run_database_update(days_back=5):
    logger.info(f"Starting database update process (days_back={days_back})")
    try:
        data = fetch_data(days_back=days_back)
        if data:
            restaurants_updated, violations_inserted = update_database_batch(data)
            logger.info(f"Update complete. Restaurants processed: {restaurants_updated}, Violations processed: {violations_inserted}")
        else:
            logger.warning("No data fetched from API to update.")
    except Exception as e:
        logger.critical(f"Uncaught exception in run_database_update: {e}", exc_info=True)
    finally:
        logger.info("Database update process finished.")
