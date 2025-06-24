import os
import re
import requests
import logging
import argparse
from datetime import datetime, timedelta
from dateutil.parser import parse as date_parse
import psycopg
from psycopg.extras import execute_values

from db_manager import DatabaseConnection
from config import APIConfig

logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def normalize_search_term_for_hybrid(text):
    if not isinstance(text, str): return ''
    normalized_text = text.lower().replace('&', ' and ')
    accent_map = { 'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e', 'á': 'a', 'à': 'a', 'â': 'a', 'ä': 'a', 'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i', 'ó': 'o', 'ò': 'o', 'ô': 'o', 'ö': 'o', 'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u', 'ç': 'c', 'ñ': 'n' }
    for accented, unaccented in accent_map.items():
        normalized_text = normalized_text.replace(accented, unaccented)
    normalized_text = re.sub(r"['.]", "", normalized_text)
    normalized_text = re.sub(r"[-/]", " ", normalized_text)
    normalized_text = re.sub(r"[^a-z0-9\s]", "", normalized_text)
    normalized_text = re.sub(r"\s+", " ", normalized_text)
    return normalized_text.strip()

def convert_date(date_str):
    if not date_str or not isinstance(date_str, str): return None
    try:
        return date_parse(date_str).date()
    except (ValueError, TypeError):
        return None

def fetch_data(days_back=15):
    logger.info(f"Fetching data from NYC API for past {days_back} days...")
    query = f"https://data.cityofnewyork.us/resource/43nn-pn8j.json?$where=inspection_date >= '{(datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')}T00:00:00.000'&$limit=50000"
    try:
        response = requests.get(query, timeout=90)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Total records fetched: {len(data)}")
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"API fetch error: {e}")
        return []

def update_database_batch(data):
    if not data: return 0, 0
    
    restaurants_to_insert = []
    violations_to_insert = []

    for item in data:
        camis = item.get("camis")
        inspection_date = convert_date(item.get("inspection_date"))
        if not (camis and inspection_date): continue
        
        dba = item.get("dba")
        # Use the imported normalization function
        normalized_dba = normalize_search_term_for_hybrid(dba) if dba else None

        restaurants_to_insert.append((
            camis, dba, normalized_dba,
            item.get("boro"), item.get("building"), item.get("street"),
            item.get("zipcode"), item.get("phone"),
            float(item.get("latitude")) if item.get("latitude") and item.get("latitude") not in ['N/A', ''] else None,
            float(item.get("longitude")) if item.get("longitude") and item.get("longitude") not in ['N/A', ''] else None,
            item.get("grade"), inspection_date, item.get("critical_flag"),
            item.get("inspection_type"), item.get("cuisine_description"),
            convert_date(item.get("grade_date"))
        ))
        
        if item.get("violation_code"):
             violations_to_insert.append((camis, inspection_date, item.get("violation_code"), item.get("violation_description")))

    r_count, v_count = 0, 0
    with DatabaseConnection() as conn, conn.cursor() as cursor:
        if restaurants_to_insert:
            unique_restaurants = list({(r[0], r[11]): r for r in restaurants_to_insert}.values())
            logger.info(f"Executing batch insert for {len(unique_restaurants)} unique restaurant inspections...")
            upsert_sql = """
                INSERT INTO restaurants (
                    camis, dba, dba_normalized_search, boro, building, street, zipcode, phone,
                    latitude, longitude, grade, inspection_date, critical_flag,
                    inspection_type, cuisine_description, grade_date
                ) VALUES %s ON CONFLICT (camis, inspection_date) DO UPDATE SET
                    dba = EXCLUDED.dba,
                    dba_normalized_search = EXCLUDED.dba_normalized_search,
                    boro = EXCLUDED.boro,
                    grade = EXCLUDED.grade;
            """
            psycopg2.extras.execute_values(cursor, upsert_sql, unique_restaurants, page_size=200)
            r_count = cursor.rowcount
            logger.info(f"Restaurant insert executed. Affected rows: {r_count}")

        if violations_to_insert:
            unique_violations = list(set(violations_to_insert))
            logger.info(f"Executing batch insert for {len(unique_violations)} unique violations...")
            insert_sql = "INSERT INTO violations (camis, inspection_date, violation_code, violation_description) VALUES %s ON CONFLICT DO NOTHING;"
            execute_values(cursor, insert_sql, unique_violations, page_size=1000)
            v_count = cursor.rowcount
            logger.info(f"Violation insert executed. Affected rows: {v_count}")
        
        conn.commit()
        logger.info("DB transaction committed.")

    return r_count, v_count

def run_database_update(days_back=15):
    logger.info(f"Starting DB update (days_back={days_back})")
    data = fetch_data(days_back=days_back)
    if data:
        r_upd, v_ins = update_database_batch(data)
        logger.info(f"Update complete. Restaurants processed: {r_upd}, Violations: {v_ins}")
    else:
        logger.warning("No data from API.")
    logger.info("DB update finished.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Update restaurant inspection database.")
    parser.add_argument("--days", type=int, default=15, help="Number of past days to fetch data for.")
    args = parser.parse_args()
    run_database_update(days_back=args.days)
    logger.info("Script execution finished.")
