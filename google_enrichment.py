# In file: google_enrichment.py

import os
import requests
import time
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Access the API key
API_KEY = os.getenv("Maps_API_KEY")
TEXT_SEARCH_ENDPOINT = "https://places.googleapis.com/v1/places:searchText"

def load_foursquare_data(filepath: str) -> pd.DataFrame:
    """Loads venue data from a CSV file into a pandas DataFrame."""
    try:
        df = pd.read_csv(filepath)
        required_cols = ['name', 'address', 'city', 'state', 'latitude', 'longitude']
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"CSV must contain the following columns: {required_cols}")
        print(f"Successfully loaded {len(df)} venues from {filepath}")
        return df
    except FileNotFoundError:
        print(f"Error: The file at {filepath} was not found.")
        return pd.DataFrame()

def get_google_place_id(session: requests.Session, venue_data: pd.Series) -> str | None:
    """
    Queries the Google Places Text Search API to find a place_id for a given venue.
    Implements exponential backoff for retries.
    """
    if not API_KEY:
        raise ValueError("Google Maps API key not found. Please set Maps_API_KEY in the .env file.")

    # Construct the query using the "business with address" prefix for accuracy
    query = f"business with address {venue_data['name']}, {venue_data['address']}, {venue_data['city']}, {venue_data['state']}"
    
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": API_KEY,
        "X-Goog-FieldMask": "places.id" # Request only the Place ID to ensure the call is free
    }
    
    payload = {
        "textQuery": query,
        "locationBias": {
            "circle": {
                "center": {
                    "latitude": venue_data['latitude'],
                    "longitude": venue_data['longitude']
                },
                "radius": 500.0
            }
        }
    }

    max_retries = 3
    base_delay = 1 # seconds

    for attempt in range(max_retries):
        try:
            response = session.post(TEXT_SEARCH_ENDPOINT, json=payload, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if "places" in data and len(data["places"]) > 0:
                return data["places"][0].get("id")
            else:
                return None # No match found

        except requests.exceptions.RequestException as e:
            print(f"  -> Request failed for '{venue_data['name']}': {e}. Attempt {attempt + 1} of {max_retries}.")
            if attempt < max_retries - 1:
                time.sleep(base_delay * (2 ** attempt)) # Exponential backoff
            else:
                print(f"  -> Max retries reached for '{venue_data['name']}'.")
                return None
    return None

def enrich_venues_with_place_ids(df: pd.DataFrame) -> pd.DataFrame:
    """
    Iterates through a DataFrame of venues and adds a 'google_place_id' column.
    """
    if df.empty:
        return df

    # Use a copy to avoid SettingWithCopyWarning
    df_copy = df.copy()
    df_copy['google_place_id'] = None
    
    with requests.Session() as session:
        for index, row in df_copy.iterrows():
            print(f"Processing {index + 1}/{len(df_copy)}: {row['name']}...")
            place_id = get_google_place_id(session, row)
            if place_id:
                df_copy.at[index, 'google_place_id'] = place_id
                print(f"  -> Found Place ID: {place_id}")
            else:
                print(f"  -> No Place ID found.")
            
            time.sleep(0.1) # Small delay to be polite to the API
            
    return df_copy

# --- Main Execution ---
if __name__ == "__main__":
    foursquare_df = load_foursquare_data('foursquare_venues.csv')
    if not foursquare_df.empty:
        enriched_df = enrich_venues_with_place_ids(foursquare_df)
        
        # Save the enriched data to a new CSV file
        enriched_df.to_csv('enriched_venues.csv', index=False)
        print("\nEnrichment complete. Results saved to 'enriched_venues.csv'.")
