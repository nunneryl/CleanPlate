# In file: matching_tester.py

import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---

# Load API key securely from environment variables
# This should be your NEW "Service API Key"
FOURSQUARE_API_KEY = os.environ.get("FOURSQUARE_API_KEY")

# Sample restaurant data to test
SAMPLE_RESTAURANTS = [
    {"name": "Katz's Delicatessen", "address": "205 E Houston St, New York, NY 10002"}
]

# --- API FUNCTION ---

def test_foursquare(name, address):
    print("\n--- Testing Foursquare (New API) ---")
    if not FOURSQUARE_API_KEY:
        print("Foursquare API Key not found in .env file or environment.")
        return

    # UPDATED: New endpoint URL with new host and no /v3/
    url = "https://places-api.foursquare.com/places/match"
    
    params = {
        "name": name,
        "address": address,
        "city": "New York",
        "state": "NY",
    }
    headers = {
        "Accept": "application/json",
        # UPDATED: Added "Bearer " prefix as required by the new auth method
        "Authorization": f"Bearer {FOURSQUARE_API_KEY}",
        # UPDATED: Added new required versioning header
        "X-Places-Api-Version": "2025-06-17"
    }
    
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status() # Raise an exception for bad status codes
        data = response.json()
        print("--- SUCCESS ---")
        print(json.dumps(data, indent=2))
    except requests.exceptions.RequestException as e:
        print("--- ERROR ---")
        print(f"Error calling Foursquare API: {e}")
        if e.response:
            print(f"Response Body: {e.response.text}")

# --- MAIN SCRIPT ---

if __name__ == "__main__":
    for restaurant in SAMPLE_RESTAURANTS:
        print("======================================================")
        print(f"Querying for: {restaurant['name']} at {restaurant['address']}")
        print("======================================================")
        
        test_foursquare(restaurant['name'], restaurant['address'])
        
    print("\nScript finished.")
