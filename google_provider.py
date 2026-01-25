# In file: google_provider.py

import os
import requests
import logging
from dotenv import load_dotenv # ADD THIS LINE

load_dotenv() # AND ADD THIS LINE

class GoogleProvider:
    def __init__(self):
        self.api_key = os.environ.get("GOOGLE_API_KEY")
        if not self.api_key:
            raise ValueError("GOOGLE_API_KEY environment variable not found.")
        self.base_url = "https://maps.googleapis.com/maps/api/place"
        self.places_v1_url = "https://places.googleapis.com/v1/places"

    def find_place_id(self, name, address):
        """Finds a place ID using a text query (name and address)."""
        if not name or not address:
            return "missing_data", None
            
        url = f"{self.base_url}/findplacefromtext/json"
        params = {
            'input': f"{name} {address}",
            'inputtype': 'textquery',
            'fields': 'place_id',
            'key': self.api_key
        }
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            if data['status'] == 'OK' and data.get('candidates'):
                return "success", data['candidates'][0]['place_id']
            else:
                return "no_match", None
        except requests.RequestException as e:
            logging.error(f"Google API (findplace) request failed: {e}")
            return "failed", None

    def get_place_details(self, place_id):
        """
        Retrieves detailed information for a given place ID using the new v1 API.
        """
        if not place_id:
            return None, "Missing place_id"

        # Define the fields we want to retrieve
        fields = "id,displayName,rating,userRatingCount,websiteUri,regularOpeningHours,priceLevel,dineIn,takeout,delivery"
        
        headers = {
            'X-Goog-Api-Key': self.api_key,
            'X-Goog-FieldMask': fields
        }
        
        url = f"{self.places_v1_url}/{place_id}"

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json(), None
        except requests.exceptions.RequestException as e:
            logging.error(f"Google Places API (details) request failed for {place_id}: {e}")
            return None, str(e)
