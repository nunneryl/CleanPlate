# In file: google_provider.py

import os
import time
import requests
import logging
from dotenv import load_dotenv

load_dotenv()

class GoogleProvider:
    """
    A provider class to interact with the Google Places API.
    """
    API_KEY = os.environ.get("Maps_API_KEY")
    API_HOST = "https://places.googleapis.com"

    def __init__(self):
        if not self.API_KEY:
            raise ValueError("Maps_API_KEY not found in .env file or environment.")
        self.logger = logging.getLogger(__name__)

    def find_place_id(self, name, address):
        """
        Finds the Google Place ID for a given restaurant.
        """
        endpoint = "/v1/places:searchText"
        url = f"{self.API_HOST}{endpoint}"
        
        query = f"{name} {address}"
        
        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self.API_KEY,
            "X-Goog-FieldMask": "places.id"
        }
        body = {
            "textQuery": query
        }

        self.logger.info(f"Querying Google for: {name}")

        retries = 5
        delay = 5
        for i in range(retries):
            try:
                response = requests.post(url, json=body, headers=headers, timeout=15)
                
                # --- NEW: Smart error handling ---
                # Check for permanent client-side errors (4xx) before retrying.
                if 400 <= response.status_code < 500:
                    if response.status_code == 403:
                        self.logger.error(f"Google API request is Forbidden (403). This is a permanent error. Check API key restrictions (IP, Bundle ID) or billing status.")
                    else:
                        self.logger.error(f"Google API returned a client error: {response.status_code}. This will not be retried.")
                    return "failed", None # Fail immediately, do not retry
                # --- END NEW ---

                response.raise_for_status() # This will trigger the 'except' block for 5xx server errors
                
                data = response.json()
                
                if "places" in data and data["places"]:
                    place_id = data["places"][0].get("id")
                    self.logger.info(f"Successfully found Google Place ID for {name}: {place_id}")
                    return "success", place_id
                else:
                    self.logger.warning(f"No Google Place ID found for {name}.")
                    return "no_match", None

            except requests.exceptions.RequestException as e:
                # This block now only handles transient errors (network issues, 5xx server errors)
                self.logger.warning(f"Google request failed with a transient error (attempt {i+1}/{retries}): {e}")
                if i < retries - 1:
                    time.sleep(delay)
                    delay *= 2
                else:
                    self.logger.error(f"Google request failed after {retries} retries.")
                    return "failed", None
        
        return "failed", None
