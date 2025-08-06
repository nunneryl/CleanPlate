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

        Args:
            name (str): The name of the restaurant.
            address (str): The street address of the restaurant.

        Returns:
            A tuple containing a status string and the Google Place ID (str) or None.
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

        # UPDATED: Increased retries and initial delay for more resilience
        retries = 5
        delay = 5
        for i in range(retries):
            try:
                response = requests.post(url, json=body, headers=headers, timeout=15) # Increased timeout
                
                if response.status_code == 429:
                    raise requests.exceptions.HTTPError(f"Rate limit exceeded (429)")

                response.raise_for_status()
                
                data = response.json()
                
                if "places" in data and data["places"]:
                    place_id = data["places"][0].get("id")
                    self.logger.info(f"Successfully found Google Place ID for {name}: {place_id}")
                    return "success", place_id
                else:
                    self.logger.warning(f"No Google Place ID found for {name}.")
                    return "no_match", None

            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Google request failed (attempt {i+1}/{retries}): {e}")
                if i < retries - 1:
                    time.sleep(delay)
                    delay *= 2 # Exponential backoff continues
                else:
                    self.logger.error(f"Google request failed after {retries} retries.")
                    return "failed", None
        
        return "failed", None
