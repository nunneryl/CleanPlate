# In file: foursquare_provider.py

import os
import time
import requests
import logging
from dotenv import load_dotenv

load_dotenv()

class FoursquareProvider:
    API_KEY = os.environ.get("FOURSQUARE_API_KEY")
    API_HOST = "https://places-api.foursquare.com"

    def __init__(self):
        if not self.API_KEY:
            raise ValueError("Foursquare API key not found in .env file or environment.")
        self.logger = logging.getLogger(__name__)

    def find_match(self, name, latitude=None, longitude=None):
        endpoint = "/places/search"
        url = f"{self.API_HOST}{endpoint}"
        
        params = {"query": name, "limit": 1}
        if not (latitude and longitude):
            return "missing_data", None
        params["ll"] = f"{latitude},{longitude}"

        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.API_KEY}",
            "X-Places-Api-Version": "2025-06-17"
        }

        self.logger.info(f"Querying Foursquare Search for: {name}")

        # --- Retry Logic ---
        retries = 3
        delay = 2
        for i in range(retries):
            try:
                response = requests.get(url, params=params, headers=headers, timeout=10)
                if response.status_code == 429: # Specifically handle rate limiting
                    raise requests.exceptions.HTTPError(f"Rate limit exceeded (429)")

                response.raise_for_status()
                data = response.json()
                
                if "results" in data and data["results"]:
                    return "success", data["results"][0]
                else:
                    return "no_match", None
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Foursquare request failed (attempt {i+1}/{retries}): {e}")
                if i < retries - 1:
                    time.sleep(delay)
                    delay *= 2 # Exponential backoff
                else:
                    self.logger.error(f"Foursquare request failed after {retries} retries.")
                    return "failed", None
