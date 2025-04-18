# In config.py
import os
from dotenv import load_dotenv
load_dotenv()

class SentryConfig:
    SENTRY_DSN = os.environ.get("SENTRY_DSN", None) # Returns None if not set

# Database configuration
class DatabaseConfig:
    DB_NAME = os.environ.get("DB_NAME", "nyc_restaurant_db")
    DB_USER = os.environ.get("DB_USER", "postgres")
    DB_PASSWORD = os.environ.get("DB_PASSWORD", "")  # No default password
    DB_HOST = os.environ.get("DB_HOST", "localhost")
    DB_PORT = os.environ.get("DB_PORT", "5432")
    
    @classmethod
    def get_connection_string(cls):
        """Return a database connection string"""
        return f"postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"

# API configuration
class APIConfig:
    DEBUG = os.environ.get("DEBUG", "False").lower() == "true"
    HOST = os.environ.get("HOST", "0.0.0.0")
    PORT = int(os.environ.get("PORT", "5000"))
    NYC_API_URL = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"
    NYC_API_APP_TOKEN = os.environ.get("NYC_API_APP_TOKEN", "")
    API_REQUEST_LIMIT = int(os.environ.get("API_REQUEST_LIMIT", "50000"))
