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
    UPDATE_SECRET_KEY = os.environ.get("UPDATE_SECRET_KEY", None)
    API_REQUEST_LIMIT = int(os.environ.get("API_REQUEST_LIMIT", "50000"))
    
class RedisConfig:
    # Load Redis connection details from environment variables provided by Railway
    # Use the exact names shown in your Railway 'Variables' tab for the Redis service
    HOST = os.environ.get("REDISHOST", "localhost")
    PORT = int(os.environ.get("REDISPORT", 6379)) # Convert port to integer
    PASSWORD = os.environ.get("REDISPASSWORD", None)
    USER = os.environ.get("REDISUSER", "default") # Use 'default' if REDISUSER isn't set or needed

    # Construct a basic connection URL (optional, depends on library usage)
    # Note: Ensure password handling is secure if constructing URLs
    # URL = os.environ.get("REDIS_URL", None) # Alternatively, use the full URL if preferred by library
