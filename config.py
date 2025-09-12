# config.py - Use Railway standard variable names
import os
from dotenv import load_dotenv

load_dotenv() # For local .env file

class SentryConfig:
    SENTRY_DSN = os.environ.get("SENTRY_DSN", None)
    
# Database configuration
class DatabaseConfig:
    # These are kept for fallback compatibility (e.g., local .env file)
    DB_NAME = os.environ.get("PGDATABASE", "nyc_restaurant_db")
    DB_USER = os.environ.get("PGUSER", "postgres")
    DB_PASSWORD = os.environ.get("PGPASSWORD", None)
    DB_HOST = os.environ.get("PGHOST", "localhost")
    DB_PORT = os.environ.get("PGPORT", "5432")
    
    @classmethod
    def get_connection_string(cls):
        """
        Return a database connection string.
        Prioritizes Railway's all-in-one DATABASE_URL for robustness,
        then falls back to individual PG* variables for local/legacy setups.
        """
        # Railway provides a single DATABASE_URL, which is the preferred way to connect.
        database_url = os.environ.get("DATABASE_URL")
        if database_url:
            # The psycopg library expects the scheme 'postgresql://'. Railway's variable
            # might use 'postgres://', so we replace it to ensure compatibility.
            return database_url.replace("postgres://", "postgresql://", 1)

        # Fallback for environments using individual PG* variables.
        # This keeps the existing production configuration working without changes.
        password_part = f":{cls.DB_PASSWORD}" if cls.DB_PASSWORD else ""
        return f"postgresql://{cls.DB_USER}{password_part}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"

# API and App configuration
class APIConfig:
    DEBUG = os.environ.get("DEBUG", "False").lower() == "true"
    HOST = os.environ.get("HOST", "0.0.0.0")
    PORT = int(os.environ.get("PORT", "5000"))

    NYC_API_URL = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"
    NYC_API_APP_TOKEN = os.environ.get("NYC_API_APP_TOKEN", None)
    API_REQUEST_LIMIT = int(os.environ.get("API_REQUEST_LIMIT", "50000"))
    UPDATE_SECRET_KEY = os.environ.get("UPDATE_SECRET_KEY", None)
    API_BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:5000")

# Redis configuration
class RedisConfig:
    HOST = os.environ.get("REDISHOST", "localhost")
    PORT = int(os.environ.get("REDISPORT", 6379))
    PASSWORD = os.environ.get("REDISPASSWORD", None)
    USER = os.environ.get("REDISUSER", "default")
