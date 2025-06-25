# config.py - Use Railway standard variable names
import os
from dotenv import load_dotenv

load_dotenv() # For local .env file

class SentryConfig:
    SENTRY_DSN = os.environ.get("SENTRY_DSN", None)
    
# Database configuration - Use Railway's default PG* names
class DatabaseConfig:
    # Read Railway's standard Postgres variables
    DB_NAME = os.environ.get("PGDATABASE", "nyc_restaurant_db") # Use PGDATABASE
    DB_USER = os.environ.get("PGUSER", "postgres")             # Use PGUSER
    DB_PASSWORD = os.environ.get("PGPASSWORD", None)           # Use PGPASSWORD
    DB_HOST = os.environ.get("PGHOST", "localhost")            # Use PGHOST
    DB_PORT = os.environ.get("PGPORT", "5432")                 # Use PGPORT
    
    @classmethod
    def get_connection_string(cls):
        """Return a database connection string using PG* vars"""
        password_part = f":{cls.DB_PASSWORD}" if cls.DB_PASSWORD else ""
        return f"postgresql://{cls.DB_USER}{password_part}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"

# API and App configuration
class APIConfig:
    DEBUG = os.environ.get("DEBUG", "False").lower() == "true"
    HOST = os.environ.get("HOST", "0.0.0.0") # Usually not needed, Gunicorn binds
    PORT = int(os.environ.get("PORT", "5000")) # Railway provides PORT

    NYC_API_URL = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"
    NYC_API_APP_TOKEN = os.environ.get("NYC_API_APP_TOKEN", None)
    API_REQUEST_LIMIT = int(os.environ.get("API_REQUEST_LIMIT", "50000"))
    UPDATE_SECRET_KEY = os.environ.get("UPDATE_SECRET_KEY", None)

# Redis configuration - Use Railway's default REDIS* names
class RedisConfig:
    HOST = os.environ.get("REDISHOST", "localhost")
    PORT = int(os.environ.get("REDISPORT", 6379))
    PASSWORD = os.environ.get("REDISPASSWORD", None)
    USER = os.environ.get("REDISUSER", "default") # Often 'default'
