# clear_cache.py
# A simple utility to clear the Redis cache for the application.

import logging

# Local application imports
# This uses your app's existing functions to connect to Redis.
try:
    from db_manager import get_redis_client
    logging.info("Successfully imported Redis client from db_manager.")
except ImportError:
    logging.critical("FATAL: Could not import get_redis_client from db_manager.py.")
    logging.critical("Please ensure this script is in the same directory as your other backend files.")
    exit(1)

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True
)
logger = logging.getLogger(__name__)

def clear_redis_cache():
    """
    Connects to Redis and flushes the entire database (db 0),
    clearing all cached search results.
    """
    logger.info("Attempting to connect to Redis to clear the cache...")
    redis_conn = get_redis_client()
    
    if redis_conn:
        try:
            # The flushdb() command deletes all keys in the current database.
            redis_conn.flushdb()
            logger.info("SUCCESS: Redis cache has been cleared.")
            logger.info("Your app will now fetch fresh results from the database on the next search.")
        except Exception as e:
            logger.error(f"ERROR: An exception occurred while trying to clear the Redis cache: {e}", exc_info=True)
    else:
        logger.error("ERROR: Could not get a Redis client. Please check your Redis configuration and environment variables.")

if __name__ == "__main__":
    clear_redis_cache()
