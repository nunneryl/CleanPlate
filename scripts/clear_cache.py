# clear_cache.py - v2 with Automated Path Fix

import logging
import sys
import os

# --- FIX for Python Import Path ---
# This block ensures that Python looks for modules in the same directory
# as this script. This resolves the "Could not import" error when running
# the script directly inside the Railway container.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(SCRIPT_DIR)
# --- End of Fix ---


# Local application imports
# This import will now succeed because we've told Python where to look.
try:
    from db_manager import get_redis_client
    logging.info("Successfully imported Redis client from db_manager.")
except ImportError:
    logging.critical("FATAL: Could not import get_redis_client from db_manager.py.")
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
    Connects to Redis and flushes the entire database, clearing all cached results.
    """
    logger.info("Attempting to connect to Redis to clear the cache...")
    redis_conn = get_redis_client()
    
    if redis_conn:
        try:
            redis_conn.flushdb()
            logger.info("SUCCESS: Redis cache has been cleared.")
            logger.info("Your app will now fetch fresh results from the database on the next search.")
        except Exception as e:
            logger.error(f"ERROR: An exception occurred while trying to clear the Redis cache: {e}", exc_info=True)
    else:
        logger.error("ERROR: Could not get a Redis client. Please check your Redis configuration and environment variables.")

if __name__ == "__main__":
    clear_redis_cache()
