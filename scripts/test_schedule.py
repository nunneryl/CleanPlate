# test_schedule.py
import os
import sys
import logging
import time

# --- Basic Logging Setup ---
# Configure logging to output to standard output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout # Explicitly direct to stdout
)
logger = logging.getLogger("test_schedule")
# --- End Logging Setup ---

# --- Dependency Import & Test Section ---
# Initialize flags
db_import_success = False
redis_import_success = False
db_connection_class = None
redis_client_instance = None
psycopg2_module = None
redis_module = None

# Try importing database components
try:
    # Adjust the import path based on your project structure
    # If DatabaseConnection is in db_manager.py:
    from db_manager import DatabaseConnection
    import psycopg2
    db_connection_class = DatabaseConnection
    psycopg2_module = psycopg2
    db_import_success = True
    logger.info("Successfully imported DatabaseConnection and psycopg2.")
except ImportError as e:
    logger.error(f"Failed to import DB components: {e}. DB test will be skipped.")
except Exception as e:
    logger.error(f"Unexpected error during DB imports: {e}. DB test will be skipped.")

# Try importing Redis components
try:
    # Adjust the import path based on your project structure
    # If redis_client is initialized in db_manager.py:
    from db_manager import redis_client
    import redis
    redis_client_instance = redis_client
    redis_module = redis
    redis_import_success = True
    logger.info("Successfully imported redis_client and redis.")
except ImportError as e:
    logger.error(f"Failed to import Redis components: {e}. Redis test will be skipped.")
except Exception as e:
    logger.error(f"Unexpected error during Redis imports: {e}. Redis test will be skipped.")
# --- End Dependency Import & Test Section ---


# --- Test Functions ---
def run_db_test():
    if not db_import_success or not db_connection_class or not psycopg2_module:
        logger.warning("Skipping DB test due to failed imports.")
        return None # Indicate skipped

    logger.info("Attempting database connection test...")
    try:
        # Use the imported DatabaseConnection class
        with db_connection_class() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1;")
                result = cursor.fetchone()
                if result and result[0] == 1:
                    logger.info("SUCCESS: Database connection and query successful.")
                    return True
                else:
                    logger.error(f"FAILURE: Database query returned unexpected result: {result}")
                    return False
    except psycopg2_module.Error as db_err:
        logger.error(f"FAILURE: Database error during connection/query: {db_err}")
        return False
    except Exception as e:
        logger.error(f"FAILURE: Unexpected error during database test: {e}", exc_info=True)
        return False

def run_redis_test():
    if not redis_import_success or not redis_client_instance or not redis_module:
        logger.warning("Skipping Redis test due to failed imports or unavailable client.")
        return None # Indicate skipped

    logger.info("Attempting Redis connection test...")
    try:
        # Use the imported redis_client instance
        test_key = f"scheduler_test_{int(time.time())}"
        response = redis_client_instance.set(test_key, "success", ex=60) # Set with 60s expiry
        if not response:
             logger.error(f"FAILURE: Redis SET command did not return success for key {test_key}.")
             return False

        retrieved_value = redis_client_instance.get(test_key)
        # Decode if necessary (assuming default bytes response)
        if isinstance(retrieved_value, bytes):
             retrieved_value = retrieved_value.decode('utf-8')

        if retrieved_value == "success":
            logger.info(f"SUCCESS: Redis SET and GET successful for key {test_key}.")
            # Clean up the test key
            try:
                redis_client_instance.delete(test_key)
            except Exception as del_e:
                logger.warning(f"Warning: Failed to delete Redis test key {test_key}: {del_e}")
            return True
        else:
            logger.error(f"FAILURE: Redis GET returned unexpected value: {retrieved_value}")
            return False
    except redis_module.exceptions.RedisError as redis_err:
        logger.error(f"FAILURE: Redis error during connection/query: {redis_err}")
        return False
    except Exception as e:
        logger.error(f"FAILURE: Unexpected error during Redis test: {e}", exc_info=True)
        return False
# --- End Test Functions ---


# --- Main Execution ---
if __name__ == "__main__":
    logger.info("--- test_schedule.py: Script starting ---")
    logger.info(f"Running with Python executable: {sys.executable}")
    logger.info(f"Current working directory: {os.getcwd()}")
    logger.info(f"Python path: {sys.path}")

    # Perform tests
    db_result = run_db_test()
    redis_result = run_redis_test()

    logger.info(f"Test Results Summary - DB Connection: {db_result}, Redis Connection: {redis_result}")

    # Add a small delay to ensure logs might capture if it hangs right at the end
    logger.info("Sleeping for 2 seconds before exiting...")
    time.sleep(2)

    logger.info("--- test_schedule.py: Script finished ---")
    # Explicitly flush standard output and error streams
    sys.stdout.flush()
    sys.stderr.flush()
# --- End Main Execution ---
