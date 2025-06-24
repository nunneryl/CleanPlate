# db_manager.py

import logging
import psycopg
from psycopg_pool import ConnectionPool
import redis
import threading

# Import config classes needed
from config import DatabaseConfig, RedisConfig, load_dotenv # Ensure load_dotenv is imported if used

logger = logging.getLogger(__name__)

# --- DatabaseManager Class (No changes needed) ---
class DatabaseManager:
    _connection_pool = None

    @classmethod
    def initialize_pool(cls, min_connections=1, max_connections=10):
        """Initialize the database connection pool"""
        if cls._connection_pool is not None:
            logger.info("Database connection pool already initialized.")
            return
        try:
            # Use Railway standard PG* vars read by DatabaseConfig
            logger.info(f"Initializing database connection pool for {DatabaseConfig.DB_NAME} on {DatabaseConfig.DB_HOST}:{DatabaseConfig.DB_PORT}")
            cls._connection_pool = ConnectionPool(conninfo=DatabaseConfig.get_connection_string(), min_size=min_connections, max_size=max_connections)
            logger.info("Database connection pool initialized successfully.")
        except psycopg2.OperationalError as e:
             logger.critical(f"Database connection failed: Check credentials/host/port/db name. Error: {e}", exc_info=True)
             raise
        except Exception as e:
            logger.critical(f"Failed to initialize database connection pool: {e}", exc_info=True)
            raise

    @classmethod
    def get_connection(cls):
        """Get a connection from the pool"""
        if cls._connection_pool is None:
            logger.warning("Connection pool not initialized. Attempting to initialize.")
            cls.initialize_pool() # Attempt initialization
        if cls._connection_pool is None: # Check again after attempt
             raise ConnectionError("Database connection pool is not available.")
        try:
            return cls._connection_pool.getconn()
        except Exception as e:
            logger.error(f"Failed to get connection from pool: {e}", exc_info=True)
            raise ConnectionError(f"Failed to get connection from pool: {e}")

    @classmethod
    def return_connection(cls, connection):
        """Return a connection to the pool"""
        if cls._connection_pool is not None and connection is not None:
             try: cls._connection_pool.putconn(connection)
             except Exception as e: logger.error(f"Failed to return connection to pool: {e}", exc_info=True)
        elif connection is None: logger.warning("Attempted to return a None connection to the pool.")
        else: logger.warning("Attempted to return connection, but pool is not initialized.")

    @classmethod
    def close_all_connections(cls):
        """Close all connections in the pool"""
        if cls._connection_pool is not None:
            try:
                cls._connection_pool.closeall()
                logger.info("All database connections closed.")
                cls._connection_pool = None
            except Exception as e:
                 logger.error(f"Error closing database connection pool: {e}", exc_info=True)
        else: logger.info("Attempted to close connections, but pool was not initialized.")

# --- DatabaseConnection Context Manager (No changes needed) ---
class DatabaseConnection:
    """Context manager for handling database connections from the pool."""
    def __init__(self):
        self.conn = None
    def __enter__(self):
        try:
            self.conn = DatabaseManager.get_connection()
            logger.debug("Database connection acquired from pool.")
            return self.conn
        except Exception as e:
            logger.error(f"Failed to acquire database connection: {e}", exc_info=True)
            raise
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn is not None:
            try:
                if exc_type is not None:
                    logger.warning(f"Exception in DB block, rolling back: {exc_val}", exc_info=exc_tb)
                    self.conn.rollback()
                # else: # No explicit commit here, assume handled in 'with' block
                #     pass
            except Exception as db_e:
                 logger.error(f"Error during DB rollback/commit on exit: {db_e}", exc_info=True)
            finally:
                 DatabaseManager.return_connection(self.conn)
                 logger.debug("Database connection returned to pool.")
        else: logger.error("DB Connection context exit called but self.conn is None.")


# --- Redis Client Initialization (Lazy Singleton Pattern) ---

# Global variable to hold the client instance once created
_redis_client_instance = None
# Lock to prevent race conditions during initialization in multi-threaded env
_redis_init_lock = threading.Lock()

def get_redis_client():
    """
    Gets the Redis client instance, initializing it on the first call.
    Uses a lock to ensure thread-safe initialization.
    Returns None if connection fails.
    """
    global _redis_client_instance
    global _redis_init_lock

    # Fast path: If already initialized, return it
    if _redis_client_instance:
        return _redis_client_instance

    # Acquire lock before checking/initializing
    with _redis_init_lock:
        # Double-check if another thread initialized it while waiting for the lock
        if _redis_client_instance:
            return _redis_client_instance

        # --- Initialization Logic ---
        try:
            # Explicitly ensure config is loaded (might be redundant if loaded early, but safe)
            # load_dotenv() # Uncomment if you suspect .env isn't loaded early enough

            # Read config values INSIDE the lock and function call
            r_host = RedisConfig.HOST
            r_port = RedisConfig.PORT
            r_password = RedisConfig.PASSWORD
            r_user = RedisConfig.USER

            logger.info(f"Initializing Redis client for the first time. Connecting to {r_host}:{r_port}")

            temp_client = redis.Redis(
                host=r_host,
                port=r_port,
                password=r_password,
                username=r_user,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
                health_check_interval=30
            )
            temp_client.ping() # Test connection
            logger.info(f"Redis client initialized and connection successful to {r_host}:{r_port}")
            _redis_client_instance = temp_client # Assign to global instance *after* success
            return _redis_client_instance

        except redis.exceptions.AuthenticationError:
            logger.error("Redis authentication failed. Check REDISPASSWORD/USER. Caching disabled.")
        except redis.exceptions.TimeoutError:
             logger.error(f"Redis connection to {r_host}:{r_port} timed out. Caching disabled.")
        except redis.exceptions.ConnectionError as e:
            logger.error(f"Failed to connect to Redis at {r_host}:{r_port}: {e}. Caching disabled.")
        except Exception as e:
            logger.error(f"An unexpected error occurred during Redis initialization: {e}", exc_info=True)

        # If any exception occurred, ensure we return None
        return None

# --- Make redis_client available via the function call ---
# Code elsewhere should now call get_redis_client() instead of using redis_client directly
# Example:
# client = get_redis_client()
# if client:
#    client.get(...)

# --- For backwards compatibility (optional, but might avoid changing app_search.py) ---
# You could try assigning the result of the first call here, but it's less robust
# than calling get_redis_client() everywhere.
# redis_client = get_redis_client()
# logger.info(f"Global redis_client initialized via get_redis_client(): {redis_client is not None}")

# --- Recommended: Modify app_search.py to use get_redis_client() ---
# Find where `redis_client` is used in app_search.py (e.g., in the /search route)
# and replace `if redis_client:` with `client = get_redis_client(); if client:`
# and use `client.` instead of `redis_client.`
# Example change in app_search.py /search route:
#   client = get_redis_client() # Get client instance
#   if client: # Check if connection succeeded
#       try:
#           cached_result_str = client.get(cache_key) # Use 'client'
#           # ... rest of cache logic using 'client' ...
#       except ...:
#           # ...
#   else:
#       logger.warning("Redis client unavailable, skipping cache check.")

