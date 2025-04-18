# In db_manager.py

import logging
import psycopg2
from psycopg2 import pool
import redis # <-- Import redis library
# Import all config classes needed
from config import DatabaseConfig, RedisConfig

logger = logging.getLogger(__name__) # Use __name__ for logger

# --- DatabaseManager Class (Keep As Is or with previous improvements) ---
class DatabaseManager:
    _connection_pool = None

    @classmethod
    def initialize_pool(cls, min_connections=1, max_connections=10):
        """Initialize the database connection pool"""
        # Check if pool already initialized
        if cls._connection_pool is not None:
            logger.info("Database connection pool already initialized.")
            return
        try:
            logger.info(f"Initializing database connection pool for {DatabaseConfig.DB_NAME} on {DatabaseConfig.DB_HOST}:{DatabaseConfig.DB_PORT}")
            cls._connection_pool = pool.ThreadedConnectionPool(
                min_connections,
                max_connections,
                user=DatabaseConfig.DB_USER,
                password=DatabaseConfig.DB_PASSWORD,
                host=DatabaseConfig.DB_HOST,
                port=DatabaseConfig.DB_PORT,
                dbname=DatabaseConfig.DB_NAME
            )
            logger.info("Database connection pool initialized successfully.")
        except psycopg2.OperationalError as e:
             logger.critical(f"Database connection failed: Check credentials, host, port, and database name. Error: {e}")
             # Optionally, exit or raise a custom exception if DB is critical at startup
             raise # Re-raise the exception
        except Exception as e:
            logger.critical(f"Failed to initialize database connection pool: {e}")
            raise # Re-raise the exception

    @classmethod
    def get_connection(cls):
        """Get a connection from the pool"""
        if cls._connection_pool is None:
            logger.warning("Connection pool not initialized. Attempting to initialize.")
            # Attempt to initialize if not already done
            cls.initialize_pool()
            # If initialization failed above, _connection_pool will still be None or an exception was raised

        if cls._connection_pool is None:
             # If still None after attempting init, raise an error
             raise ConnectionError("Database connection pool is not available.")

        try:
            # Get connection from the initialized pool
            return cls._connection_pool.getconn()
        except Exception as e:
            logger.error(f"Failed to get connection from pool: {e}")
            # Consider if pool needs reset or just raise error
            raise ConnectionError(f"Failed to get connection from pool: {e}")


    @classmethod
    def return_connection(cls, connection):
        """Return a connection to the pool"""
        if cls._connection_pool is not None and connection is not None:
             try:
                 cls._connection_pool.putconn(connection)
             except Exception as e:
                 logger.error(f"Failed to return connection to pool: {e}")
        elif connection is None:
             logger.warning("Attempted to return a None connection to the pool.")
        else: # pool is None
             logger.warning("Attempted to return connection, but pool is not initialized.")


    @classmethod
    def close_all_connections(cls):
        """Close all connections in the pool"""
        if cls._connection_pool is not None:
            try:
                cls._connection_pool.closeall()
                logger.info("All database connections closed.")
                cls._connection_pool = None # Reset pool state
            except Exception as e:
                 logger.error(f"Error closing database connection pool: {e}")
        else:
             logger.info("Attempted to close connections, but pool was not initialized.")

# --- DatabaseConnection Context Manager (Keep As Is or with previous improvements) ---
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
            logger.error(f"Failed to acquire database connection: {e}")
            # Propagate the error so the calling code knows connection failed
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Ensure connection is returned even if it's None (though __enter__ should raise)
        if self.conn is not None:
            try:
                if exc_type is not None:
                    # An exception occurred within the 'with' block, rollback
                    logger.warning(f"Exception occurred in DB block, rolling back transaction: {exc_val}")
                    self.conn.rollback()
                else:
                    # No exception, commit (optional, depends on usage pattern)
                    # Often commit is handled explicitly within the 'with' block
                    # self.conn.commit() # Uncomment if auto-commit is desired
                    pass
            except Exception as db_e:
                 logger.error(f"Error during DB rollback/commit on exit: {db_e}")
            finally:
                 # Always return the connection to the pool
                 DatabaseManager.return_connection(self.conn)
                 logger.debug("Database connection returned to pool.")
        else:
             # This case should ideally not happen if __enter__ raises properly
             logger.error("DatabaseConnection context manager exit called but self.conn is None.")


# --- Redis Client Initialization ---
redis_client = None # Initialize as None globally
try:
    logger.info(f"Attempting to connect to Redis at {RedisConfig.HOST}:{RedisConfig.PORT}")
    # Create a Redis client instance using connection details from config
    # decode_responses=True automatically decodes responses from bytes to strings
    # Added health check timeout to prevent indefinite hangs
    redis_client = redis.Redis(
        host=RedisConfig.HOST,
        port=RedisConfig.PORT,
        password=RedisConfig.PASSWORD,
        username=RedisConfig.USER, # Pass username if provided by Railway/config
        decode_responses=True, # Decode responses to strings automatically
        socket_timeout=5, # Timeout for individual operations
        socket_connect_timeout=5, # Timeout for establishing connection
        health_check_interval=30 # Check connection health periodically
    )
    # Test the connection with ping()
    redis_client.ping()
    logger.info(f"Redis client initialized and connection successful to {RedisConfig.HOST}:{RedisConfig.PORT}")
except redis.exceptions.AuthenticationError:
    logger.error("Redis authentication failed. Check REDISPASSWORD and REDISUSER.")
    redis_client = None # Ensure client is None if connection fails
except redis.exceptions.TimeoutError:
     logger.error("Redis connection timed out.")
     redis_client = None
except redis.exceptions.ConnectionError as e:
    logger.error(f"Failed to connect to Redis: {e}. Caching will be disabled.")
    redis_client = None # Ensure client is None if connection fails
except Exception as e:
    # Catch any other unexpected errors during initialization
    logger.error(f"An unexpected error occurred during Redis initialization: {e}")
    redis_client = None # Ensure client is None on unexpected errors
# --- End Redis Client Initialization ---
