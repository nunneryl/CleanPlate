# In db_manager.py

import logging
import psycopg2
from psycopg2 import pool
import redis # <-- Import redis library
from config import DatabaseConfig, RedisConfig # <-- Import RedisConfig

logger = logging.getLogger(__name__)

class DatabaseManager:
    _connection_pool = None

    @classmethod
    def initialize_pool(cls, min_connections=1, max_connections=10):
        """Initialize the database connection pool"""
        try:
            cls._connection_pool = pool.ThreadedConnectionPool(
                min_connections,
                max_connections,
                user=DatabaseConfig.DB_USER,
                password=DatabaseConfig.DB_PASSWORD,
                host=DatabaseConfig.DB_HOST,
                port=DatabaseConfig.DB_PORT,
                dbname=DatabaseConfig.DB_NAME
            )
            logger.info("Database connection pool initialized successfully")
        except Exception as e:
            logger.critical(f"Failed to initialize database connection pool: {e}")
            raise

    @classmethod
    def get_connection(cls):
        """Get a connection from the pool"""
        if cls._connection_pool is None:
            cls.initialize_pool()
        # Add a try-except block for robustness when getting connection
        try:
            return cls._connection_pool.getconn()
        except Exception as e:
            logger.error(f"Failed to get connection from pool: {e}")
            # Re-initialize pool if connection fails, might help transient issues
            logger.info("Attempting to re-initialize database pool.")
            cls.initialize_pool()
            # Try getting connection again
            try:
                 return cls._connection_pool.getconn()
            except Exception as final_e:
                 logger.critical(f"Failed to get connection after re-initializing pool: {final_e}")
                 raise # Raise the final error if still failing

    @classmethod
    def return_connection(cls, connection):
        """Return a connection to the pool"""
        if cls._connection_pool is not None:
             try:
                 cls._connection_pool.putconn(connection)
             except Exception as e:
                 logger.error(f"Failed to return connection to pool: {e}")
        else:
             logger.warning("Attempted to return connection, but pool is not initialized.")


    @classmethod
    def close_all_connections(cls):
        """Close all connections in the pool"""
        if cls._connection_pool is not None:
            cls._connection_pool.closeall()
            logger.info("All database connections closed")

class DatabaseConnection:
    def __init__(self):
        self.conn = None

    def __enter__(self):
        self.conn = DatabaseManager.get_connection()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn: # Ensure conn exists before trying to rollback/return
            if exc_type is not None:
                # An exception occurred, rollback
                try:
                    self.conn.rollback()
                    logger.warning(f"Database transaction rolled back due to exception: {exc_val}")
                except Exception as rb_e:
                     logger.error(f"Error during database rollback: {rb_e}")

            DatabaseManager.return_connection(self.conn)
        else:
             logger.error("DatabaseConnection context manager exit called but self.conn is None.")

redis_client = None
try:
    # Create a Redis client instance using connection details from config
    # decode_responses=True automatically decodes responses from bytes to strings
    redis_client = redis.Redis(
        host=RedisConfig.HOST,
        port=RedisConfig.PORT,
        password=RedisConfig.PASSWORD,
        username=RedisConfig.USER, # Pass username if provided by Railway/config
        decode_responses=True, # Decode responses to strings automatically
        socket_timeout=5, # Add a timeout
        socket_connect_timeout=5
    )
    # Test the connection
    redis_client.ping()
    logger.info(f"Redis client initialized and connected successfully to {RedisConfig.HOST}:{RedisConfig.PORT}")
except redis.exceptions.ConnectionError as e:
    logger.error(f"Failed to connect to Redis: {e}. Caching will be disabled.")
    redis_client = None # Ensure client is None if connection fails
except Exception as e:
    logger.error(f"An unexpected error occurred during Redis initialization: {e}")
    redis_client = None # Ensure client is None if connection fails

