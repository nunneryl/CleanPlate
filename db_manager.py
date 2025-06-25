import logging
import psycopg
from psycopg_pool import ConnectionPool
import threading

from config import DatabaseConfig, RedisConfig, load_dotenv

logger = logging.getLogger(__name__)

class DatabaseManager:
    _connection_pool = None

    @classmethod
    def initialize_pool(cls, min_connections=1, max_connections=10):
        """Initialize the database connection pool"""
        if cls._connection_pool is not None:
            logger.info("Database connection pool already initialized.")
            return

        try:
            # Get the connection string directly and use it
            conn_str = DatabaseConfig.get_connection_string()
            logger.info(f"Initializing database connection pool for {DatabaseConfig.DB_NAME} on {DatabaseConfig.DB_HOST}:{DatabaseConfig.DB_PORT}")
            cls._connection_pool = ConnectionPool(conninfo=conn_str, min_size=min_connections, max_size=max_connections)
            logger.info("Database connection pool initialized successfully.")
        except psycopg.OperationalError as e:
            logger.critical(f"Database connection failed: Check credentials/host/port/db name. Error: {e}", exc_info=True)
            raise

    @classmethod
    def get_connection(cls):
        if cls._connection_pool is None:
            logger.warning("Connection pool not initialized. Attempting to initialize.")
            cls.initialize_pool()
        if cls._connection_pool is None:
             raise ConnectionError("Database connection pool is not available.")
        try:
            return cls._connection_pool.getconn()
        except Exception as e:
            logger.error(f"Failed to get connection from pool: {e}", exc_info=True)
            raise ConnectionError(f"Failed to get connection from pool: {e}")

    @classmethod
    def return_connection(cls, connection):
        if cls._connection_pool is not None and connection is not None:
             try: cls._connection_pool.putconn(connection)
             except Exception as e: logger.error(f"Failed to return connection to pool: {e}", exc_info=True)
        elif connection is None: logger.warning("Attempted to return a None connection to the pool.")
        else: logger.warning("Attempted to return connection, but pool is not initialized.")

    @classmethod
    def close_all_connections(cls):
        if cls._connection_pool is not None:
            try:
                cls._connection_pool.close() # Use .close() for the new library
                logger.info("All database connections closed.")
                cls._connection_pool = None
            except Exception as e:
                 logger.error(f"Error closing database connection pool: {e}", exc_info=True)
        else: logger.info("Attempted to close connections, but pool was not initialized.")

class DatabaseConnection:
    def __init__(self):
        self.conn = None
    def __enter__(self):
        try:
            self.conn = DatabaseManager.get_connection()
            logger.debug("Database connection acquired from pool and set to autocommit.")
            return self.conn
        except Exception as e:
            logger.error(f"Failed to acquire database connection: {e}", exc_info=True)
            raise
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn is not None:
            try:
                # With autocommit=True, we no longer need explicit rollback logic here.
                # The connection is simply returned to the pool.
                pass
            except Exception as db_e:
                 logger.error(f"Error during DB exit: {db_e}", exc_info=True)
            finally:
                 DatabaseManager.return_connection(self.conn)
                 logger.debug("Database connection returned to pool.")
        else: logger.error("DB Connection context exit called but self.conn is None.")
