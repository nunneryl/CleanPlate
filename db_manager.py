import logging
import psycopg2
from psycopg2 import pool
from config import DatabaseConfig

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
        return cls._connection_pool.getconn()
    
    @classmethod
    def return_connection(cls, connection):
        """Return a connection to the pool"""
        cls._connection_pool.putconn(connection)
    
    @classmethod
    def close_all_connections(cls):
        """Close all connections in the pool"""
        if cls._connection_pool is not None:
            cls._connection_pool.closeall()
            logger.info("All database connections closed")


# Context manager for database connections
class DatabaseConnection:
    def __init__(self):
        self.conn = None
        
    def __enter__(self):
        self.conn = DatabaseManager.get_connection()
        return self.conn
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            # An exception occurred, rollback
            self.conn.rollback()
        DatabaseManager.return_connection(self.conn)
