# gunicorn_config.py
import logging

    # Optional: Configure Gunicorn's own logging if needed
    # You can customize log format, level, and output here
    # errorlog = '-' # Log errors to stderr
    # accesslog = '-' # Log access logs to stdout
    # loglevel = 'info' # Set log level (debug, info, warning, error, critical)

    # Server hook called just after a worker process has been successfully forked.
    # This is a reliable place to initialize resources needed by each worker.
def post_fork(server, worker):
        # Use Gunicorn's logger for messages related to the hook itself
        server.log.info(f"Worker {worker.pid}: Initializing database pool via post_fork hook...")
        try:
            # Import DatabaseManager *inside* the hook function
            # This avoids potential import issues in the master process
            from db_manager import DatabaseManager
            # Initialize the pool for this specific worker process
            DatabaseManager.initialize_pool()
            server.log.info(f"Worker {worker.pid}: Database pool initialization attempt complete.")
        except Exception as e:
            # Log critically if initialization fails within a worker
            server.log.critical(f"Worker {worker.pid}: CRITICAL: Failed to initialize database pool in post_fork: {e}")
            # Depending on your needs, you might want the worker to exit if DB is essential
            # import sys
            # sys.exit(1) # Exit the worker if pool init fails

    # You can add other hooks like on_starting, worker_exit if needed later
    # def on_starting(server):
    #     server.log.info("Gunicorn master process starting...")

    # def worker_exit(server, worker):
    #     server.log.info(f"Worker {worker.pid} exiting...")
    #     # Add cleanup code here if necessary (like closing DB pool connections)
    #     try:
    #         from db_manager import DatabaseManager
    #         DatabaseManager.close_all_connections()
    #     except Exception as e:
    #         server.log.error(f"Worker {worker.pid}: Error closing DB connections on exit: {e}")

    
