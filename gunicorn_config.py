# gunicorn_config.py - Reverted to include DB init
import logging
import os # Import os if needed for other settings later

# Explicitly configure Gunicorn's logging to ensure output
# Send error logs to stderr (standard error stream)
errorlog = '-'
# Send access logs to stdout (standard output stream)
accesslog = '-'
# Set the log level (info shows startup messages, requests, etc.)
loglevel = 'info'

# Optional: Bind to the port specified by Railway
# Gunicorn often picks this up automatically, but being explicit can help
# bind = f"0.0.0.0:{os.environ.get('PORT', '8080')}"

# Optional: Set number of workers (adjust based on Railway plan resources)
# workers = int(os.environ.get('WEB_CONCURRENCY', 2)) # Example: Use WEB_CONCURRENCY or default to 2

# --- post_fork hook ---
# This hook runs in each worker process *after* it's created.
# Make sure there's only ONE definition line below
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
        server.log.critical(f"Worker {worker.pid}: CRITICAL: Failed to initialize database pool in post_fork: {e}", exc_info=True)
        # Depending on your needs, you might want the worker to exit if DB is essential
        # import sys
        # sys.exit(1) # Exit the worker if pool init fails

# Optional: Add other hooks if needed for debugging later
# def on_starting(server):
#     server.log.info("Gunicorn master process starting...")
#
# def worker_exit(server, worker):
#     server.log.info(f"Worker {worker.pid} exiting...")
#     # Add cleanup code here if necessary
#     # Example: Closing DB pool connections
#     # try:
#     #     from db_manager import DatabaseManager
#     #     DatabaseManager.close_all_connections() # Assuming such a method exists
#     # except Exception as e:
#     #     server.log.error(f"Worker {worker.pid}: Error closing DB connections on exit: {e}")
