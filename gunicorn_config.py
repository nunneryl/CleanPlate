# gunicorn_config.py
import logging
import os

# Explicitly configure Gunicorn's logging to ensure output
errorlog = '-'  # Send error logs to stderr
accesslog = '-' # Send access logs to stdout
loglevel = 'info' # Set the log level

# --- INCREASED WORKER TIMEOUT FOR CATCH-UP ---
# Set a longer timeout (e.g., 300 seconds = 5 minutes) for the catch-up.
# Gunicorn's default is 30 seconds. Remember to change back to 180 after.
timeout = 300

# Optional: Bind to the port specified by Railway
# bind = f"0.0.0.0:{os.environ.get('PORT', '8080')}"

# Optional: Set number of workers
# workers = int(os.environ.get('WEB_CONCURRENCY', 2))

# --- post_fork hook ---
def post_fork(server, worker):
    server.log.info(f"Worker {worker.pid}: Initializing database pool via post_fork hook...")
    try:
        from db_manager import DatabaseManager # Assuming your DB manager is named this
        DatabaseManager.initialize_pool()
        server.log.info(f"Worker {worker.pid}: Database pool initialization attempt complete.")
    except Exception as e:
        server.log.critical(f"Worker {worker.pid}: CRITICAL: Failed to initialize database pool in post_fork: {e}", exc_info=True)
        # import sys
        # sys.exit(1) # Exit the worker if pool init fails
