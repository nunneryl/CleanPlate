# gunicorn_config.py
import logging

# Explicitly configure Gunicorn's logging
errorlog = '-'
accesslog = '-'
loglevel = 'info'

# --- post_fork hook ---
# Make sure there's only ONE definition line below
def post_fork(server, worker):
    # Use Gunicorn's logger for messages related to the hook itself
    server.log.info(f"Worker {worker.pid}: post_fork hook executing.") # Simplified log message

    # --- TEMPORARILY COMMENTED OUT DB INIT FOR TESTING ---
    # server.log.info(f"Worker {worker.pid}: Initializing database pool via post_fork hook...")
    # try:
    #     from db_manager import DatabaseManager
    #     DatabaseManager.initialize_pool()
    #     server.log.info(f"Worker {worker.pid}: Database pool initialization attempt complete.")
    # except Exception as e:
    #     server.log.critical(f"Worker {worker.pid}: CRITICAL: Failed to initialize database pool in post_fork: {e}")
    # --- END TEMPORARILY COMMENTED OUT SECTION ---

    server.log.info(f"Worker {worker.pid}: post_fork hook finished (DB init skipped).")
