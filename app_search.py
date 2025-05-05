# app_search.py (Minimal Test Version)
import logging
import os
from flask import Flask, jsonify

# Configure basic logging to stdout/stderr
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logger.info("--- Minimal Flask App: Top level starting ---")

app = Flask(__name__)

@app.route('/')
def hello():
    logger.info("--- Minimal Flask App: Received request for / ---")
    return jsonify({"message": "Minimal Flask app is running!"})

# Note: No imports from config, db_manager, update_database
# Note: No Sentry init, no CORS for simplicity

logger.info("--- Minimal Flask App: Flask app object created ---")

# This block is only for running locally (python app_search.py)
# Gunicorn runs the 'app' object directly via Procfile
if __name__ == "__main__":
    logger.info("--- Minimal Flask App: Running locally via __main__ ---")
    # Use a default port if RAILWAY_PORT isn't set locally
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=False)

logger.info("--- Minimal Flask App: Script loaded completely ---")
