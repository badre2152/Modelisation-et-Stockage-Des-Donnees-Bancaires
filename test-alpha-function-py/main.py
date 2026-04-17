import os
import logging
from db_connection import create_database_if_not_exists
from verify_integrity import run_health_check
from create_tables import init_db
from load_data import load_csv_to_db
 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("automation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
 
 
def run_pipeline():
    logger.info("=" * 50)
    logger.info("  PIPELINE START")
    logger.info("=" * 50)
 
    try:
        # Step 1: Create the database if it doesn't exist
        logger.info("[Step 1/4] Checking / Creating database...")
        create_database_if_not_exists()
 
        # Step 2: Health check — abort if DB is not ready
        logger.info("[Step 2/4] Running health check...")
        if not run_health_check():
            logger.warning("Pipeline aborted: health check failed. Fix the issues above and retry.")
            return
 
        # Step 3: Create tables and views
        logger.info("[Step 3/4] Initializing tables and views...")
        init_db()
 
        # Step 4: Load CSV data
        # FIX #11: Path now comes from CSV_PATH env var (with a sensible fallback).
        # Set CSV_PATH in your .env file or export it before running.
        csv_path = os.getenv("CSV_PATH", "data/financecore_clean.csv")
        logger.info(f"[Step 4/4] Loading CSV data from '{csv_path}'...")
        load_csv_to_db(csv_path)
 
        logger.info("=" * 50)
        logger.info("  PIPELINE FINISHED SUCCESSFULLY ✅")
        logger.info("=" * 50)
 
    except FileNotFoundError as e:
        logger.critical(f"Pipeline aborted — CSV file missing: {e}")
 
    except Exception as e:
        logger.critical(f"Pipeline crashed unexpectedly: {e}", exc_info=True)
 
 
if __name__ == "__main__":
    run_pipeline()