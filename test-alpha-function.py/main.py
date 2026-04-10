import logging
from db_connection import create_database_if_not_exists
from verify_integrity import run_health_check
from create_tables import init_db
from load_data import load_csv_to_db

# ─────────────────────────────────────────────
# BUG FIXED #5: Only ONE logging.basicConfig here.
# The original had it duplicated (once before imports, once after).
# Python only respects the FIRST call — the second was silently ignored.
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("automation.log"),  # Saves to file
        logging.StreamHandler()                 # Also prints to terminal
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
        logger.info("[Step 4/4] Loading CSV data into database...")
        load_csv_to_db('/Users/mac/Desktop/stockage des donnees bancaires/data/financecore_clean.csv')

        logger.info("=" * 50)
        logger.info("  PIPELINE FINISHED SUCCESSFULLY ✅")
        logger.info("=" * 50)

    except FileNotFoundError as e:
        logger.critical(f"Pipeline aborted — CSV file missing: {e}")

    except Exception as e:
        logger.critical(f"Pipeline crashed unexpectedly: {e}", exc_info=True)


if __name__ == "__main__":
    run_pipeline()
