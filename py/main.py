import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"), # Saves logs to a file
        logging.StreamHandler()             # Also prints to terminal
    ]
)
logger = logging.getLogger(__name__)
import logging
from db_connection import create_database_if_not_exists
from verify_integrity import run_health_check
from create_tables import init_db
from load_data import load_csv_to_db

# Logging Config
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("automation.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def run_pipeline():
    logger.info("--- PIPELINE START ---")
    
    try:
        create_database_if_not_exists()
        
        if not run_health_check():
            logger.warning("Pipeline aborted due to failed health check.")
            return

        init_db()
        load_csv_to_db('financecore_clean.csv')
        
        logger.info("--- PIPELINE FINISHED SUCCESSFULLY ---")
    except Exception as e:
        logger.critical(f"Pipeline crashed: {e}")

if __name__ == "__main__":
    run_pipeline()