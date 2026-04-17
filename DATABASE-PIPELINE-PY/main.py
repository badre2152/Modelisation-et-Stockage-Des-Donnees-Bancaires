import os
import sys
import logging
from db_connection import create_database_if_not_exists
from verify_integrity import run_health_check
from create_tables import init_db
from load_data import load_csv_to_db

# Add ANALYTICS-SQL to path so kpi_queries is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ANALYTICS-SQL'))
from kpi_queries import create_dashboard_views, run_all_analytics, print_analytics

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
    logger.info("=" * 55)
    logger.info("  PIPELINE START")
    logger.info("=" * 55)

    try:
        # ── Step 1: Create database ───────────────────────────────────
        logger.info("[Step 1/5] Checking / Creating database...")
        create_database_if_not_exists()

        # ── Step 2: Health check ──────────────────────────────────────
        logger.info("[Step 2/5] Running health check...")
        if not run_health_check():
            logger.warning("Pipeline aborted: health check failed.")
            return

        # ── Step 3: Tables + operational views ───────────────────────
        logger.info("[Step 3/5] Initializing tables and operational views...")
        init_db()

        # ── Step 4: Load CSV data ─────────────────────────────────────
        csv_path = os.getenv("CSV_PATH", "data/financecore_clean.csv")
        logger.info(f"[Step 4/5] Loading CSV data from '{csv_path}'...")
        load_csv_to_db(csv_path)

        # ── Step 5: Dashboard views + analytics ───────────────────────
        # Creates all 7 views used by the Streamlit dashboard:
        #   vue_kpi_global, vue_kpi_agence, vue_kpi_segment,
        #   vue_kpi_produit, vue_kpi_mensuel,
        #   vue_clients_risque, vue_taux_defaut
        logger.info("[Step 5/5] Creating dashboard views + running analytics...")
        create_dashboard_views()
        results = run_all_analytics()
        print_analytics(results)

        logger.info("=" * 55)
        logger.info("  PIPELINE FINISHED SUCCESSFULLY ✅")
        logger.info("=" * 55)

    except FileNotFoundError as e:
        logger.critical(f"Pipeline aborted — file missing: {e}")

    except Exception as e:
        logger.critical(f"Pipeline crashed: {e}", exc_info=True)


if __name__ == "__main__":
    run_pipeline()