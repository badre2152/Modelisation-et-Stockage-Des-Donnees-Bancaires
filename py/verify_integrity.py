import logging
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from db_connection import get_engine

logger = logging.getLogger(__name__)

def run_health_check():
    logger.info("Starting system integrity check...")
    engine = get_engine()
    try:
        with engine.connect() as conn:
            res = conn.execute(text("SELECT current_user, current_database();"))
            user, db = res.fetchone()
            logger.info(f"DB Connection Stable. User: {user}, DB: {db}")
            
            # Test Write/Delete permission
            conn.execute(text("CREATE TEMP TABLE test_log (id int); DROP TABLE test_log;"))
            logger.info("Permissions check passed.")
            return True
    except SQLAlchemyError as e:
        logger.error(f"System integrity check failed: {e}")
        return False