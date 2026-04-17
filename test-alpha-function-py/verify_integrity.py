import logging
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from db_connection import get_engine
 
logger = logging.getLogger(__name__)
 
 
def run_health_check() -> bool:
    """
    Performs a system integrity check before the pipeline runs.
    Verifies:
      1. Database connection is alive
      2. User has read/write/delete permissions
 
    Returns True if all checks pass, False otherwise.
 
    BUG FIXED #4: Split the CREATE + DROP into two separate execute() calls.
    Some DB drivers don't support multiple statements in a single execute().
    """
    logger.info("Starting system integrity check...")
    engine = get_engine()
 
    try:
        with engine.connect() as conn:
 
            # ── CHECK 1: Connection ───────────────────────────────────
            res = conn.execute(text("SELECT current_user, current_database();"))
            user, db = res.fetchone()
            logger.info(f"✅ DB Connection Stable. User: '{user}', DB: '{db}'")
 
            # ── CHECK 2: Write / Delete permissions ───────────────────
            # ✅ FIX: Two separate execute() calls instead of one with ";"
            conn.execute(text("CREATE TEMP TABLE _health_check_test (id INT)"))
            conn.execute(text("DROP TABLE _health_check_test"))
            logger.info("✅ Read/Write/Delete permissions check passed.")
 
            # ── CHECK 3: Required tables exist ───────────────────────
            required_tables = ['clients', 'produits', 'agences', 'temps', 'transactions']
            missing_tables = []
 
            for table in required_tables:
                result = conn.execute(
                    text("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables
                            WHERE table_schema = 'public'
                            AND table_name = :tname
                        )
                    """),
                    {"tname": table}
                )
                exists = result.scalar()
                if not exists:
                    missing_tables.append(table)
 
            if missing_tables:
                logger.warning(f"⚠️ Missing tables (will be created by init_db): {missing_tables}")
            else:
                logger.info("✅ All required tables exist.")
 
            return True
 
    except SQLAlchemyError as e:
        logger.error(f"❌ System integrity check failed: {e}")
        return False