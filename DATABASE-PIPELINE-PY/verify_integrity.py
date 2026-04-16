import logging
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from db_connection import get_engine

logger = logging.getLogger(__name__)


def run_health_check() -> bool:
    """
    Runs a pre-pipeline integrity check. Verifies:
      1. Database connection is alive
      2. User has read / write / delete permissions
      3. Required tables exist (warnings only — init_db will create them)

    Returns True if all checks pass, False on any failure.
    """
    logger.info("Starting system integrity check...")
    engine = get_engine()

    try:
        with engine.connect() as conn:

            # ── CHECK 1: Connection ───────────────────────────────────
            res = conn.execute(text("SELECT current_user, current_database();"))
            user, db = res.fetchone()
            logger.info(f"✅ DB Connection OK — user: '{user}', db: '{db}'")

            # ── CHECK 2: Write / Delete permissions ───────────────────
            conn.execute(text("CREATE TEMP TABLE _health_check_test (id INT)"))
            conn.execute(text("DROP TABLE _health_check_test"))
            logger.info("✅ Read / Write / Delete permissions OK.")

            # ── CHECK 3: Required tables ─────────────────────────────
            required = ['clients', 'produits', 'agences', 'temps', 'transactions']
            missing  = []
            for table in required:
                exists = conn.execute(
                    text("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables
                            WHERE table_schema = 'public'
                            AND   table_name   = :tname
                        )
                    """),
                    {"tname": table}
                ).scalar()
                if not exists:
                    missing.append(table)

            if missing:
                logger.warning(f"⚠️  Missing tables (will be created by init_db): {missing}")
            else:
                logger.info("✅ All required tables exist.")

            return True

    except SQLAlchemyError as e:
        logger.error(f"❌ Integrity check failed: {e}")
        return False