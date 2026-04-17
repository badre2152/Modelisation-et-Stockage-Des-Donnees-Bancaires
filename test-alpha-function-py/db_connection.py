import os
import re
import logging
import urllib.parse
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
 
logger = logging.getLogger(__name__)
load_dotenv(dotenv_path="test.env")
 
 
def get_engine(db_name=None):
    """
    Creates and returns a SQLAlchemy engine.
    Reads credentials from .env file.
    """
    try:
        user = os.getenv("DB_USER")
        password = urllib.parse.quote_plus(os.getenv("DB_PASSWORD", ""))
        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "5432")
        name = db_name or os.getenv("DB_NAME")
 
        if not all([user, host, port, name]):
            raise ValueError("Missing one or more required DB environment variables (DB_USER, DB_HOST, DB_PORT, DB_NAME).")
 
        url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
        return create_engine(url, pool_pre_ping=True)  # pool_pre_ping checks connection health before using it
 
    except Exception as e:
        logger.error(f"Failed to generate database engine: {e}")
        raise
 
 
def create_database_if_not_exists():
    """
    Connects to the default 'postgres' DB and creates the target DB if it doesn't exist.
 
    BUG FIXED #1: SQL Injection vulnerability.
      - SELECT query now uses parameterized query (:name) — safe ✅
      - CREATE DATABASE uses regex validation before injecting into SQL — safe ✅
    """
    db_to_create = os.getenv("DB_NAME")
 
    # ✅ FIX: Validate the DB name before using it in a raw SQL string
    if not db_to_create or not re.match(r'^[a-zA-Z0-9_]+$', db_to_create):
        raise ValueError(f"Invalid or missing DB_NAME: '{db_to_create}'. Only letters, numbers, and underscores are allowed.")
 
    engine = get_engine("postgres")
 
    try:
        with engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT")
 
            # ✅ FIX: Parameterized query — no injection possible here
            result = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": db_to_create}
            )
 
            if not result.scalar():
                # CREATE DATABASE doesn't support parameters in PostgreSQL,
                # but the name is already validated by regex above — safe ✅
                conn.execute(text(f'CREATE DATABASE "{db_to_create}"'))
                logger.info(f"Database '{db_to_create}' created successfully.")
            else:
                logger.info(f"Database '{db_to_create}' already exists.")
 
    except Exception as e:
        logger.error(f"Error while checking/creating database: {e}")
        raise