import os
import logging
import urllib.parse
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)
load_dotenv()

def get_engine(db_name=None):
    try:
        user = os.getenv("DB_USER")
        password = urllib.parse.quote_plus(os.getenv("DB_PASSWORD"))
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        name = db_name or os.getenv("DB_NAME")
        
        url = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{name}"
        return create_engine(url)
    except Exception as e:
        logger.error(f"Failed to generate database engine: {e}")
        raise

def create_database_if_not_exists():
    db_to_create = os.getenv("DB_NAME")
    engine = get_engine("postgres")
    
    try:
        with engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT")
            result = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname = '{db_to_create}'"))
            if not result.scalar():
                conn.execute(text(f"CREATE DATABASE {db_to_create}"))
                logger.info(f"Database '{db_to_create}' created successfully.")
            else:
                logger.info(f"Database '{db_to_create}' already exists.")
    except Exception as e:
        logger.error(f"Error while checking/creating database: {e}")