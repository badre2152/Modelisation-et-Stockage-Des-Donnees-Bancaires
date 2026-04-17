import logging
import pandas as pd
from sqlalchemy.orm import sessionmaker
from db_connection import get_engine
from create_tables import Client, Produit, Agence, Temps, Transaction

logger = logging.getLogger(__name__)

def load_csv_to_db(file_path):
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        logger.info(f"Reading data from {file_path}")
        df = pd.read_csv(file_path)
        df['date_transaction'] = pd.to_datetime(df['date_transaction'])

        # --- Example Logging for a specific step ---
        logger.info("Loading Clients...")
        # ... (Your merge logic) ...
        
        logger.info("Loading Transactions...")
        # ... (Your bulk insert logic) ...

        session.commit()
        logger.info("Data loading completed successfully.")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Critical error during data load: {e}", exc_info=True) 
        # exc_info=True records the full stack trace in the log
    finally:
        session.close()