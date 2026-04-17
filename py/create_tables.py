from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, Integer, String, Float, Date, Boolean, ForeignKey, Index, CheckConstraint, text
from db_connection import get_engine

Base = declarative_base()

# ... (Paste all your Class definitions: Client, Produit, Agence, Temps, Transaction here) ...

def init_db():
    engine = get_engine()
    Base.metadata.create_all(engine)
    
    # Create Views
    views = [
        """CREATE VIEW vue_transactions_detail AS ... """, # Paste SQL from your snippet
        """CREATE VIEW vue_transactions_client AS ... """,
        """CREATE VIEW vue_anomalies AS ... """
    ]
    
    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        for view_sql in views:
            conn.execute(text(view_sql))
    print("🚀 Tables and Views initialized.")