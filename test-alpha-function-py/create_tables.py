import logging
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import (
    Column, Integer, String, Float, Date, Boolean,
    ForeignKey, Index, text
)
from db_connection import get_engine
 
logger = logging.getLogger(__name__)
 
Base = declarative_base()
 
 
# ─────────────────────────────────────────────
# DIMENSION TABLES
# ─────────────────────────────────────────────
 
class Client(Base):
    """
    One row = one client.
    client_id is set explicitly from the CSV (numeric part of 'CLI0023' → 23).
    autoincrement=False so load_data.py controls the PK value.
    """
    __tablename__ = 'clients'
 
    client_id           = Column(Integer, primary_key=True, autoincrement=False)
    score_credit_client = Column(Integer)       # from CSV 'score_credit_client'
    segment_client      = Column(String(50))    # from CSV 'segment_client'
 
    transactions = relationship("Transaction", back_populates="client")
 
    __table_args__ = (
        Index('idx_client_segment', 'segment_client'),
    )
 
 
class Produit(Base):
    """
    One row = one financial product/service.
    Natural PK: produit varchar (the product name string from CSV).
    """
    __tablename__ = 'produits'
 
    produit          = Column(String(150), primary_key=True)  # from CSV 'produit'
    categorie        = Column(String(100))                     # from CSV 'categorie' or PRODUIT_CATEGORIE_MAP
    categorie_risque = Column(String(100))                     # from CSV 'categorie_risque'
 
    # back_populates must match the relationship name in Transaction: 'produit_rel'
    transactions = relationship("Transaction", back_populates="produit_rel")
 
    __table_args__ = (
        Index('idx_produit_categorie', 'categorie'),
    )
 
 
class Agence(Base):
    """
    One row = one bank branch.
    Natural PK: agence varchar (the branch name string from CSV).
    """
    __tablename__ = 'agences'
 
    agence = Column(String(150), primary_key=True)  # from CSV 'agence'
 
    # back_populates must match the relationship name in Transaction: 'agence_rel'
    transactions = relationship("Transaction", back_populates="agence_rel")
 
 
class Temps(Base):
    """
    Date dimension — one row per unique transaction date.
    Natural PK: date_transaction (date from CSV).
    """
    __tablename__ = 'temps'
 
    date_transaction = Column(Date, primary_key=True)
    annee            = Column(Integer)
    mois             = Column(Integer)
    trimestre        = Column(Integer)
    jour_semaine     = Column(String(20))
 
    transactions = relationship("Transaction", back_populates="temps")
 
    __table_args__ = (
        Index('idx_temps_annee_mois', 'annee', 'mois'),
    )
 
 
# ─────────────────────────────────────────────
# FACT TABLE
# ─────────────────────────────────────────────
 
class Transaction(Base):
    """
    Core fact table. One row = one financial transaction.
 
    FK column → referenced table / column:
      client_id        → clients.client_id        (int)
      produit          → produits.produit          (varchar natural key)
      agence           → agences.agence            (varchar natural key)
      date_transaction → temps.date_transaction    (date natural key)
 
    Relationship names deliberately distinct from FK column names
    to avoid SQLAlchemy ambiguity:
      produit_rel, agence_rel  (FK columns are 'produit', 'agence')
      client, temps            (no naming conflict)
    """
    __tablename__ = 'transactions'
 
    transaction_id      = Column(Integer,      primary_key=True, autoincrement=True)
    client_id           = Column(Integer,      ForeignKey('clients.client_id'),         nullable=False)
    produit             = Column(String(150),  ForeignKey('produits.produit'),           nullable=False)
    agence              = Column(String(150),  ForeignKey('agences.agence'),             nullable=False)
    date_transaction    = Column(Date,         ForeignKey('temps.date_transaction'),     nullable=False)
 
    montant             = Column(Float,        nullable=False)   # negative = debit
    devise              = Column(String(10))                     # 'EUR', 'USD', 'CHF' …
    taux_change_eur     = Column(Float)                          # exchange rate to EUR
    montant_eur         = Column(Float)                          # amount converted to EUR
    type_operation      = Column(String(50))                     # from CSV 'categorie' (Paiement CB, Retrait DAB …)
    statut              = Column(String(50),   default='Complete')
    is_anomaly          = Column(Boolean,      default=False)    # from CSV 'is.anomaly'
    montant_eur_verifie = Column(Float)                          # from CSV 'montant_eur_verifie'
 
    client      = relationship("Client",  back_populates="transactions")
    produit_rel = relationship("Produit", back_populates="transactions")
    agence_rel  = relationship("Agence",  back_populates="transactions")
    temps       = relationship("Temps",   back_populates="transactions")
 
    __table_args__ = (
        Index('idx_transaction_client',  'client_id'),
        Index('idx_transaction_produit', 'produit'),
        Index('idx_transaction_agence',  'agence'),
        Index('idx_transaction_date',    'date_transaction'),
        Index('idx_transaction_statut',  'statut'),
    )
 
 
# ─────────────────────────────────────────────
# PRODUCT CATEGORY MAP
# Fallback: derives 'categorie' from product name when the CSV column is absent.
# ─────────────────────────────────────────────
 
PRODUIT_CATEGORIE_MAP = {
    'Compte Courant':      'Compte',
    'Compte Epargne':      'Compte',
    'Livret A':            'Epargne',
    'PEA':                 'Epargne',
    'Assurance Vie':       'Assurance',
    'Credit Auto':         'Credit',
    'Credit Immobilier':   'Credit',
    'Credit Consommation': 'Credit',
}
 
 
# ─────────────────────────────────────────────
# INIT
# ─────────────────────────────────────────────
 
def init_db():
    """
    Creates all tables (idempotent) then creates/replaces SQL views.
    Safe to run multiple times.
    """
    engine = get_engine()
    Base.metadata.create_all(engine)
    logger.info("✅ All tables created (or already exist).")
 
    views = [
        """
        CREATE OR REPLACE VIEW vue_transactions_detail AS
        SELECT
            t.transaction_id,
            t.client_id,
            c.segment_client,
            c.score_credit_client,
            t.produit,
            p.categorie          AS produit_categorie,
            p.categorie_risque,
            t.agence,
            t.date_transaction,
            tp.annee,
            tp.mois,
            tp.trimestre,
            tp.jour_semaine,
            t.montant,
            t.devise,
            t.taux_change_eur,
            t.montant_eur,
            t.montant_eur_verifie,
            t.type_operation,
            t.statut,
            t.is_anomaly
        FROM transactions t
        JOIN clients  c  ON t.client_id       = c.client_id
        JOIN produits p  ON t.produit          = p.produit
        JOIN agences  a  ON t.agence           = a.agence
        JOIN temps    tp ON t.date_transaction = tp.date_transaction
        """,
 
        """
        CREATE OR REPLACE VIEW vue_transactions_client AS
        SELECT
            c.client_id,
            c.segment_client,
            c.score_credit_client,
            COUNT(t.transaction_id) AS nb_transactions,
            SUM(t.montant_eur)      AS montant_eur_total,
            AVG(t.montant_eur)      AS montant_eur_moyen,
            MIN(t.date_transaction) AS premiere_transaction,
            MAX(t.date_transaction) AS derniere_transaction
        FROM clients c
        LEFT JOIN transactions t ON c.client_id = t.client_id
        GROUP BY c.client_id, c.segment_client, c.score_credit_client
        """,
 
        """
        CREATE OR REPLACE VIEW vue_anomalies AS
        SELECT
            t.transaction_id,
            t.client_id,
            t.agence,
            t.date_transaction,
            t.montant,
            t.montant_eur,
            t.devise,
            t.type_operation,
            t.statut
        FROM transactions t
        WHERE t.is_anomaly = TRUE
        ORDER BY t.date_transaction DESC
        """
    ]
 
    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        for view_sql in views:
            conn.execute(text(view_sql))
        logger.info("✅ Views created (or replaced).")
 
    print("🚀 Tables and Views initialized successfully.")