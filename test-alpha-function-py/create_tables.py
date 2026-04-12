from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import (
    Column, Integer, String, Float, Date, Boolean,
    ForeignKey, Index, CheckConstraint, text
)
from db_connection import get_engine
 
Base = declarative_base()
 
 
# ─────────────────────────────────────────────
# DIMENSION TABLES (الجداول الوصفية)
# ─────────────────────────────────────────────
 
class Client(Base):
    """
    Who made the transaction?
    كل صف = عميل واحد
 
    FIX #1 & #7: Removed nom/prenom/email/telephone/ville — not present in CSV.
    Added client_code (the CLI0023 identifier from CSV) as the natural unique key.
    Added score_credit (from score_credit_client in CSV).
    """
    __tablename__ = 'clients'
 
    client_id    = Column(Integer, primary_key=True, autoincrement=True)
    client_code  = Column(String(20), unique=True, nullable=False)  # e.g. 'CLI0023'
    segment      = Column(String(50))    # 'Standard', 'Premium', 'Risque'
    score_credit = Column(Float)         # score_credit_client from CSV
    actif        = Column(Boolean, default=True)
 
    transactions = relationship("Transaction", back_populates="client")
 
    __table_args__ = (
        Index('idx_client_code',    'client_code'),
        Index('idx_client_segment', 'segment'),
    )
 
 
class Produit(Base):
    """
    What product/service was involved?
    كل صف = منتج أو خدمة مالية واحدة
 
    FIX #2: nom_produit comes from CSV column 'produit'.
    categorie is derived from the product name (no direct CSV source).
    sous_categorie and taux_interet are not available in CSV — kept nullable.
    """
    __tablename__ = 'produits'
 
    produit_id     = Column(Integer, primary_key=True, autoincrement=True)
    nom_produit    = Column(String(150), unique=True, nullable=False)  # from CSV 'produit'
    categorie      = Column(String(100))    # derived: 'Credit', 'Epargne', 'Assurance', 'Compte'
    sous_categorie = Column(String(100))    # not in CSV — nullable
    taux_interet   = Column(Float, default=0.0)  # not in CSV — defaults to 0
    actif          = Column(Boolean, default=True)
 
    transactions = relationship("Transaction", back_populates="produit")
 
    __table_args__ = (
        CheckConstraint('taux_interet >= 0', name='check_taux_positif'),
        Index('idx_produit_categorie', 'categorie'),
    )
 
 
class Agence(Base):
    """
    Which branch processed the transaction?
    كل صف = فرع بنكي
 
    FIX #3: nom_agence comes from CSV column 'agence'.
    ville and region are not available in the CSV — kept nullable.
    """
    __tablename__ = 'agences'
 
    agence_id  = Column(Integer, primary_key=True, autoincrement=True)
    nom_agence = Column(String(150), unique=True, nullable=False)  # from CSV 'agence'
    ville      = Column(String(100))   # not in CSV — nullable
    region     = Column(String(100))   # not in CSV — nullable
    pays       = Column(String(100), default='France')
    directeur  = Column(String(150))   # not in CSV — nullable
 
    transactions = relationship("Transaction", back_populates="agence")
 
    __table_args__ = (
        Index('idx_agence_region', 'region'),
    )
 
 
class Temps(Base):
    """
    Date dimension — one row per unique date.
    No changes needed — all fields are derivable from date_transaction.
    """
    __tablename__ = 'temps'
 
    temps_id      = Column(Integer, primary_key=True, autoincrement=True)
    date_complete = Column(Date, unique=True, nullable=False)
    jour          = Column(Integer)
    mois          = Column(Integer)
    trimestre     = Column(Integer)
    annee         = Column(Integer)
    jour_semaine  = Column(String(20))
    est_weekend   = Column(Boolean, default=False)
    est_ferie     = Column(Boolean, default=False)
 
    transactions = relationship("Transaction", back_populates="temps")
 
    __table_args__ = (
        Index('idx_temps_annee_mois', 'annee', 'mois'),
    )
 
 
# ─────────────────────────────────────────────
# FACT TABLE (جدول الحقائق — الأضخم)
# ─────────────────────────────────────────────
 
class Transaction(Base):
    """
    The core fact table. One row = one financial transaction.
 
    FIX #8: Removed CHECK montant > 0 — CSV has 1,005 negative montant values
            (debits are legitimately negative). Replaced with montant != 0.
    FIX #10: Added montant_eur, devise, solde_avant columns (present in CSV).
    FIX #6:  canal defaults to 'Inconnu' — column not present in CSV.
 
    COLUMN MAPPING vs CSV:
      type_operation ← CSV 'categorie'      (Paiement CB, Retrait DAB, Virement…)
      direction      ← CSV 'type_operation' (Debit / Credit)
      est_anomalie   ← CSV 'is.anomaly'
    """
    __tablename__ = 'transactions'
 
    transaction_id  = Column(Integer, primary_key=True, autoincrement=True)
    client_id       = Column(Integer, ForeignKey('clients.client_id'),  nullable=False)
    produit_id      = Column(Integer, ForeignKey('produits.produit_id'), nullable=False)
    agence_id       = Column(Integer, ForeignKey('agences.agence_id'),  nullable=False)
    temps_id        = Column(Integer, ForeignKey('temps.temps_id'),     nullable=False)
 
    montant         = Column(Float, nullable=False)      # negative = debit, positive = credit
    montant_eur     = Column(Float)                      # FIX #10: from CSV 'montant_eur'
    devise          = Column(String(10))                 # FIX #10: from CSV 'devise' (EUR/USD/CHF…)
    solde_avant     = Column(Float)                      # FIX #10: from CSV 'solde_avant'
 
    type_operation  = Column(String(50))  # from CSV 'categorie' (Paiement CB, Retrait DAB…)
    direction       = Column(String(10))  # from CSV 'type_operation' (Debit / Credit)
    statut          = Column(String(50), default='Complete')  # 'Complete', 'Rejete', 'En attente'
    est_anomalie    = Column(Boolean, default=False)     # from CSV 'is.anomaly'
    canal           = Column(String(50), default='Inconnu')   # FIX #6: not in CSV
 
    client  = relationship("Client",  back_populates="transactions")
    produit = relationship("Produit", back_populates="transactions")
    agence  = relationship("Agence",  back_populates="transactions")
    temps   = relationship("Temps",   back_populates="transactions")
 
    __table_args__ = (
        # FIX #8: montant != 0 instead of montant > 0 (debits are negative)
        # No CHECK on montant value — zeros are valid (fee waivers). nullable=False already enforced above.
        Index('idx_transaction_client',  'client_id'),
        Index('idx_transaction_produit', 'produit_id'),
        Index('idx_transaction_agence',  'agence_id'),
        Index('idx_transaction_temps',   'temps_id'),
        Index('idx_transaction_statut',  'statut'),
    )
 
 
# ─────────────────────────────────────────────
# INIT FUNCTION
# ─────────────────────────────────────────────
 
# Product name → financial category mapping (derived, not in CSV)
PRODUIT_CATEGORIE_MAP = {
    'Compte Courant':        'Compte',
    'Compte Epargne':        'Compte',
    'Livret A':              'Epargne',
    'PEA':                   'Epargne',
    'Assurance Vie':         'Assurance',
    'Credit Auto':           'Credit',
    'Credit Immobilier':     'Credit',
    'Credit Consommation':   'Credit',
}
 
 
def migrate_schema():
    """
    Safe, non-destructive schema migration.
    Adds new columns / drops old ones on EXISTING tables without touching data.
 
    Uses ADD COLUMN IF NOT EXISTS and DROP COLUMN IF EXISTS — both are idempotent
    and safe to run on tables with millions of rows (no full table rewrite in Postgres).
 
    Called automatically by init_db() before creating views.
    """
    engine = get_engine()
    migrations = [
        # ── clients: add new columns ──────────────────────────────────
        "ALTER TABLE clients ADD COLUMN IF NOT EXISTS client_code  VARCHAR(20)",
        "ALTER TABLE clients ADD COLUMN IF NOT EXISTS score_credit FLOAT",
 
        # ── clients: drop columns that no longer exist in the model ───
        "ALTER TABLE clients DROP COLUMN IF EXISTS nom",
        "ALTER TABLE clients DROP COLUMN IF EXISTS prenom",
        "ALTER TABLE clients DROP COLUMN IF EXISTS email",
        "ALTER TABLE clients DROP COLUMN IF EXISTS telephone",
        "ALTER TABLE clients DROP COLUMN IF EXISTS ville",
        "ALTER TABLE clients DROP COLUMN IF EXISTS pays",
 
        # ── transactions: add new columns ─────────────────────────────
        "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS montant_eur  FLOAT",
        "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS devise       VARCHAR(10)",
        "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS solde_avant  FLOAT",
        "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS direction    VARCHAR(10)",
 
        # ── transactions: drop old constraint that blocked negatives ───
        "ALTER TABLE transactions DROP CONSTRAINT IF EXISTS check_montant_positif",
        "ALTER TABLE transactions DROP CONSTRAINT IF EXISTS check_montant_non_zero",
    ]
 
    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        for sql in migrations:
            try:
                conn.execute(text(sql))
                logger.info(f"  migration: {sql.strip()}")
            except Exception as e:
                # Log but never crash — some DBs don't support IF NOT EXISTS on constraints
                logger.warning(f"  migration skipped ({e}): {sql.strip()}")
 
    logger.info("✅ Schema migration complete.")
 
 
def init_db():
    """
    Creates all tables, runs safe column migrations, then creates SQL Views.
 
    FIX #9: Views updated to use client_code instead of nom/prenom
            (those columns were removed from the Client model).
    Uses CREATE OR REPLACE VIEW — safe to run multiple times.
    """
    engine = get_engine()
    Base.metadata.create_all(engine)
    logger.info("✅ All tables created (or already exist).")
 
    # Always run migrations — idempotent, safe on live data
    migrate_schema()
 
    views = [
        # FIX #9: Replaced c.nom || ' ' || c.prenom with c.client_code
        """
        CREATE OR REPLACE VIEW vue_transactions_detail AS
        SELECT
            t.transaction_id,
            c.client_code,
            c.segment                  AS client_segment,
            p.nom_produit,
            p.categorie                AS produit_categorie,
            a.nom_agence,
            a.region                   AS agence_region,
            tp.date_complete           AS date_transaction,
            tp.annee,
            tp.mois,
            tp.trimestre,
            t.montant,
            t.montant_eur,
            t.devise,
            t.type_operation,
            t.direction,
            t.statut,
            t.canal,
            t.est_anomalie
        FROM transactions t
        JOIN clients  c  ON t.client_id  = c.client_id
        JOIN produits p  ON t.produit_id = p.produit_id
        JOIN agences  a  ON t.agence_id  = a.agence_id
        JOIN temps    tp ON t.temps_id   = tp.temps_id
        """,
 
        """
        CREATE OR REPLACE VIEW vue_transactions_client AS
        SELECT
            c.client_id,
            c.client_code,
            c.segment,
            c.score_credit,
            COUNT(t.transaction_id)   AS nb_transactions,
            SUM(t.montant)            AS montant_total,
            AVG(t.montant)            AS montant_moyen,
            MIN(tp.date_complete)     AS premiere_transaction,
            MAX(tp.date_complete)     AS derniere_transaction
        FROM clients c
        LEFT JOIN transactions t  ON c.client_id = t.client_id
        LEFT JOIN temps        tp ON t.temps_id  = tp.temps_id
        GROUP BY c.client_id, c.client_code, c.segment, c.score_credit
        """,
 
        """
        CREATE OR REPLACE VIEW vue_anomalies AS
        SELECT
            t.transaction_id,
            c.client_code,
            a.nom_agence,
            tp.date_complete          AS date_transaction,
            t.montant,
            t.montant_eur,
            t.devise,
            t.type_operation,
            t.direction,
            t.statut,
            t.canal
        FROM transactions t
        JOIN clients c  ON t.client_id = c.client_id
        JOIN agences a  ON t.agence_id = a.agence_id
        JOIN temps   tp ON t.temps_id  = tp.temps_id
        WHERE t.est_anomalie = TRUE
        ORDER BY tp.date_complete DESC
        """
    ]
 
    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        for view_sql in views:
            conn.execute(text(view_sql))
 
    print("🚀 Tables and Views initialized successfully.")
 
 
import logging
logger = logging.getLogger(__name__)