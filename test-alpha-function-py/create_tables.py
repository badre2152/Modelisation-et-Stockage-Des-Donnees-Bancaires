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
    """
    __tablename__ = 'clients'

    client_id    = Column(Integer, primary_key=True, autoincrement=True)
    nom          = Column(String(100), nullable=False)
    prenom       = Column(String(100), nullable=False)
    email        = Column(String(150), unique=True, nullable=False)
    telephone    = Column(String(20))
    ville        = Column(String(100))
    pays         = Column(String(100), default='Maroc')
    segment      = Column(String(50))   # e.g. 'VIP', 'Standard', 'Premium'
    actif        = Column(Boolean, default=True)

    transactions = relationship("Transaction", back_populates="client")

    __table_args__ = (
        Index('idx_client_email', 'email'),
        Index('idx_client_segment', 'segment'),
    )


class Produit(Base):
    """
    What product/service was involved?
    كل صف = منتج أو خدمة مالية واحدة
    """
    __tablename__ = 'produits'

    produit_id   = Column(Integer, primary_key=True, autoincrement=True)
    nom_produit  = Column(String(150), nullable=False)
    categorie    = Column(String(100))   # e.g. 'Crédit', 'Epargne', 'Assurance'
    sous_categorie = Column(String(100))
    taux_interet = Column(Float, default=0.0)
    actif        = Column(Boolean, default=True)

    transactions = relationship("Transaction", back_populates="produit")

    __table_args__ = (
        CheckConstraint('taux_interet >= 0', name='check_taux_positif'),
        Index('idx_produit_categorie', 'categorie'),
    )


class Agence(Base):
    """
    Which branch processed the transaction?
    كل صف = فرع بنكي
    """
    __tablename__ = 'agences'

    agence_id    = Column(Integer, primary_key=True, autoincrement=True)
    nom_agence   = Column(String(150), nullable=False)
    ville        = Column(String(100))
    region       = Column(String(100))
    pays         = Column(String(100), default='Maroc')
    directeur    = Column(String(150))

    transactions = relationship("Transaction", back_populates="agence")

    __table_args__ = (
        Index('idx_agence_region', 'region'),
    )


class Temps(Base):
    """
    Date dimension — one row per unique date.
    يخلي الفلترة بالتاريخ أسرع بكثير من التحويل في كل query
    """
    __tablename__ = 'temps'

    temps_id     = Column(Integer, primary_key=True, autoincrement=True)
    date_complete = Column(Date, unique=True, nullable=False)
    jour         = Column(Integer)   # 1-31
    mois         = Column(Integer)   # 1-12
    trimestre    = Column(Integer)   # 1-4
    annee        = Column(Integer)
    jour_semaine = Column(String(20))  # 'Lundi', 'Mardi'...
    est_weekend  = Column(Boolean, default=False)
    est_ferie    = Column(Boolean, default=False)

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
    يربط كل الجداول الثانية ببعض (Star Schema)
    """
    __tablename__ = 'transactions'

    transaction_id = Column(Integer, primary_key=True, autoincrement=True)
    client_id      = Column(Integer, ForeignKey('clients.client_id'), nullable=False)
    produit_id     = Column(Integer, ForeignKey('produits.produit_id'), nullable=False)
    agence_id      = Column(Integer, ForeignKey('agences.agence_id'), nullable=False)
    temps_id       = Column(Integer, ForeignKey('temps.temps_id'), nullable=False)

    montant        = Column(Float, nullable=False)
    type_operation = Column(String(50))   # 'Virement', 'Retrait', 'Depot', 'Paiement'
    statut         = Column(String(50), default='Valide')  # 'Valide', 'En attente', 'Rejeté'
    est_anomalie   = Column(Boolean, default=False)
    canal          = Column(String(50))   # 'Agence', 'Mobile', 'Web', 'ATM'

    # Relationships
    client  = relationship("Client",  back_populates="transactions")
    produit = relationship("Produit", back_populates="transactions")
    agence  = relationship("Agence",  back_populates="transactions")
    temps   = relationship("Temps",   back_populates="transactions")

    __table_args__ = (
        CheckConstraint('montant > 0', name='check_montant_positif'),
        Index('idx_transaction_client',  'client_id'),
        Index('idx_transaction_produit', 'produit_id'),
        Index('idx_transaction_agence',  'agence_id'),
        Index('idx_transaction_temps',   'temps_id'),
        Index('idx_transaction_statut',  'statut'),
    )


# ─────────────────────────────────────────────
# INIT FUNCTION
# ─────────────────────────────────────────────

def init_db():
    """
    Creates all tables, then creates SQL Views.

    BUG FIXED #2: Views now use CREATE OR REPLACE VIEW
    so re-running the pipeline won't crash if views already exist.
    """
    engine = get_engine()
    Base.metadata.create_all(engine)
    logger.info("✅ All tables created (or already exist).")

    # ✅ FIX: CREATE OR REPLACE VIEW — safe to run multiple times
    views = [
        """
        CREATE OR REPLACE VIEW vue_transactions_detail AS
        SELECT
            t.transaction_id,
            c.nom || ' ' || c.prenom   AS client_nom_complet,
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
            t.type_operation,
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
            c.nom || ' ' || c.prenom  AS client_nom_complet,
            c.segment,
            c.ville,
            COUNT(t.transaction_id)   AS nb_transactions,
            SUM(t.montant)            AS montant_total,
            AVG(t.montant)            AS montant_moyen,
            MIN(tp.date_complete)     AS premiere_transaction,
            MAX(tp.date_complete)     AS derniere_transaction
        FROM clients c
        LEFT JOIN transactions t  ON c.client_id = t.client_id
        LEFT JOIN temps        tp ON t.temps_id  = tp.temps_id
        GROUP BY c.client_id, c.nom, c.prenom, c.segment, c.ville
        """,

        """
        CREATE OR REPLACE VIEW vue_anomalies AS
        SELECT
            t.transaction_id,
            c.nom || ' ' || c.prenom  AS client_nom_complet,
            a.nom_agence,
            tp.date_complete          AS date_transaction,
            t.montant,
            t.type_operation,
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
