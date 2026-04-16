import logging
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import (
    Column, Integer, String, Float, Date, Boolean,
    ForeignKey, Index, text
)
from db_connection import get_engine

logger = logging.getLogger(__name__)

Base = declarative_base()


class Client(Base):
   
    __tablename__ = 'clients'

    client_id           = Column(Integer, primary_key=True, autoincrement=False)
    score_credit_client = Column(Integer)    
    segment_client      = Column(String(50))  

    transactions = relationship("Transaction", back_populates="client")

    __table_args__ = (
        Index('idx_client_segment', 'segment_client'),
    )


class Produit(Base):
    
    __tablename__ = 'produits'

    produit          = Column(String(150), primary_key=True)  
    categorie        = Column(String(100))                     
    categorie_risque = Column(String(100))                     

    transactions = relationship("Transaction", back_populates="produit_rel")

    __table_args__ = (
        Index('idx_produit_categorie', 'categorie'),
    )


class Agence(Base):
 
    __tablename__ = 'agences'

    agence = Column(String(150), primary_key=True)  # from CSV 'agence'

    transactions = relationship("Transaction", back_populates="agence_rel")


class Temps(Base):
 
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



class Transaction(Base):
   
    __tablename__ = 'transactions'

    transaction_id      = Column(Integer,      primary_key=True, autoincrement=True)
    client_id           = Column(Integer,      ForeignKey('clients.client_id'),         nullable=False)
    produit             = Column(String(150),  ForeignKey('produits.produit'),           nullable=False)
    agence              = Column(String(150),  ForeignKey('agences.agence'),             nullable=False)
    date_transaction    = Column(Date,         ForeignKey('temps.date_transaction'),     nullable=False)

    montant             = Column(Float,  nullable=False)   
    devise              = Column(String(10))               
    taux_change_eur     = Column(Float)                   
    montant_eur         = Column(Float)                    
    montant_eur_verifie = Column(Float)                    
    type_operation      = Column(String(50))               
    statut              = Column(String(50), default='Complete') 
    is_anomaly          = Column(Boolean,    default=False)       

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




def init_db():
   
    engine = get_engine()
    Base.metadata.create_all(engine)
    logger.info("✅ All tables created (or already exist).")

    operational_views = [
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
        """,
    ]

    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        for sql in operational_views:
            conn.execute(text(sql))
    logger.info("✅ Operational views created (or replaced).")
    print("🚀 Tables and views initialized successfully.")