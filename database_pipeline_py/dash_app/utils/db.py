import os
import sys
import logging
import streamlit as st
import pandas as pd
from pathlib import Path
from sqlalchemy import text

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Reuse get_engine from the pipeline — same .env, same credentials.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'DATABASE-PIPELINE-PY'))
from database_pipeline_py.db_connection import get_engine as _get_engine

# Path to views.sql
_VIEWS_SQL = Path(__file__).parents[2] / 'ANALYTICS-SQL' / 'views.sql'


# ENGINE — one pool for the entire app lifetime

@st.cache_resource
def get_engine():
    logger.info("Initializing database engine...")
    engine = _get_engine()

    logger.info("Ensuring KPI views exist...")
    _ensure_views(engine)

    logger.info("Engine ready.")
    return engine


def _ensure_views(engine):
    """
    Creates (or replaces) all 7 KPI views from ANALYTICS-SQL/views.sql.

    FIX: The original code used engine.connect() + execution_options() which
    in SQLAlchemy 2.x does not reliably apply AUTOCOMMIT to DDL statements.
    We now use engine.begin() which wraps everything in an explicit transaction
    and commits on exit — DDL (CREATE OR REPLACE VIEW) is auto-committed by
    PostgreSQL when issued inside a transaction block.
    We also strip comment-only lines before splitting on ';' to avoid passing
    empty/comment fragments to execute().
    """
    if not _VIEWS_SQL.exists():
        logger.warning(f"views.sql not found at {_VIEWS_SQL}. Views will not be created.")
        return

    logger.info(f"Reading views from {_VIEWS_SQL}")

    sql_text = _VIEWS_SQL.read_text(encoding='utf-8')

    # Strip single-line SQL comments before splitting so we don't get
    # comment-only "statements" that confuse the CREATE check.
    lines = [ln for ln in sql_text.splitlines() if not ln.strip().startswith('--')]
    clean_sql = '\n'.join(lines)
    statements = [s.strip() for s in clean_sql.split(';') if s.strip()]

    try:
        # engine.begin() opens a connection, starts a transaction, and
        # automatically commits on __exit__ (or rolls back on exception).
        with engine.begin() as conn:
            for stmt in statements:
                if stmt.upper().lstrip().startswith('CREATE'):
                    logger.info("Executing CREATE VIEW statement...")
                    conn.execute(text(stmt))

        logger.info("All views ensured successfully.")

    except Exception as e:
        logger.error(f"Error while creating views: {e}", exc_info=True)


def _query(sql: str, params: dict | None = None) -> pd.DataFrame:
    logger.info("Executing query...")
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


# FILTER OPTIONS

@st.cache_data(ttl=3600)
def get_filter_options() -> dict:
    logger.info("Fetching filter options...")
    return {
        "agences":  _query("SELECT agence FROM agences ORDER BY agence")["agence"].tolist(),
        "produits": _query("SELECT produit FROM produits ORDER BY produit")["produit"].tolist(),
        "segments": _query("SELECT DISTINCT segment_client FROM clients ORDER BY segment_client")["segment_client"].tolist(),
        "years":    _query("SELECT DISTINCT annee FROM temps ORDER BY annee")["annee"].tolist(),
    }


# KPI GLOBAL

@st.cache_data(ttl=300)
def get_kpi_global() -> dict:
    logger.info("Fetching global KPIs...")
    row = _query("SELECT * FROM vue_kpi_global").iloc[0]
    return row.to_dict()


# TRANSACTIONS

@st.cache_data(ttl=300)
def get_transactions(agences, produits, segments, year_min, year_max) -> pd.DataFrame:
    """
    FIX: replaced SELECT * with an explicit column list.

    Using SELECT * across 4 JOINed tables produces duplicate column names
    (e.g. client_id appears in both transactions and clients, produit appears
    in both transactions and produits, date_transaction in both transactions
    and temps).  When pandas reads a result with duplicate column names it
    stacks them into a 2-D array-like column, which breaks groupby() with
    "Grouper for 'X' not 1-dimensional".

    The explicit list below selects every useful column exactly once,
    using table-qualified aliases where names clash.
    """
    logger.info("Fetching filtered transactions...")

    sql = """
        SELECT
            t.transaction_id,
            t.client_id,
            t.produit,
            t.agence,
            t.date_transaction,
            t.montant,
            t.devise,
            t.taux_change_eur,
            t.montant_eur,
            t.montant_eur_verifie,
            t.type_operation,
            t.statut,
            t.is_anomaly,

            c.segment_client,
            c.score_credit_client,

            p.categorie          AS produit_categorie,
            p.categorie_risque,

            tp.annee,
            tp.mois,
            tp.trimestre,
            tp.jour_semaine
        FROM transactions t
        JOIN clients  c  ON t.client_id       = c.client_id
        JOIN produits p  ON t.produit          = p.produit
        JOIN agences  a  ON t.agence           = a.agence
        JOIN temps    tp ON t.date_transaction = tp.date_transaction
        WHERE t.agence           = ANY(:agences)
          AND t.produit          = ANY(:produits)
          AND c.segment_client   = ANY(:segments)
          AND tp.annee BETWEEN :year_min AND :year_max
    """

    return _query(sql, {
        "agences":  list(agences),
        "produits": list(produits),
        "segments": list(segments),
        "year_min": year_min,
        "year_max": year_max,
    })


# CLIENTS RISQUE

@st.cache_data(ttl=300)
def get_clients_risque(segments, year_min, year_max) -> pd.DataFrame:
    logger.info("Fetching risky clients...")

    sql = """
        SELECT vcr.*
        FROM vue_clients_risque vcr
        JOIN clients c ON vcr.client_id = c.client_id
        WHERE c.segment_client = ANY(:segments)
          AND EXISTS (
              SELECT 1 FROM transactions t
              JOIN temps tp ON t.date_transaction = tp.date_transaction
              WHERE t.client_id = vcr.client_id
                AND tp.annee BETWEEN :year_min AND :year_max
          )
    """

    return _query(sql, {
        "segments": list(segments),
        "year_min": year_min,
        "year_max": year_max,
    })


# TAUX DEFAUT

@st.cache_data(ttl=300)
def get_taux_defaut() -> pd.DataFrame:
    logger.info("Fetching taux defaut...")
    return _query("SELECT * FROM vue_taux_defaut")


# KPI MENSUEL

@st.cache_data(ttl=300)
def get_kpi_mensuel() -> pd.DataFrame:
    logger.info("Fetching KPI mensuel...")
    return _query("SELECT * FROM vue_kpi_mensuel")


# COMPUTE KPIS

def compute_kpis(df: pd.DataFrame) -> dict:
    logger.info("Computing KPIs from DataFrame...")

    credits = df.loc[df["montant_eur"] > 0, "montant_eur"]

    return {
        "nb_transactions": len(df),
        "ca_total":        round(credits.sum(), 2),
        "nb_clients":      df["client_id"].nunique(),   # scalar int
        "montant_moyen":   round(df["montant_eur"].mean(), 2),
        "nb_anomalies":    int(df["is_anomaly"].sum()),
        "taux_anomalie":   round(100 * df["is_anomaly"].mean(), 2),
    }