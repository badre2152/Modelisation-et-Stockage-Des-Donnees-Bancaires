import os
import sys
import logging
import streamlit as st
import pandas as pd
from pathlib import Path
from sqlalchemy import text

# ── Logging setup ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Reuse get_engine from the pipeline — same .env, same credentials.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'DATABASE-PIPELINE-PY'))
from db_connection import get_engine as _get_engine

# Path to views.sql
_VIEWS_SQL = Path(__file__).parents[2] / 'ANALYTICS-SQL' / 'views.sql'


# ─────────────────────────────────────────────────────────────────
# ENGINE — one pool for the entire app lifetime
# ─────────────────────────────────────────────────────────────────

@st.cache_resource
def get_engine():
    logger.info("Initializing database engine...")
    engine = _get_engine()

    logger.info("Ensuring KPI views exist...")
    _ensure_views(engine)

    logger.info("Engine ready.")
    return engine


def _ensure_views(engine):
    if not _VIEWS_SQL.exists():
        logger.warning(f"views.sql not found at {_VIEWS_SQL}")
        return

    logger.info(f"Reading views from {_VIEWS_SQL}")

    sql_text = _VIEWS_SQL.read_text(encoding='utf-8')
    statements = [s.strip() for s in sql_text.split(';') if s.strip()]

    try:
        with engine.connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")

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


# ─────────────────────────────────────────────────────────────────
# FILTER OPTIONS
# ─────────────────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def get_filter_options() -> dict:
    logger.info("Fetching filter options...")
    return {
        "agences":  _query("SELECT agence FROM agences ORDER BY agence")["agence"].tolist(),
        "produits": _query("SELECT produit FROM produits ORDER BY produit")["produit"].tolist(),
        "segments": _query("SELECT DISTINCT segment_client FROM clients ORDER BY segment_client")["segment_client"].tolist(),
        "years":    _query("SELECT DISTINCT annee FROM temps ORDER BY annee")["annee"].tolist(),
    }


# ─────────────────────────────────────────────────────────────────
# KPI GLOBAL
# ─────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_kpi_global() -> dict:
    logger.info("Fetching global KPIs...")
    row = _query("SELECT * FROM vue_kpi_global").iloc[0]
    return row.to_dict()


# ─────────────────────────────────────────────────────────────────
# TRANSACTIONS
# ─────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_transactions(agences, produits, segments, year_min, year_max) -> pd.DataFrame:
    logger.info("Fetching filtered transactions...")

    sql = """
        SELECT *
        FROM transactions t
        JOIN clients c ON t.client_id = c.client_id
        JOIN produits p ON t.produit = p.produit
        JOIN agences a ON t.agence = a.agence
        JOIN temps tp ON t.date_transaction = tp.date_transaction
        WHERE t.agence = ANY(:agences)
          AND t.produit = ANY(:produits)
          AND c.segment_client = ANY(:segments)
          AND tp.annee BETWEEN :year_min AND :year_max
    """

    return _query(sql, {
        "agences": list(agences),
        "produits": list(produits),
        "segments": list(segments),
        "year_min": year_min,
        "year_max": year_max,
    })


# ─────────────────────────────────────────────────────────────────
# CLIENTS RISQUE
# ─────────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────
# TAUX DEFAUT
# ─────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_taux_defaut() -> pd.DataFrame:
    logger.info("Fetching taux defaut...")
    return _query("SELECT * FROM vue_taux_defaut")


# ─────────────────────────────────────────────────────────────────
# KPI MENSUEL
# ─────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_kpi_mensuel() -> pd.DataFrame:
    logger.info("Fetching KPI mensuel...")
    return _query("SELECT * FROM vue_kpi_mensuel")


# ─────────────────────────────────────────────────────────────────
# COMPUTE KPIS
# ─────────────────────────────────────────────────────────────────

def compute_kpis(df: pd.DataFrame) -> dict:
    logger.info("Computing KPIs from DataFrame...")

    credits = df.loc[df["montant_eur"] > 0, "montant_eur"]

    return {
        "nb_transactions": len(df),
        "ca_total": round(credits.sum(), 2),
        "nb_clients": df["client_id"].nunique(),
        "montant_moyen": round(df["montant_eur"].mean(), 2),
        "nb_anomalies": int(df["is_anomaly"].sum()),
        "taux_anomalie": round(100 * df["is_anomaly"].mean(), 2),
    }
