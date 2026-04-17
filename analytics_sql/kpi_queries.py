"""
kpi_queries.py  —  ANALYTICS-SQL/
──────────────────────────────────
Python runner for all analytical queries.
Loads queries from queries.sql, creates views from views.sql,
and exposes clean functions callable from main.py or notebooks.

Public API:
  create_dashboard_views()        → persists all KPI views in the DB
  get_aggregations()              → dict of GROUP BY / HAVING DataFrames
  get_clients_sous_moyenne()      → DataFrame (sous-requête)
  get_taux_defaut()               → DataFrame (CASE WHEN)
  get_vue_complete(limit)         → DataFrame (jointure multi-tables)
  run_all_analytics()             → dict of all results combined
  print_analytics(results)        → pretty-print to stdout
"""
import os
import logging
import pandas as pd
from pathlib import Path
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# db_connection.py lives in DATABASE-PIPELINE-PY — resolve path dynamically
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'DATABASE-PIPELINE-PY'))
from db_connection import get_engine

logger = logging.getLogger(__name__)

_HERE = Path(__file__).parent


# ─────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────

def _read_sql_file(filename: str) -> str:
    return (_HERE / filename).read_text(encoding='utf-8')


def _run(conn, sql, params: dict | None, label: str) -> pd.DataFrame:
    df = pd.read_sql(text(sql) if isinstance(sql, str) else sql, conn, params=params)
    logger.info(f"  ✔ {label}: {len(df)} lignes")
    return df


# ─────────────────────────────────────────────────────────────────
# NAMED QUERIES  (extracted from queries.sql by section)
# ─────────────────────────────────────────────────────────────────

SQL_AGG_AGENCE = """
    SELECT
        t.agence,
        COUNT(t.transaction_id)                                        AS nb_transactions,
        ROUND(SUM(t.montant_eur)::numeric, 2)                          AS total_montant_eur,
        ROUND(AVG(t.montant_eur)::numeric, 2)                          AS moyenne_montant_eur,
        ROUND(SUM(t.montant_eur) FILTER (WHERE t.montant_eur > 0)::numeric, 2)  AS total_credits_eur,
        ROUND(ABS(SUM(t.montant_eur) FILTER (WHERE t.montant_eur < 0))::numeric, 2) AS total_debits_eur,
        COUNT(t.transaction_id) FILTER (WHERE t.is_anomaly)            AS nb_anomalies,
        ROUND(
            100.0 * COUNT(t.transaction_id) FILTER (WHERE t.is_anomaly)
            / NULLIF(COUNT(t.transaction_id), 0), 2
        )                                                              AS taux_anomalie_pct
    FROM transactions t
    GROUP BY t.agence
    HAVING COUNT(t.transaction_id) >= :min_tx
    ORDER BY total_montant_eur DESC
"""

SQL_AGG_PRODUIT = """
    SELECT
        t.produit,
        p.categorie,
        p.categorie_risque,
        COUNT(t.transaction_id)                                        AS nb_transactions,
        ROUND(SUM(t.montant_eur)::numeric, 2)                          AS total_montant_eur,
        ROUND(AVG(t.montant_eur)::numeric, 2)                          AS moyenne_montant_eur,
        ROUND(MIN(t.montant_eur)::numeric, 2)                          AS min_montant_eur,
        ROUND(MAX(t.montant_eur)::numeric, 2)                          AS max_montant_eur,
        COUNT(t.transaction_id) FILTER (WHERE t.is_anomaly)            AS nb_anomalies,
        ROUND(
            100.0 * COUNT(t.transaction_id) FILTER (WHERE t.is_anomaly)
            / NULLIF(COUNT(t.transaction_id), 0), 2
        )                                                              AS taux_anomalie_pct
    FROM transactions t
    JOIN produits p ON t.produit = p.produit
    GROUP BY t.produit, p.categorie, p.categorie_risque
    HAVING COUNT(t.transaction_id) >= :min_tx
    ORDER BY total_montant_eur DESC
"""

SQL_AGG_MOIS = """
    SELECT
        tp.annee,
        tp.mois,
        tp.trimestre,
        COUNT(t.transaction_id)                                        AS nb_transactions,
        ROUND(SUM(t.montant_eur)::numeric, 2)                          AS total_montant_eur,
        ROUND(AVG(t.montant_eur)::numeric, 2)                          AS moyenne_montant_eur,
        ROUND(SUM(t.montant_eur) FILTER (WHERE t.montant_eur > 0)::numeric, 2)  AS total_credits_eur,
        ROUND(ABS(SUM(t.montant_eur) FILTER (WHERE t.montant_eur < 0))::numeric, 2) AS total_debits_eur,
        COUNT(t.transaction_id) FILTER (WHERE t.is_anomaly)            AS nb_anomalies
    FROM transactions t
    JOIN temps tp ON t.date_transaction = tp.date_transaction
    GROUP BY tp.annee, tp.mois, tp.trimestre
    HAVING COUNT(t.transaction_id) >= :min_tx
    ORDER BY tp.annee, tp.mois
"""

SQL_AGG_AGENCE_MOIS = """
    SELECT
        t.agence,
        tp.annee,
        tp.mois,
        COUNT(t.transaction_id)               AS nb_transactions,
        ROUND(SUM(t.montant_eur)::numeric, 2) AS total_montant_eur,
        ROUND(AVG(t.montant_eur)::numeric, 2) AS moyenne_montant_eur
    FROM transactions t
    JOIN temps tp ON t.date_transaction = tp.date_transaction
    GROUP BY t.agence, tp.annee, tp.mois
    HAVING COUNT(t.transaction_id) >= :min_tx
    ORDER BY t.agence, tp.annee, tp.mois
"""

SQL_AGG_PRODUIT_MOIS = """
    SELECT
        t.produit,
        p.categorie,
        tp.annee,
        tp.mois,
        COUNT(t.transaction_id)               AS nb_transactions,
        ROUND(SUM(t.montant_eur)::numeric, 2) AS total_montant_eur,
        ROUND(AVG(t.montant_eur)::numeric, 2) AS moyenne_montant_eur
    FROM transactions t
    JOIN produits p ON t.produit          = p.produit
    JOIN temps   tp ON t.date_transaction = tp.date_transaction
    GROUP BY t.produit, p.categorie, tp.annee, tp.mois
    HAVING COUNT(t.transaction_id) >= :min_tx
    ORDER BY t.produit, tp.annee, tp.mois
"""

SQL_CLIENTS_SOUS_MOYENNE = """
    WITH flux_par_client AS (
        SELECT
            client_id,
            ROUND(SUM(montant_eur)::numeric, 2)                        AS flux_net_eur,
            COUNT(transaction_id)                                      AS nb_transactions,
            SUM(CASE WHEN is_anomaly OR statut = 'Rejete' THEN 1 ELSE 0 END)
                                                                       AS nb_defauts
        FROM transactions
        GROUP BY client_id
    ),
    stats_nationales AS (
        SELECT
            ROUND(AVG(flux_net_eur)::numeric,    2) AS moy_flux_eur,
            ROUND(STDDEV(flux_net_eur)::numeric, 2) AS stddev_flux_eur
        FROM flux_par_client
    )
    SELECT
        f.client_id,
        c.segment_client,
        c.score_credit_client,
        f.flux_net_eur,
        s.moy_flux_eur                                                 AS moyenne_nationale_eur,
        ROUND((f.flux_net_eur - s.moy_flux_eur)::numeric, 2)           AS ecart_a_la_moyenne,
        f.nb_transactions,
        f.nb_defauts,
        ROUND(100.0 * f.nb_defauts / NULLIF(f.nb_transactions, 0), 2)  AS taux_defaut_client_pct,
        CASE
            WHEN f.flux_net_eur < (s.moy_flux_eur - s.stddev_flux_eur) THEN 'Très à risque'
            WHEN f.flux_net_eur <  s.moy_flux_eur                      THEN 'Sous la moyenne'
            ELSE 'Au-dessus de la moyenne'
        END                                                            AS profil_flux
    FROM flux_par_client  f
    JOIN clients          c ON f.client_id = c.client_id
    CROSS JOIN stats_nationales s
    WHERE f.flux_net_eur < s.moy_flux_eur
    ORDER BY f.flux_net_eur ASC
"""

SQL_TAUX_DEFAUT = """
    SELECT
        c.segment_client,
        p.categorie_risque,
        COUNT(t.transaction_id)                                        AS nb_total,
        SUM(CASE WHEN t.is_anomaly OR t.statut = 'Rejete' THEN 1 ELSE 0 END)
                                                                       AS nb_defauts,
        ROUND(
            100.0 * SUM(CASE WHEN t.is_anomaly OR t.statut = 'Rejete' THEN 1 ELSE 0 END)
            / NULLIF(COUNT(t.transaction_id), 0)
        , 2)                                                           AS taux_defaut_pct,
        CASE
            WHEN (100.0 * SUM(CASE WHEN t.is_anomaly OR t.statut = 'Rejete' THEN 1 ELSE 0 END)
                  / NULLIF(COUNT(t.transaction_id), 0)) >= 15 THEN 'Critique'
            WHEN (100.0 * SUM(CASE WHEN t.is_anomaly OR t.statut = 'Rejete' THEN 1 ELSE 0 END)
                  / NULLIF(COUNT(t.transaction_id), 0)) >=  8 THEN 'Élevé'
            WHEN (100.0 * SUM(CASE WHEN t.is_anomaly OR t.statut = 'Rejete' THEN 1 ELSE 0 END)
                  / NULLIF(COUNT(t.transaction_id), 0)) >=  4 THEN 'Modéré'
            ELSE 'Faible'
        END                                                            AS niveau_risque_calcule,
        ROUND(AVG(c.score_credit_client)::numeric, 1)                  AS score_credit_moyen,
        COUNT(t.transaction_id) FILTER (WHERE t.statut = 'Complete')   AS nb_complete,
        COUNT(t.transaction_id) FILTER (WHERE t.statut = 'Rejete')     AS nb_rejete,
        COUNT(t.transaction_id) FILTER (WHERE t.statut = 'En attente') AS nb_en_attente
    FROM transactions  t
    JOIN clients       c ON t.client_id = c.client_id
    JOIN produits      p ON t.produit   = p.produit
    GROUP BY c.segment_client, p.categorie_risque
    ORDER BY taux_defaut_pct DESC
"""

SQL_VUE_COMPLETE = """
    SELECT
        t.transaction_id,
        t.client_id,
        t.produit,
        t.agence,
        t.date_transaction,
        c.segment_client,
        c.score_credit_client,
        CASE
            WHEN c.score_credit_client >= 750 THEN 'Excellent'
            WHEN c.score_credit_client >= 650 THEN 'Bon'
            WHEN c.score_credit_client >= 500 THEN 'Moyen'
            ELSE 'Faible'
        END                                                            AS categorie_score_credit,
        p.categorie                                                    AS produit_categorie,
        p.categorie_risque,
        tp.annee, tp.mois, tp.trimestre, tp.jour_semaine,
        t.montant, t.devise, t.taux_change_eur,
        t.montant_eur, t.montant_eur_verifie,
        ROUND((t.montant_eur - t.montant_eur_verifie)::numeric, 4)     AS ecart_conversion,
        t.type_operation, t.statut, t.is_anomaly,
        CASE WHEN t.is_anomaly OR t.statut = 'Rejete' THEN TRUE ELSE FALSE END AS est_defaut,
        CASE
            WHEN t.montant_eur > 0 THEN 'Crédit'
            WHEN t.montant_eur < 0 THEN 'Débit'
            ELSE 'Neutre'
        END                                                            AS sens_flux
    FROM transactions  t
    JOIN clients       c  ON t.client_id       = c.client_id
    JOIN produits      p  ON t.produit          = p.produit
    JOIN agences       a  ON t.agence           = a.agence
    JOIN temps         tp ON t.date_transaction = tp.date_transaction
    ORDER BY t.date_transaction DESC, t.transaction_id DESC
    LIMIT :limit
"""


# ─────────────────────────────────────────────────────────────────
# PUBLIC FUNCTIONS
# ─────────────────────────────────────────────────────────────────

def create_dashboard_views():
    """
    Reads views.sql and executes all CREATE OR REPLACE VIEW statements.
    Idempotent — safe to call multiple times.
    """
    views_sql = _read_sql_file('views.sql')
    engine    = get_engine()

    # Split on semicolons and execute each statement individually
    statements = [s.strip() for s in views_sql.split(';') if s.strip()]

    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        created = 0
        for stmt in statements:
            if stmt.upper().startswith(('CREATE', '--')):
                if stmt.upper().startswith('CREATE'):
                    conn.execute(text(stmt))
                    created += 1

    logger.info(f"✅ {created} dashboard views created / replaced.")


def get_aggregations(min_tx_agence: int = 10,
                     min_tx_produit: int = 10,
                     min_tx_mois: int    = 5) -> dict[str, pd.DataFrame]:
    """
    Returns GROUP BY / HAVING aggregations:
      'par_agence', 'par_produit', 'par_mois',
      'agence_par_mois', 'produit_par_mois'
    """
    engine  = get_engine()
    results = {}

    with engine.connect() as conn:
        results['par_agence']      = _run(conn, SQL_AGG_AGENCE,       {'min_tx': min_tx_agence},  'par_agence')
        results['par_produit']     = _run(conn, SQL_AGG_PRODUIT,      {'min_tx': min_tx_produit}, 'par_produit')
        results['par_mois']        = _run(conn, SQL_AGG_MOIS,         {'min_tx': min_tx_mois},    'par_mois')
        results['agence_par_mois'] = _run(conn, SQL_AGG_AGENCE_MOIS,  {'min_tx': min_tx_mois},    'agence_par_mois')
        results['produit_par_mois']= _run(conn, SQL_AGG_PRODUIT_MOIS, {'min_tx': min_tx_mois},    'produit_par_mois')

    return results


def get_clients_sous_moyenne() -> pd.DataFrame:
    """Clients whose net EUR flow is below the national average (sous-requête CTE)."""
    engine = get_engine()
    with engine.connect() as conn:
        df = _run(conn, SQL_CLIENTS_SOUS_MOYENNE, None, 'clients_sous_moyenne')
    return df


def get_taux_defaut() -> pd.DataFrame:
    """Default rate by segment_client × categorie_risque (CASE WHEN)."""
    engine = get_engine()
    with engine.connect() as conn:
        df = _run(conn, SQL_TAUX_DEFAUT, None, 'taux_defaut')
    return df


def get_vue_complete(limit: int = 500) -> pd.DataFrame:
    """Full 5-table join with derived columns (capped at `limit` rows)."""
    engine = get_engine()
    with engine.connect() as conn:
        df = _run(conn, SQL_VUE_COMPLETE, {'limit': int(limit)}, f'vue_complete (limit={limit})')
    return df


def run_all_analytics() -> dict[str, pd.DataFrame]:
    """
    Runs all analytics in one call.
    Returns a dict with keys:
      Aggregations : 'par_agence', 'par_produit', 'par_mois',
                     'agence_par_mois', 'produit_par_mois'
      Analytics    : 'clients_sous_moyenne', 'taux_defaut', 'vue_complete'
    """
    logger.info("── Running all analytics ────────────────────────────────")
    results = {}
    results.update(get_aggregations())
    results['clients_sous_moyenne'] = get_clients_sous_moyenne()
    results['taux_defaut']          = get_taux_defaut()
    results['vue_complete']         = get_vue_complete(limit=100)
    logger.info("✅ All analytics complete.")
    return results


def print_analytics(results: dict[str, pd.DataFrame]):
    """Pretty-prints a 5-row preview of each result."""
    labels = {
        'par_agence':           'AGRÉGATIONS — PAR AGENCE',
        'par_produit':          'AGRÉGATIONS — PAR PRODUIT',
        'par_mois':             'AGRÉGATIONS — PAR MOIS',
        'agence_par_mois':      'AGRÉGATIONS — AGENCE × MOIS',
        'produit_par_mois':     'AGRÉGATIONS — PRODUIT × MOIS',
        'clients_sous_moyenne': 'SOUS-REQUÊTE — CLIENTS SOUS LA MOYENNE NATIONALE',
        'taux_defaut':          'CASE WHEN — TAUX DE DÉFAUT PAR SEGMENT × RISQUE',
        'vue_complete':         'JOINTURE MULTI-TABLES — VUE COMPLÈTE',
    }
    for key, df in results.items():
        print(f"\n{'═' * 62}")
        print(f"  {labels.get(key, key)}  ({len(df)} lignes)")
        print(f"{'═' * 62}")
        print(df.head(5).to_string(index=False))


# ─────────────────────────────────────────────────────────────────
# STANDALONE
# ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    create_dashboard_views()
    results = run_all_analytics()
    print_analytics(results)