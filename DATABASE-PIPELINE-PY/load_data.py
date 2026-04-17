import os
import re
import logging
import pandas as pd
from sqlalchemy.orm import sessionmaker
from db_connection import get_engine
from create_tables import Client, Produit, Agence, Temps, Transaction, PRODUIT_CATEGORIE_MAP

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# CSV COLUMN → DB DESTINATION
# ─────────────────────────────────────────────────────────────────
#  client_id  (CLI0023)    → clients.client_id        (parsed int)
#  segment_client          → clients.segment_client
#  score_credit_client     → clients.score_credit_client
#  produit                 → produits.produit          (natural PK)
#  categorie               → produits.categorie  (fallback: PRODUIT_CATEGORIE_MAP)
#  categorie_risque        → produits.categorie_risque
#  agence                  → agences.agence            (natural PK)
#  date_transaction        → temps.date_transaction    (natural PK)
#  annee/mois/trimestre/jour_semaine → temps.*         (derived)
#  montant                 → transactions.montant
#  devise                  → transactions.devise
#  taux_change_eur         → transactions.taux_change_eur
#  montant_eur             → transactions.montant_eur
#  montant_eur_verifie     → transactions.montant_eur_verifie
#  categorie               → transactions.type_operation
#  statut                  → transactions.statut
#  is.anomaly              → transactions.is_anomaly   (dot in CSV name!)
#
#  NOT LOADED (absent from DBML schema):
#    solde_avant, nb_transactions, montant_moyen, nb_produits,
#    type_operation (Debit/Credit), transaction_id (CSV string TXN…)
# ─────────────────────────────────────────────────────────────────


def _parse_client_id(raw: str) -> int:
    """Extract integer PK from client code: 'CLI0023' → 23"""
    m = re.search(r'\d+', str(raw))
    if not m:
        raise ValueError(f"Cannot parse integer from client_id: '{raw}'")
    return int(m.group())


def load_csv_to_db(file_path: str):
    """
    Reads financecore_clean.csv and loads all dimension + fact data.
    Idempotent: skips rows already present (checked by natural PK).
    Rolls back the entire session on any error.
    """
    if not os.path.exists(file_path):
        logger.error(f"CSV file not found: '{file_path}'")
        raise FileNotFoundError(f"CSV file not found: '{file_path}'")

    engine  = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # ── 1. READ ───────────────────────────────────────────────────
        logger.info(f"Reading '{file_path}'...")
        df = pd.read_csv(file_path)
        df['date_transaction'] = pd.to_datetime(df['date_transaction'])
        logger.info(f"Loaded {len(df):,} rows, {len(df.columns)} columns.")

        # ── 2. CLIENTS ────────────────────────────────────────────────
        logger.info("Loading Clients...")
        client_stats = (
            df.groupby('client_id')
              .agg(
                  segment_client      = ('segment_client',      'first'),
                  score_credit_client = ('score_credit_client', 'mean'),
              )
              .reset_index()
        )
        new_clients = 0
        for _, row in client_stats.iterrows():
            pk = _parse_client_id(row['client_id'])
            if session.get(Client, pk) is None:
                session.add(Client(
                    client_id           = pk,
                    segment_client      = row['segment_client'],
                    score_credit_client = round(row['score_credit_client']),
                ))
                new_clients += 1
        session.flush()
        logger.info(f"✅ Clients      — {new_clients} inserted, "
                    f"{len(client_stats) - new_clients} already existed.")

        # ── 3. PRODUITS ───────────────────────────────────────────────
        logger.info("Loading Produits...")
        has_categorie  = 'categorie'        in df.columns
        has_cat_risque = 'categorie_risque' in df.columns
        produit_meta   = df.groupby('produit').first().reset_index()

        new_produits = 0
        for _, row in produit_meta.iterrows():
            nom = row['produit']
            if session.get(Produit, nom) is None:
                categorie = (
                    row['categorie']
                    if has_categorie and pd.notna(row.get('categorie'))
                    else PRODUIT_CATEGORIE_MAP.get(nom, 'Autre')
                )
                session.add(Produit(
                    produit          = nom,
                    categorie        = categorie,
                    categorie_risque = row['categorie_risque'] if has_cat_risque else None,
                ))
                new_produits += 1
        session.flush()
        logger.info(f"✅ Produits     — {new_produits} inserted, "
                    f"{len(produit_meta) - new_produits} already existed.")

        # ── 4. AGENCES ────────────────────────────────────────────────
        logger.info("Loading Agences...")
        agence_names = df['agence'].unique()
        new_agences  = 0
        for nom in agence_names:
            if session.get(Agence, nom) is None:
                session.add(Agence(agence=nom))
                new_agences += 1
        session.flush()
        logger.info(f"✅ Agences      — {new_agences} inserted, "
                    f"{len(agence_names) - new_agences} already existed.")

        # ── 5. TEMPS ─────────────────────────────────────────────────
        logger.info("Loading Temps...")
        unique_dates = df['date_transaction'].dt.date.unique()
        new_dates    = 0
        for date in unique_dates:
            if session.get(Temps, date) is None:
                dt = pd.Timestamp(date)
                session.add(Temps(
                    date_transaction = date,
                    annee            = dt.year,
                    mois             = dt.month,
                    trimestre        = dt.quarter,
                    jour_semaine     = dt.strftime('%A'),
                ))
                new_dates += 1
        session.flush()
        logger.info(f"✅ Temps        — {new_dates} inserted, "
                    f"{len(unique_dates) - new_dates} already existed.")

        # ── 6. TRANSACTIONS ───────────────────────────────────────────
        # 'is.anomaly' has a dot in the CSV name — must use df[col] not row.get()
        logger.info("Loading Transactions (bulk insert)...")
        anomaly_col = 'is.anomaly' if 'is.anomaly' in df.columns else None

        transaction_objects = []
        for _, row in df.iterrows():
            transaction_objects.append(Transaction(
                client_id           = _parse_client_id(row['client_id']),
                produit             = row['produit'],
                agence              = row['agence'],
                date_transaction    = row['date_transaction'].date(),
                montant             = row['montant'],
                devise              = row.get('devise', 'EUR'),
                taux_change_eur     = row.get('taux_change_eur'),
                montant_eur         = row.get('montant_eur'),
                montant_eur_verifie = row.get('montant_eur_verifie'),
                type_operation      = row.get('categorie', 'Inconnu'),
                statut              = row.get('statut', 'Complete'),
                is_anomaly          = bool(row[anomaly_col]) if anomaly_col else False,
            ))
        session.bulk_save_objects(transaction_objects)
        logger.info(f"✅ Transactions — {len(transaction_objects):,} inserted.")

        # ── 7. COMMIT ─────────────────────────────────────────────────
        session.commit()
        logger.info("🎉 Data loading completed successfully.")

    except Exception as e:
        session.rollback()
        logger.error(f"Rolled back — critical error: {e}", exc_info=True)
        raise

    finally:
        session.close()