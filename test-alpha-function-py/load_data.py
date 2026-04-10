import os
import logging
import pandas as pd
from sqlalchemy.orm import sessionmaker
from db_connection import get_engine
from create_tables import Client, Produit, Agence, Temps, Transaction

logger = logging.getLogger(__name__)


def load_csv_to_db(file_path: str):
    """
    Reads a CSV file and loads its data into the database.
    Uses merge logic to avoid duplicates for dimension tables.
    Uses bulk insert for the fact table (transactions) for speed.

    BUG FIXED #3: Added explicit file existence check with a clear error message.
    """

    # ✅ FIX: Check file exists BEFORE doing anything else
    if not os.path.exists(file_path):
        logger.error(f"CSV file not found at path: '{file_path}'. Please verify the file location.")
        raise FileNotFoundError(f"CSV file not found: '{file_path}'")

    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # ── 1. READ CSV ───────────────────────────────────────────────
        logger.info(f"Reading data from '{file_path}'...")
        df = pd.read_csv(file_path)
        df['date_transaction'] = pd.to_datetime(df['date_transaction'])
        logger.info(f"Loaded {len(df):,} rows from CSV.")

        # ── 2. CLIENTS ────────────────────────────────────────────────
        logger.info("Loading Clients...")
        client_map = {}  # email → client_id (used later to link transactions)

        unique_clients = df[['client_nom', 'client_prenom', 'client_email',
                              'client_telephone', 'client_ville', 'client_segment']].drop_duplicates()

        for _, row in unique_clients.iterrows():
            existing = session.query(Client).filter_by(email=row['client_email']).first()
            if not existing:
                client = Client(
                    nom       = row['client_nom'],
                    prenom    = row['client_prenom'],
                    email     = row['client_email'],
                    telephone = row.get('client_telephone'),
                    ville     = row.get('client_ville'),
                    segment   = row.get('client_segment', 'Standard'),
                )
                session.add(client)
                session.flush()   # Flush so we can read the generated client_id immediately
                client_map[row['client_email']] = client.client_id
            else:
                client_map[row['client_email']] = existing.client_id

        logger.info(f"✅ {len(client_map)} clients processed.")

        # ── 3. PRODUITS ───────────────────────────────────────────────
        logger.info("Loading Produits...")
        produit_map = {}  # nom_produit → produit_id

        unique_produits = df[['nom_produit', 'produit_categorie',
                               'produit_sous_categorie', 'taux_interet']].drop_duplicates()

        for _, row in unique_produits.iterrows():
            existing = session.query(Produit).filter_by(nom_produit=row['nom_produit']).first()
            if not existing:
                produit = Produit(
                    nom_produit    = row['nom_produit'],
                    categorie      = row.get('produit_categorie'),
                    sous_categorie = row.get('produit_sous_categorie'),
                    taux_interet   = row.get('taux_interet', 0.0),
                )
                session.add(produit)
                session.flush()
                produit_map[row['nom_produit']] = produit.produit_id
            else:
                produit_map[row['nom_produit']] = existing.produit_id

        logger.info(f"✅ {len(produit_map)} produits processed.")

        # ── 4. AGENCES ────────────────────────────────────────────────
        logger.info("Loading Agences...")
        agence_map = {}  # nom_agence → agence_id

        unique_agences = df[['nom_agence', 'agence_ville',
                              'agence_region']].drop_duplicates()

        for _, row in unique_agences.iterrows():
            existing = session.query(Agence).filter_by(nom_agence=row['nom_agence']).first()
            if not existing:
                agence = Agence(
                    nom_agence = row['nom_agence'],
                    ville      = row.get('agence_ville'),
                    region     = row.get('agence_region'),
                )
                session.add(agence)
                session.flush()
                agence_map[row['nom_agence']] = agence.agence_id
            else:
                agence_map[row['nom_agence']] = existing.agence_id

        logger.info(f"✅ {len(agence_map)} agences processed.")

        # ── 5. TEMPS (Date Dimension) ─────────────────────────────────
        logger.info("Loading Temps (date dimension)...")
        temps_map = {}  # date_complete → temps_id

        unique_dates = df['date_transaction'].dt.date.unique()

        for date in unique_dates:
            existing = session.query(Temps).filter_by(date_complete=date).first()
            if not existing:
                dt = pd.Timestamp(date)
                temps = Temps(
                    date_complete = date,
                    jour          = dt.day,
                    mois          = dt.month,
                    trimestre     = dt.quarter,
                    annee         = dt.year,
                    jour_semaine  = dt.strftime('%A'),   # e.g. 'Monday'
                    est_weekend   = dt.weekday() >= 5,
                    est_ferie     = False,               # Can be enriched later
                )
                session.add(temps)
                session.flush()
                temps_map[date] = temps.temps_id
            else:
                temps_map[date] = existing.temps_id

        logger.info(f"✅ {len(temps_map)} dates processed.")

        # ── 6. TRANSACTIONS (Bulk Insert) ─────────────────────────────
        logger.info("Loading Transactions (bulk insert)...")

        transaction_objects = []
        for _, row in df.iterrows():
            transaction_objects.append(Transaction(
                client_id      = client_map[row['client_email']],
                produit_id     = produit_map[row['nom_produit']],
                agence_id      = agence_map[row['nom_agence']],
                temps_id       = temps_map[row['date_transaction'].date()],
                montant        = row['montant'],
                type_operation = row.get('type_operation', 'Inconnu'),
                statut         = row.get('statut', 'Valide'),
                est_anomalie   = bool(row.get('est_anomalie', False)),
                canal          = row.get('canal', 'Agence'),
            ))

        # bulk_save_objects is much faster than individual session.add() calls
        session.bulk_save_objects(transaction_objects)
        logger.info(f"✅ {len(transaction_objects):,} transactions prepared for insert.")

        # ── 7. COMMIT ─────────────────────────────────────────────────
        session.commit()
        logger.info("🎉 Data loading completed successfully.")

    except Exception as e:
        session.rollback()
        logger.error(f"Critical error during data load — rolled back all changes: {e}", exc_info=True)
        raise   # Re-raise so main.py catches it too

    finally:
        session.close()
