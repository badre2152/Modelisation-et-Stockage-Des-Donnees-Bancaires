import os
import logging
import pandas as pd
from sqlalchemy.orm import sessionmaker
from db_connection import get_engine
from create_tables import Client, Produit, Agence, Temps, Transaction, PRODUIT_CATEGORIE_MAP
 
logger = logging.getLogger(__name__)
 
 
def load_csv_to_db(file_path: str):
    """
    Reads financecore_clean.csv and loads its data into the database.
 
    CSV COLUMN MAPPING REFERENCE:
    ┌──────────────────────────┬────────────────────────────────────────────────┐
    │ CSV Column               │ DB Destination                                 │
    ├──────────────────────────┼────────────────────────────────────────────────┤
    │ client_id  (CLI0023)     │ clients.client_code                            │
    │ segment_client           │ clients.segment          FIX #4                │
    │ score_credit_client      │ clients.score_credit     FIX #10               │
    │ produit                  │ produits.nom_produit     FIX #2                │
    │ (derived from produit)   │ produits.categorie       FIX #2                │
    │ agence                   │ agences.nom_agence       FIX #3                │
    │ categorie                │ transactions.type_operation (Paiement CB…)     │
    │ type_operation           │ transactions.direction   (Debit/Credit)        │
    │ is.anomaly               │ transactions.est_anomalie FIX #5               │
    │ montant_eur              │ transactions.montant_eur  FIX #10              │
    │ devise                   │ transactions.devise       FIX #10              │
    │ solde_avant              │ transactions.solde_avant  FIX #10              │
    │ canal                    │ transactions.canal → 'Inconnu' (absent) FIX #6 │
    └──────────────────────────┴────────────────────────────────────────────────┘
    """
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
        # FIX #1 #4 #10: CSV has client_id (CLI0023), segment_client, score_credit_client.
        # No nom/prenom/email/telephone/ville in this dataset.
        # Deduplicate by client_id; take the MEAN score_credit per client
        # (score may vary across rows for the same client).
        logger.info("Loading Clients...")
        client_map = {}  # client_code → client_id (PK)
 
        client_stats = (
            df.groupby('client_id')
              .agg(segment=('segment_client', 'first'),
                   score_credit=('score_credit_client', 'mean'))
              .reset_index()
        )
 
        for _, row in client_stats.iterrows():
            existing = session.query(Client).filter_by(client_code=row['client_id']).first()
            if not existing:
                client = Client(
                    client_code  = row['client_id'],
                    segment      = row['segment'],
                    score_credit = row['score_credit'],
                )
                session.add(client)
                session.flush()
                client_map[row['client_id']] = client.client_id
            else:
                client_map[row['client_id']] = existing.client_id
 
        logger.info(f"✅ {len(client_map)} clients processed.")
 
        # ── 3. PRODUITS ───────────────────────────────────────────────
        # FIX #2: CSV column is 'produit' (not 'nom_produit').
        # categorie is derived from PRODUIT_CATEGORIE_MAP (not in CSV).
        # sous_categorie and taux_interet are absent — left as defaults.
        logger.info("Loading Produits...")
        produit_map = {}  # nom_produit → produit_id
 
        for nom in df['produit'].unique():
            existing = session.query(Produit).filter_by(nom_produit=nom).first()
            if not existing:
                produit = Produit(
                    nom_produit    = nom,
                    categorie      = PRODUIT_CATEGORIE_MAP.get(nom, 'Autre'),
                    sous_categorie = None,   # not in CSV
                    taux_interet   = 0.0,    # not in CSV
                )
                session.add(produit)
                session.flush()
                produit_map[nom] = produit.produit_id
            else:
                produit_map[nom] = existing.produit_id
 
        logger.info(f"✅ {len(produit_map)} produits processed.")
 
        # ── 4. AGENCES ────────────────────────────────────────────────
        # FIX #3: CSV column is 'agence' (not 'nom_agence').
        # agence_ville and agence_region are absent in CSV — left as None.
        logger.info("Loading Agences...")
        agence_map = {}  # nom_agence → agence_id
 
        for nom in df['agence'].unique():
            existing = session.query(Agence).filter_by(nom_agence=nom).first()
            if not existing:
                agence = Agence(
                    nom_agence = nom,
                    ville      = None,   # not in CSV
                    region     = None,   # not in CSV
                )
                session.add(agence)
                session.flush()
                agence_map[nom] = agence.agence_id
            else:
                agence_map[nom] = existing.agence_id
 
        logger.info(f"✅ {len(agence_map)} agences processed.")
 
        # ── 5. TEMPS (Date Dimension) ─────────────────────────────────
        logger.info("Loading Temps (date dimension)...")
        temps_map = {}  # date_complete → temps_id
 
        for date in df['date_transaction'].dt.date.unique():
            existing = session.query(Temps).filter_by(date_complete=date).first()
            if not existing:
                dt = pd.Timestamp(date)
                temps = Temps(
                    date_complete = date,
                    jour          = dt.day,
                    mois          = dt.month,
                    trimestre     = dt.quarter,
                    annee         = dt.year,
                    jour_semaine  = dt.strftime('%A'),
                    est_weekend   = dt.weekday() >= 5,
                    est_ferie     = False,
                )
                session.add(temps)
                session.flush()
                temps_map[date] = temps.temps_id
            else:
                temps_map[date] = existing.temps_id
 
        logger.info(f"✅ {len(temps_map)} dates processed.")
 
        # ── 6. TRANSACTIONS (Bulk Insert) ─────────────────────────────
        # FIX #5:  est_anomalie ← CSV 'is.anomaly'  (dot in column name)
        # FIX #6:  canal not in CSV — defaults to 'Inconnu'
        # FIX #10: montant_eur, devise, solde_avant now stored
        # type_operation ← CSV 'categorie'  (Paiement CB, Retrait DAB…)
        # direction      ← CSV 'type_operation' (Debit / Credit)
        logger.info("Loading Transactions (bulk insert)...")
        zero_montant = df[df['montant'] == 0]
        if not zero_montant.empty:
            logger.warning(
                f"⚠️  {len(zero_montant)} rows have montant=0 (e.g. fee waivers). "
                f"They will be loaded as-is. Transaction IDs: "
                f"{list(zero_montant['transaction_id'])}"
            )
 
        transaction_objects = []
        for _, row in df.iterrows():
            transaction_objects.append(Transaction(
                client_id      = client_map[row['client_id']],
                produit_id     = produit_map[row['produit']],
                agence_id      = agence_map[row['agence']],
                temps_id       = temps_map[row['date_transaction'].date()],
                montant        = row['montant'],
                montant_eur    = row.get('montant_eur'),           # FIX #10
                devise         = row.get('devise', 'EUR'),         # FIX #10
                solde_avant    = row.get('solde_avant'),           # FIX #10
                type_operation = row.get('categorie', 'Inconnu'),  # FIX #2 (CSV 'categorie')
                direction      = row.get('type_operation', 'Inconnu'),  # Debit / Credit
                statut         = row.get('statut', 'Complete'),
                est_anomalie   = bool(row.get('is.anomaly', False)),   # FIX #5
                canal          = 'Inconnu',                        # FIX #6: not in CSV
            ))
 
        session.bulk_save_objects(transaction_objects)
        logger.info(f"✅ {len(transaction_objects):,} transactions prepared for insert.")
 
        # ── 7. COMMIT ─────────────────────────────────────────────────
        session.commit()
        logger.info("🎉 Data loading completed successfully.")
 
    except Exception as e:
        session.rollback()
        logger.error(f"Critical error during data load — rolled back all changes: {e}", exc_info=True)
        raise
 
    finally:
        session.close()