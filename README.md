# FinanceCore

## Overview

FinanceCore est une plateforme presque complète de Data  et Analytics qui transforme des données financières brutes en informations exploitables.

Le projet simule un système BI réel comprenant :

* Pipeline ETL (Extract, Transform, Load)
* Data Warehouse PostgreSQL (modèle en étoile)
* Couche analytique SQL (KPI, règles métier)
* Dashboard interactif avec Streamlit

---

## Architecture

Raw Data (CSV)
→ ETL Pipeline (Python / Pandas)
→ Data Warehouse (PostgreSQL)
→ Validation des données
→ Couche analytique (SQL)
→ Dashboard (Streamlit)

---

## Structure du projet

project_root/

database_pipeline_py/

* load_data.py
* create_tables.py
* db_connection.py
* verify_integrity.py
* main.py

analytics_sql/

* kpi_queries.py
* queries.sql
* views.sql

dash_app/

* app.py
* pages/
* utils/

data/
docs/
erd.png
note.md
rapport_erreur.md
automation.log
requirements.txt
README.md

---

## ETL Pipeline

### Extract

* Données financières en format CSV

### Transform

* Nettoyage des données avec Pandas
* Conversion des types (dates, numériques)
* Création de nouvelles variables
* Structuration des identifiants clients

### Load

* Chargement dans PostgreSQL avec SQLAlchemy
* Insertion optimisée (bulk)

---

## Modèle de données

### Table de faits

* transactions

### Tables de dimensions

* clients
* produits
* agences
* temps

---

## Couche analytique

### KPI principaux

* Nombre total de transactions
* Volume crédit / débit
* Taux d’anomalies
* Taux de rejet

### Analyse des risques

* Taux de défaut par segment
* Classification du risque
* Profil comportemental des clients

### Analyse temporelle

* Évolution mensuelle
* Analyse trimestrielle
* Tendances saisonnières

---

## Qualité des données

Le module verify_integrity.py permet de :

* Vérifier la connexion à la base
* Valider l’existence des tables
* Contrôler les permissions
* Effectuer des tests d’intégrité

---

## Dashboard

Le dashboard propose :

* Indicateurs clés (KPI)
* Analyse des risques
* Performance par produit et agence
* Filtres interactifs
* Navigation multi-pages

---

## outiles

* Python
* Pandas
* PostgreSQL
* SQLAlchemy
* SQL
* Streamlit

---

## Exécution

### Installation

pip install -r requirements.txt

### Configuration

Créer un fichier .env :

DB_USER=...
DB_PASSWORD=...
DB_HOST=...
DB_NAME=financecore_db

### Lancer le pipeline

python database_pipeline_py/main.py

### Lancer le dashboard

streamlit run database_pipeline_py/dash_app/app.py

---

## Fonctionnalités principales

* Pipeline de données complet
* Modélisation en étoile
* Analyse avancée en SQL
* Dashboard interactif
* Architecture modulaire

---

## Limitations

* Pas de système de cache
* Dépendance à sys.path
* Couplage entre certaines couches

---

## Améliorations futures

* Ajout d’une API (FastAPI)
* Mise en cache (Redis)
* Orchestration avec Airflow
* Conteneurisation avec Docker
* Optimisation de la base (index, partitionnement)

---

## Objectif du projet

Simuler un système BI réel capable de transformer des données financières brutes en informations structurées, fiables et utiles pour la prise de décision.
