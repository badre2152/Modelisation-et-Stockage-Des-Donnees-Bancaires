# 📊 Note de Modélisation — Base de Données Transactions Financières

---

## 1. Vue d'ensemble

Ce schéma modélise un système de **suivi de transactions financières** avec détection d'anomalies. Il suit une architecture **star schema** (schéma en étoile), typique des entrepôts de données analytiques (Data Warehouse / BI).

- **Table de faits centrale :** `transactions`
- **Tables de dimensions :** `clients`, `produits`, `agences`, `temps`

---

## 2. Description des entités

### 🧑 `clients`
Représente les clients de l'établissement financier.

| Colonne | Type | Rôle |
|---|---|---|
| `client_id` | int (PK) | Identifiant unique du client |
| `score_credit_client` | int | Score de crédit (risque client) |
| `segment_client` | varchar | Segmentation commerciale (ex: VIP, Standard, PME) |

**Observations :**
- Le `score_credit_client` suggère une dimension risque importante dans les analyses.
- Le `segment_client` permet des analyses de rentabilité par profil client.

---

### 📦 `produits`
Représente les produits financiers proposés.

| Colonne | Type | Rôle |
|---|---|---|
| `produit` | varchar (PK) | Nom/code du produit (PK textuelle) |
| `categorie` | varchar | Catégorie fonctionnelle (ex: Crédit, Dépôt, Change) |
| `categorie_risque` | varchar | Niveau de risque associé au produit |

**Observations :**
- La PK est une clé **naturelle textuelle** (`varchar`) — attention aux risques de doublons liés à la casse ou aux espaces.
- La double catégorisation (`categorie` + `categorie_risque`) permet une analyse multi-axe du risque produit.

---

### 🏢 `agences`
Dimension géographique / organisationnelle.

| Colonne | Type | Rôle |
|---|---|---|
| `agence` | varchar (PK) | Identifiant ou nom de l'agence |

**Observations :**
- Table très légère — pourrait être enrichie (région, ville, type d'agence).
- Clé naturelle textuelle, même vigilance que pour `produits`.

---

### 💳 `transactions` *(Table de faits)*
Cœur du modèle. Enregistre chaque opération financière.

| Colonne | Type | Rôle |
|---|---|---|
| `transaction_id` | int (PK) | Identifiant unique |
| `client_id` | int (FK) | Lien vers `clients` |
| `produit` | varchar (FK) | Lien vers `produits` |
| `agence` | varchar (FK) | Lien vers `agences` |
| `date_transaction` | date (FK) | Lien vers `temps` |
| `montant` | float | Montant dans la devise d'origine |
| `devise` | varchar | Code devise (ex: USD, MAD, EUR) |
| `taux_change_eur` | float | Taux de change appliqué vers EUR |
| `montant_eur` | float | Montant converti en EUR (calculé) |
| `type_operation` | varchar | Nature de l'opération (ex: Virement, Retrait) |
| `statut` | varchar | État de la transaction (ex: Validée, En attente, Rejetée) |
| `is_anomaly` | boolean | Flag de détection d'anomalie |
| `montant_eur_verifie` | float | Montant EUR après vérification/correction |

**Observations :**
- La présence de `montant_eur` et `montant_eur_verifie` indique un **pipeline de validation** des montants convertis — utile pour la détection de fraude ou d'erreurs de taux.
- `is_anomaly` est un champ de **résultat de scoring** (ML ou règle métier), central pour les cas d'usage de conformité (AML, KYC).
- `montant` en `float` peut poser des problèmes de précision pour des montants financiers — envisager `decimal(15,4)`.

---

### 📅 `temps` *(Dimension temporelle — optionnelle)*
Table de calendrier analytique.

| Colonne | Type | Rôle |
|---|---|---|
| `date_transaction` | date (PK) | Date pivot |
| `annee` | int | Année |
| `mois` | int | Mois (1–12) |
| `trimestre` | int | Trimestre (1–4) |
| `jour_semaine` | varchar | Nom du jour (ex: Lundi) |

**Observations :**
- Marquée **optionnelle** dans le DBML — cependant fortement recommandée pour les requêtes analytiques (agrégations par période).
- La relation est `temps.date_transaction < transactions.date_transaction` (one-to-many), ce qui est correct.

---

## 3. Analyse des relations

```
clients     ──< transactions >──  produits
                     │
agences     ──<      │      >──   temps
```

| Relation | Cardinalité | Nature |
|---|---|---|
| `clients` → `transactions` | 1:N | Un client peut avoir plusieurs transactions |
| `produits` → `transactions` | 1:N | Un produit peut apparaître dans plusieurs transactions |
| `agences` → `transactions` | 1:N | Une agence traite plusieurs transactions |
| `temps` → `transactions` | 1:N | Une date peut couvrir plusieurs transactions |

---

## 4. Cas d'usage couverts

| Cas d'usage | Champs concernés |
|---|---|
| Analyse de risque client | `score_credit_client`, `segment_client`, `is_anomaly` |
| Détection de fraude / AML | `is_anomaly`, `montant_eur`, `montant_eur_verifie`, `statut` |
| Analyse de change & exposition devises | `devise`, `taux_change_eur`, `montant_eur` |
| Reporting par agence / produit | `agence`, `produit`, `categorie`, `categorie_risque` |
| Analyse temporelle (tendances, saisonnalité) | `temps`, `date_transaction` |
| Suivi opérationnel | `type_operation`, `statut` |

---

## 5. Points forts

- ✅ Architecture **star schema** claire et adaptée à la BI
- ✅ Séparation nette entre **faits** (transactions) et **dimensions**
- ✅ Dimension de **validation de qualité** intégrée (`montant_eur_verifie`)
- ✅ Flag d'anomalie directement en table de faits — requêtes rapides
- ✅ Dimension temps séparable pour performance analytique

---

## 6. Points d'attention & recommandations

| # | Problème | Recommandation |
|---|---|---|
| 1 | `montant` en `float` | Utiliser `DECIMAL(15, 4)` pour éviter les imprécisions monétaires |
| 2 | PKs textuelles (`produit`, `agence`) | Préférer des surrogate keys (`int`) + colonne libellé séparée |
| 3 | `devise` non normalisée | Créer une table `devises` (code ISO, libellé) pour éviter les valeurs libres |
| 4 | Table `temps` optionnelle | La rendre obligatoire si le contexte est analytique/BI |
| 5 | `type_operation` et `statut` libres | Envisager des tables de référence ou des `ENUM` pour contraindre les valeurs |
| 6 | Absence d'horodatage | Ajouter `created_at`, `updated_at` sur `transactions` pour la traçabilité |
| 7 | Champ `is_anomaly` booléen seul | Ajouter un `score_anomalie float` et `raison_anomalie varchar` pour enrichir l'audit |

---

## 7. Schéma logique synthétique

```
[temps] ──────────────────────────────────────┐
                                               ↓
[clients] ──→ [transactions] ←── [produits]
                    ↑
               [agences]
```

**Pattern :** Schéma en étoile (Star Schema) — 1 table de faits, 4 dimensions.

---

*Note rédigée le 12 avril 2026 — à partir de l'analyse du diagramme DBML fourni.*
