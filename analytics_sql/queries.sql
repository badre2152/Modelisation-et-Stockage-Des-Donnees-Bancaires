-- ═══════════════════════════════════════════════════════════════════
-- queries.sql
-- Requêtes analytiques — financecore_clean
--
-- Sections :
--   1. Agrégations  : GROUP BY / HAVING par agence, produit, mois
--   2. Sous-requêtes: clients avec flux net sous la moyenne nationale
--   3. CASE WHEN    : taux de défaut par segment × risque
--   4. Jointures    : vue complète multi-tables
-- ═══════════════════════════════════════════════════════════════════


-- ───────────────────────────────────────────────────────────────────
-- 1A. AGRÉGATIONS — PAR AGENCE
--     Total et moyenne des transactions par agence.
--     HAVING : agences avec au moins 10 transactions.
-- ───────────────────────────────────────────────────────────────────
SELECT
    t.agence,
    COUNT(t.transaction_id)                                        AS nb_transactions,
    ROUND(SUM(t.montant_eur)::numeric, 2)                          AS total_montant_eur,
    ROUND(AVG(t.montant_eur)::numeric, 2)                          AS moyenne_montant_eur,
    ROUND(SUM(t.montant_eur) FILTER (WHERE t.montant_eur > 0)::numeric, 2)  AS total_credits_eur,
    ROUND(ABS(SUM(t.montant_eur) FILTER (WHERE t.montant_eur < 0))::numeric, 2) AS total_debits_eur,
    COUNT(t.transaction_id)  FILTER (WHERE t.is_anomaly)           AS nb_anomalies,
    ROUND(
        100.0 * COUNT(t.transaction_id) FILTER (WHERE t.is_anomaly)
        / NULLIF(COUNT(t.transaction_id), 0), 2
    )                                                              AS taux_anomalie_pct
FROM transactions t
GROUP BY t.agence
HAVING COUNT(t.transaction_id) >= 10
ORDER BY total_montant_eur DESC;


-- ───────────────────────────────────────────────────────────────────
-- 1B. AGRÉGATIONS — PAR PRODUIT
--     Total et moyenne des transactions par produit.
--     HAVING : produits avec au moins 10 transactions.
-- ───────────────────────────────────────────────────────────────────
SELECT
    t.produit,
    p.categorie,
    p.categorie_risque,
    COUNT(t.transaction_id)                                        AS nb_transactions,
    ROUND(SUM(t.montant_eur)::numeric, 2)                          AS total_montant_eur,
    ROUND(AVG(t.montant_eur)::numeric, 2)                          AS moyenne_montant_eur,
    ROUND(MIN(t.montant_eur)::numeric, 2)                          AS min_montant_eur,
    ROUND(MAX(t.montant_eur)::numeric, 2)                          AS max_montant_eur,
    COUNT(t.transaction_id)  FILTER (WHERE t.is_anomaly)           AS nb_anomalies,
    ROUND(
        100.0 * COUNT(t.transaction_id) FILTER (WHERE t.is_anomaly)
        / NULLIF(COUNT(t.transaction_id), 0), 2
    )                                                              AS taux_anomalie_pct
FROM transactions t
JOIN produits p ON t.produit = p.produit
GROUP BY t.produit, p.categorie, p.categorie_risque
HAVING COUNT(t.transaction_id) >= 10
ORDER BY total_montant_eur DESC;


-- ───────────────────────────────────────────────────────────────────
-- 1C. AGRÉGATIONS — PAR MOIS
--     Total et moyenne des transactions par mois calendaire.
--     HAVING : mois avec au moins 5 transactions.
-- ───────────────────────────────────────────────────────────────────
SELECT
    tp.annee,
    tp.mois,
    tp.trimestre,
    COUNT(t.transaction_id)                                        AS nb_transactions,
    ROUND(SUM(t.montant_eur)::numeric, 2)                          AS total_montant_eur,
    ROUND(AVG(t.montant_eur)::numeric, 2)                          AS moyenne_montant_eur,
    ROUND(SUM(t.montant_eur) FILTER (WHERE t.montant_eur > 0)::numeric, 2)  AS total_credits_eur,
    ROUND(ABS(SUM(t.montant_eur) FILTER (WHERE t.montant_eur < 0))::numeric, 2) AS total_debits_eur,
    COUNT(t.transaction_id)  FILTER (WHERE t.is_anomaly)           AS nb_anomalies
FROM transactions t
JOIN temps tp ON t.date_transaction = tp.date_transaction
GROUP BY tp.annee, tp.mois, tp.trimestre
HAVING COUNT(t.transaction_id) >= 5
ORDER BY tp.annee, tp.mois;


-- ───────────────────────────────────────────────────────────────────
-- 1D. AGRÉGATIONS CROISÉES — AGENCE × MOIS
-- ───────────────────────────────────────────────────────────────────
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
HAVING COUNT(t.transaction_id) >= 5
ORDER BY t.agence, tp.annee, tp.mois;


-- ───────────────────────────────────────────────────────────────────
-- 1E. AGRÉGATIONS CROISÉES — PRODUIT × MOIS
-- ───────────────────────────────────────────────────────────────────
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
HAVING COUNT(t.transaction_id) >= 5
ORDER BY t.produit, tp.annee, tp.mois;


-- ═══════════════════════════════════════════════════════════════════
-- 2. SOUS-REQUÊTES — Clients avec flux net inférieur à la moyenne
--
--    Flux net d'un client = SUM(montant_eur) sur toutes ses transactions.
--    CTE 1 : calcul du flux net par client.
--    CTE 2 : calcul de la moyenne et écart-type nationaux.
--    Requête principale : filtre les clients sous la moyenne,
--    enrichis de leur profil de risque.
-- ═══════════════════════════════════════════════════════════════════
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
        WHEN f.flux_net_eur < s.moy_flux_eur                       THEN 'Sous la moyenne'
        ELSE 'Au-dessus de la moyenne'
    END                                                            AS profil_flux
FROM flux_par_client  f
JOIN clients          c ON f.client_id = c.client_id
CROSS JOIN stats_nationales s
WHERE f.flux_net_eur < s.moy_flux_eur
ORDER BY f.flux_net_eur ASC;


-- ═══════════════════════════════════════════════════════════════════
-- 3. CASE WHEN — Taux de défaut par segment × risque produit
--
--    Défaut = is_anomaly = TRUE  OU  statut = 'Rejete'.
--    CASE WHEN labellise le taux calculé en niveau de risque.
--    Segments : Standard | Premium | Risque
--    Risque produit : Low | Medium | High
-- ═══════════════════════════════════════════════════════════════════
SELECT
    c.segment_client,
    p.categorie_risque,
    COUNT(t.transaction_id)                                        AS nb_total,

    -- Nombre de défauts (CASE WHEN sur chaque ligne, agrégé)
    SUM(CASE
            WHEN t.is_anomaly = TRUE OR t.statut = 'Rejete'
            THEN 1 ELSE 0
        END)                                                       AS nb_defauts,

    -- Taux de défaut en pourcentage
    ROUND(
        100.0 * SUM(CASE WHEN t.is_anomaly OR t.statut = 'Rejete' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(t.transaction_id), 0)
    , 2)                                                           AS taux_defaut_pct,

    -- Labellisation du niveau de risque calculé
    CASE
        WHEN (100.0 * SUM(CASE WHEN t.is_anomaly OR t.statut = 'Rejete' THEN 1 ELSE 0 END)
              / NULLIF(COUNT(t.transaction_id), 0)) >= 15 THEN 'Critique'
        WHEN (100.0 * SUM(CASE WHEN t.is_anomaly OR t.statut = 'Rejete' THEN 1 ELSE 0 END)
              / NULLIF(COUNT(t.transaction_id), 0)) >=  8 THEN 'Élevé'
        WHEN (100.0 * SUM(CASE WHEN t.is_anomaly OR t.statut = 'Rejete' THEN 1 ELSE 0 END)
              / NULLIF(COUNT(t.transaction_id), 0)) >=  4 THEN 'Modéré'
        ELSE 'Faible'
    END                                                            AS niveau_risque_calcule,

    -- Score crédit moyen du segment
    ROUND(AVG(c.score_credit_client)::numeric, 1)                  AS score_credit_moyen,

    -- Répartition des statuts pour contexte
    COUNT(t.transaction_id) FILTER (WHERE t.statut = 'Complete')   AS nb_complete,
    COUNT(t.transaction_id) FILTER (WHERE t.statut = 'Rejete')     AS nb_rejete,
    COUNT(t.transaction_id) FILTER (WHERE t.statut = 'En attente') AS nb_en_attente

FROM transactions  t
JOIN clients       c ON t.client_id = c.client_id
JOIN produits      p ON t.produit   = p.produit
GROUP BY c.segment_client, p.categorie_risque
ORDER BY taux_defaut_pct DESC;


-- ═══════════════════════════════════════════════════════════════════
-- 4. JOINTURE MULTI-TABLES — Vue analytique complète
--
--    Combine les 5 tables du schéma étoile avec des colonnes dérivées.
--    Utilisée comme base pour les exports et l'exploration ad-hoc.
-- ═══════════════════════════════════════════════════════════════════
SELECT
    -- Identifiants
    t.transaction_id,
    t.client_id,
    t.produit,
    t.agence,
    t.date_transaction,

    -- Dimension Client
    c.segment_client,
    c.score_credit_client,
    CASE
        WHEN c.score_credit_client >= 750 THEN 'Excellent'
        WHEN c.score_credit_client >= 650 THEN 'Bon'
        WHEN c.score_credit_client >= 500 THEN 'Moyen'
        ELSE                                   'Faible'
    END                                                            AS categorie_score_credit,

    -- Dimension Produit
    p.categorie                                                    AS produit_categorie,
    p.categorie_risque,

    -- Dimension Temps
    tp.annee,
    tp.mois,
    tp.trimestre,
    tp.jour_semaine,

    -- Mesures
    t.montant,
    t.devise,
    t.taux_change_eur,
    t.montant_eur,
    t.montant_eur_verifie,
    ROUND((t.montant_eur - t.montant_eur_verifie)::numeric, 4)     AS ecart_conversion,
    t.type_operation,
    t.statut,
    t.is_anomaly,

    -- Drapeaux dérivés
    CASE
        WHEN t.is_anomaly = TRUE OR t.statut = 'Rejete' THEN TRUE
        ELSE FALSE
    END                                                            AS est_defaut,
    CASE
        WHEN t.montant_eur > 0 THEN 'Crédit'
        WHEN t.montant_eur < 0 THEN 'Débit'
        ELSE                        'Neutre'
    END                                                            AS sens_flux

FROM transactions  t
JOIN clients       c  ON t.client_id       = c.client_id
JOIN produits      p  ON t.produit          = p.produit
JOIN agences       a  ON t.agence           = a.agence
JOIN temps         tp ON t.date_transaction = tp.date_transaction
ORDER BY t.date_transaction DESC, t.transaction_id DESC;