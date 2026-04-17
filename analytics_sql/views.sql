-- ═══════════════════════════════════════════════════════════════════
-- views.sql
-- Vues analytiques KPI — dashboard financecore
--
-- Toutes les vues utilisent CREATE OR REPLACE → idempotentes.
-- Exécuter ce fichier en entier pour initialiser ou mettre à jour
-- l'ensemble des vues dashboard.
--
-- Vues créées :
--   vue_kpi_global        — KPIs généraux de toute la base
--   vue_kpi_agence        — KPIs agrégés par agence
--   vue_kpi_segment       — KPIs par segment client
--   vue_kpi_produit       — KPIs par produit
--   vue_kpi_mensuel       — Évolution mensuelle
--   vue_clients_risque    — Clients sous la moyenne + profil de risque
--   vue_taux_defaut       — Taux de défaut segment × risque produit
-- ═══════════════════════════════════════════════════════════════════


-- ───────────────────────────────────────────────────────────────────
-- VUE 1 : KPI GLOBAL
-- Snapshot de l'ensemble du portefeuille.
-- ───────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vue_kpi_global AS
SELECT
    COUNT(DISTINCT t.client_id)                                    AS nb_clients_actifs,
    COUNT(t.transaction_id)                                        AS nb_transactions_total,
    ROUND(SUM(t.montant_eur)::numeric, 2)                          AS flux_net_total_eur,
    ROUND(AVG(t.montant_eur)::numeric, 2)                          AS montant_moyen_eur,
    ROUND(SUM(t.montant_eur) FILTER (WHERE t.montant_eur > 0)::numeric, 2) AS volume_credits_eur,
    ROUND(ABS(SUM(t.montant_eur) FILTER (WHERE t.montant_eur < 0))::numeric, 2) AS volume_debits_eur,
    COUNT(t.transaction_id) FILTER (WHERE t.is_anomaly)            AS nb_anomalies,
    ROUND(
        100.0 * COUNT(t.transaction_id) FILTER (WHERE t.is_anomaly)
        / NULLIF(COUNT(t.transaction_id), 0), 2
    )                                                              AS taux_anomalie_pct,
    COUNT(t.transaction_id) FILTER (WHERE t.statut = 'Rejete')     AS nb_rejets,
    COUNT(t.transaction_id) FILTER (WHERE t.statut = 'En attente') AS nb_en_attente,
    MIN(t.date_transaction)                                        AS date_premiere_tx,
    MAX(t.date_transaction)                                        AS date_derniere_tx
FROM transactions t;


-- ───────────────────────────────────────────────────────────────────
-- VUE 2 : KPI PAR AGENCE
-- ───────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vue_kpi_agence AS
SELECT
    t.agence,
    COUNT(DISTINCT t.client_id)                                    AS nb_clients,
    COUNT(t.transaction_id)                                        AS nb_transactions,
    ROUND(SUM(t.montant_eur)::numeric, 2)                          AS flux_net_eur,
    ROUND(AVG(t.montant_eur)::numeric, 2)                          AS montant_moyen_eur,
    ROUND(SUM(t.montant_eur) FILTER (WHERE t.montant_eur > 0)::numeric, 2) AS total_credits_eur,
    ROUND(ABS(SUM(t.montant_eur) FILTER (WHERE t.montant_eur < 0))::numeric, 2) AS total_debits_eur,
    COUNT(t.transaction_id) FILTER (WHERE t.is_anomaly)            AS nb_anomalies,
    ROUND(
        100.0 * COUNT(t.transaction_id) FILTER (WHERE t.is_anomaly)
        / NULLIF(COUNT(t.transaction_id), 0), 2
    )                                                              AS taux_anomalie_pct,
    COUNT(t.transaction_id) FILTER (WHERE t.statut = 'Rejete')     AS nb_rejets
FROM transactions t
GROUP BY t.agence
ORDER BY flux_net_eur DESC;


-- ───────────────────────────────────────────────────────────────────
-- VUE 3 : KPI PAR SEGMENT CLIENT
-- Segments : Standard | Premium | Risque
-- ───────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vue_kpi_segment AS
SELECT
    c.segment_client,
    COUNT(DISTINCT c.client_id)                                    AS nb_clients,
    COUNT(t.transaction_id)                                        AS nb_transactions,
    ROUND(AVG(c.score_credit_client)::numeric, 1)                  AS score_credit_moyen,
    ROUND(SUM(t.montant_eur)::numeric, 2)                          AS flux_net_eur,
    ROUND(AVG(t.montant_eur)::numeric, 2)                          AS montant_moyen_eur,
    COUNT(t.transaction_id) FILTER (WHERE t.is_anomaly)            AS nb_anomalies,
    ROUND(
        100.0 * COUNT(t.transaction_id) FILTER (WHERE t.is_anomaly)
        / NULLIF(COUNT(t.transaction_id), 0), 2
    )                                                              AS taux_anomalie_pct,
    SUM(CASE WHEN t.is_anomaly OR t.statut = 'Rejete' THEN 1 ELSE 0 END)
                                                                   AS nb_defauts,
    ROUND(
        100.0 * SUM(CASE WHEN t.is_anomaly OR t.statut = 'Rejete' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(t.transaction_id), 0), 2
    )                                                              AS taux_defaut_pct
FROM clients c
LEFT JOIN transactions t ON c.client_id = t.client_id
GROUP BY c.segment_client
ORDER BY taux_defaut_pct DESC;


-- ───────────────────────────────────────────────────────────────────
-- VUE 4 : KPI PAR PRODUIT
-- ───────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vue_kpi_produit AS
SELECT
    t.produit,
    p.categorie,
    p.categorie_risque,
    COUNT(DISTINCT t.client_id)                                    AS nb_clients,
    COUNT(t.transaction_id)                                        AS nb_transactions,
    ROUND(SUM(t.montant_eur)::numeric, 2)                          AS flux_net_eur,
    ROUND(AVG(t.montant_eur)::numeric, 2)                          AS montant_moyen_eur,
    ROUND(MIN(t.montant_eur)::numeric, 2)                          AS min_montant_eur,
    ROUND(MAX(t.montant_eur)::numeric, 2)                          AS max_montant_eur,
    COUNT(t.transaction_id) FILTER (WHERE t.is_anomaly)            AS nb_anomalies,
    ROUND(
        100.0 * COUNT(t.transaction_id) FILTER (WHERE t.is_anomaly)
        / NULLIF(COUNT(t.transaction_id), 0), 2
    )                                                              AS taux_anomalie_pct,
    SUM(CASE WHEN t.is_anomaly OR t.statut = 'Rejete' THEN 1 ELSE 0 END)
                                                                   AS nb_defauts,
    ROUND(
        100.0 * SUM(CASE WHEN t.is_anomaly OR t.statut = 'Rejete' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(t.transaction_id), 0), 2
    )                                                              AS taux_defaut_pct
FROM transactions t
JOIN produits p ON t.produit = p.produit
GROUP BY t.produit, p.categorie, p.categorie_risque
ORDER BY flux_net_eur DESC;


-- ───────────────────────────────────────────────────────────────────
-- VUE 5 : KPI MENSUEL — Évolution dans le temps
-- ───────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vue_kpi_mensuel AS
SELECT
    tp.annee,
    tp.mois,
    tp.trimestre,
    COUNT(t.transaction_id)                                        AS nb_transactions,
    COUNT(DISTINCT t.client_id)                                    AS nb_clients_actifs,
    ROUND(SUM(t.montant_eur)::numeric, 2)                          AS flux_net_eur,
    ROUND(AVG(t.montant_eur)::numeric, 2)                          AS montant_moyen_eur,
    ROUND(SUM(t.montant_eur) FILTER (WHERE t.montant_eur > 0)::numeric, 2) AS total_credits_eur,
    ROUND(ABS(SUM(t.montant_eur) FILTER (WHERE t.montant_eur < 0))::numeric, 2) AS total_debits_eur,
    COUNT(t.transaction_id) FILTER (WHERE t.is_anomaly)            AS nb_anomalies,
    ROUND(
        100.0 * COUNT(t.transaction_id) FILTER (WHERE t.is_anomaly)
        / NULLIF(COUNT(t.transaction_id), 0), 2
    )                                                              AS taux_anomalie_pct,
    COUNT(t.transaction_id) FILTER (WHERE t.statut = 'Rejete')     AS nb_rejets
FROM transactions t
JOIN temps tp ON t.date_transaction = tp.date_transaction
GROUP BY tp.annee, tp.mois, tp.trimestre
ORDER BY tp.annee, tp.mois;


-- ───────────────────────────────────────────────────────────────────
-- VUE 6 : CLIENTS À RISQUE
-- Clients dont le flux net est sous la moyenne nationale,
-- classés par profil de risque (1 stddev sous la moyenne = Très à risque).
-- ───────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vue_clients_risque AS
WITH flux_par_client AS (
    SELECT
        client_id,
        SUM(montant_eur)                                           AS flux_net_eur,
        COUNT(*)                                                   AS nb_transactions,
        SUM(CASE WHEN is_anomaly OR statut = 'Rejete' THEN 1 ELSE 0 END)
                                                                   AS nb_defauts
    FROM transactions
    GROUP BY client_id
),
stats_nationales AS (
    SELECT
        AVG(flux_net_eur)    AS moy_flux,
        STDDEV(flux_net_eur) AS stddev_flux
    FROM flux_par_client
)
SELECT
    f.client_id,
    c.segment_client,
    c.score_credit_client,
    ROUND(f.flux_net_eur::numeric, 2)                              AS flux_net_eur,
    ROUND(s.moy_flux::numeric, 2)                                  AS moyenne_nationale_eur,
    ROUND((f.flux_net_eur - s.moy_flux)::numeric, 2)               AS ecart_a_la_moyenne,
    f.nb_transactions,
    f.nb_defauts,
    ROUND(100.0 * f.nb_defauts / NULLIF(f.nb_transactions, 0), 2)  AS taux_defaut_client_pct,
    CASE
        WHEN f.flux_net_eur < (s.moy_flux - s.stddev_flux) THEN 'Très à risque'
        WHEN f.flux_net_eur < s.moy_flux                   THEN 'Sous la moyenne'
        ELSE                                                    'Au-dessus de la moyenne'
    END                                                            AS profil_flux
FROM flux_par_client  f
JOIN clients          c ON f.client_id = c.client_id
CROSS JOIN stats_nationales s
ORDER BY f.flux_net_eur ASC;


-- ───────────────────────────────────────────────────────────────────
-- VUE 7 : TAUX DE DÉFAUT — Segment × Risque produit
-- Défaut = is_anomaly = TRUE  OU  statut = 'Rejete'
-- ───────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vue_taux_defaut AS
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
    ROUND(AVG(c.score_credit_client)::numeric, 1)                  AS score_credit_moyen
FROM transactions  t
JOIN clients       c ON t.client_id = c.client_id
JOIN produits      p ON t.produit   = p.produit
GROUP BY c.segment_client, p.categorie_risque
ORDER BY taux_defaut_pct DESC;