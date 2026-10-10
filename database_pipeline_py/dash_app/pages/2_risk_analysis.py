"""
pages/2_risk_analysis.py  —  STREAMLIT-DASHBOARD/pages/
─────────────────────────────────────────────────────────
Page 2: Analyse des Risques.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from database_pipeline_py.dash_app.utils.filters import render_sidebar
from database_pipeline_py.dash_app.utils.db import get_transactions, get_clients_risque, get_taux_defaut

try:
    st.set_page_config(
        page_title="Analyse des Risques — FinanceCore",
        page_icon="️",
        layout="wide",
    )
except Exception:
    pass

# Sidebar
filters = render_sidebar()

# Data
with st.spinner("Chargement des données de risque..."):
    df = get_transactions(
        agences  = tuple(filters["agences"]),
        produits = tuple(filters["produits"]),
        segments = tuple(filters["segments"]),
        year_min = filters["year_min"],
        year_max = filters["year_max"],
    )
    df_risque = get_clients_risque(
        segments = tuple(filters["segments"]),
        year_min = filters["year_min"],
        year_max = filters["year_max"],
    )
    df_defaut = get_taux_defaut()

if df.empty:
    st.warning("Aucune donnée pour les filtres sélectionnés.")
    st.stop()

# SAFETY GUARD — deduplicate columns
#
# FIX: if get_transactions ever returns duplicate column names
# (e.g. from a future SELECT * join), pandas silently stacks them
# into 2-D array columns which break groupby() with
# "Grouper for 'X' not 1-dimensional".
# Dropping duplicate columns here ensures this page is robust
# regardless of upstream query changes. The root cause is already
# fixed in db.py (explicit column list), but this guard stays cheap.
df = df.loc[:, ~df.columns.duplicated()]

# Header
st.title("️ Analyse des Risques")
st.caption(
    f"{len(df):,} transactions · "
    f"{int(df['is_anomaly'].sum())} anomalies · "
    f"{int((df['statut'] == 'Rejete').sum())} rejets"
)
st.markdown("---")

# 1. HEATMAP — Default rate by segment × product risk
st.subheader("🔥 Taux de défaut — Segment × Risque produit")
st.caption("Défaut = is_anomaly = TRUE ou statut = 'Rejete' · Source : vue_taux_defaut")

pivot = df_defaut.pivot(
    index   = "segment_client",
    columns = "categorie_risque",
    values  = "taux_defaut_pct",
).fillna(0)

col_order = [c for c in ["Low", "Medium", "High"] if c in pivot.columns]
pivot = pivot[col_order]

fig_heat = px.imshow(
    pivot,
    text_auto              = ".1f",
    color_continuous_scale = "RdYlGn_r",
    zmin                   = 0,
    zmax                   = pivot.values.max() * 1.1,
    labels                 = {"color": "Taux défaut %"},
    aspect                 = "auto",
)
fig_heat.update_layout(
    xaxis_title = "Risque produit",
    yaxis_title = "Segment client",
    margin      = dict(t=20, b=20, l=20, r=20),
    height      = 300,
)
st.plotly_chart(fig_heat, use_container_width=True)

worst_seg, worst_risk = pivot.stack().idxmax()
worst_val = pivot.loc[worst_seg, worst_risk]
st.info(
    f"📌 Combinaison la plus risquée : segment **{worst_seg}** × risque **{worst_risk}** "
    f"— taux de défaut **{worst_val:.1f}%**"
)

st.markdown("---")

# 2. SCATTER PLOT — Score crédit vs Montant moyen
#
# FIX: removed the debug print/diagnostic block that was left in
# production code. The underlying groupby crash is fixed in db.py
# by using an explicit SELECT column list instead of SELECT *,
# which prevented duplicate columns being returned from the JOIN.
st.subheader("🔵 Score crédit vs Montant moyen — par niveau de risque produit")

client_scatter = (
    df.groupby(["client_id", "segment_client", "score_credit_client", "categorie_risque"])
      .agg(
          montant_moyen   = ("montant_eur",    "mean"),
          nb_transactions = ("transaction_id", "count"),
          nb_anomalies    = ("is_anomaly",      "sum"),
          taux_rejet      = ("statut",          lambda x: round(100 * (x == "Rejete").mean(), 1)),
      )
      .reset_index()
)

fig_scatter = px.scatter(
    client_scatter,
    x          = "score_credit_client",
    y          = "montant_moyen",
    color      = "categorie_risque",
    color_discrete_map = {
        "Low":    "#1D9E75",
        "Medium": "#EF9F27",
        "High":   "#D85A30",
    },
    size       = "nb_transactions",
    size_max   = 30,
    hover_data = {
        "client_id":           True,
        "segment_client":      True,
        "nb_anomalies":        True,
        "taux_rejet":          True,
        "nb_transactions":     True,
        "score_credit_client": True,
        "montant_moyen":       ":.2f",
    },
    labels = {
        "score_credit_client": "Score crédit",
        "montant_moyen":       "Montant moyen (EUR)",
        "categorie_risque":    "Risque produit",
    },
    opacity = 0.75,
)
fig_scatter.add_vline(x=500, line_dash="dot", line_color="gray",
                      annotation_text="score seuil 500", annotation_position="top right")
fig_scatter.add_hline(y=0,   line_dash="dot", line_color="gray",
                      annotation_text="flux neutre",     annotation_position="bottom right")
fig_scatter.update_layout(
    legend_title_text = "Risque produit",
    margin = dict(t=20, b=20),
    height = 430,
)
st.plotly_chart(fig_scatter, use_container_width=True)

st.markdown("---")

# 3. TOP 10 CLIENTS À RISQUE
st.subheader("🚨 Top 10 clients les plus à risque")

if df_risque.empty:
    st.info("Aucune donnée de risque disponible pour les filtres sélectionnés.")
else:
    risk = df_risque.copy()

    def _norm(s: pd.Series) -> pd.Series:
        rng = s.max() - s.min()
        return (s - s.min()) / rng if rng > 0 else pd.Series(0.0, index=s.index)

    risk["risk_score"] = (
        0.40 * _norm(risk["nb_defauts"] / risk["nb_transactions"].clip(lower=1)) +
        0.35 * _norm(risk["taux_defaut_client_pct"]) +
        0.25 * _norm(-risk["score_credit_client"])
    ).round(3) * 100

    top10 = (
        risk.sort_values("risk_score", ascending=False)
            .head(10)
            .reset_index(drop=True)
    )[[
        "client_id", "segment_client", "score_credit_client",
        "flux_net_eur", "nb_transactions", "nb_defauts",
        "taux_defaut_client_pct", "profil_flux", "risk_score",
    ]]

    top10.columns = [
        "Client ID", "Segment", "Score crédit",
        "Flux net (EUR)", "Transactions", "Défauts",
        "Taux défaut %", "Profil flux", "Risk score",
    ]

    def _color_risk(val: float) -> str:
        if val >= 70:
            return "background-color: #FAECE7; color: #993C1D"
        elif val >= 40:
            return "background-color: #FAEEDA; color: #854F0B"
        else:
            return "background-color: #EAF3DE; color: #3B6D11"

    styled = (
        top10.style
             .map(_color_risk, subset=["Risk score"])
             .format({
                 "Score crédit":   "{:.0f}",
                 "Flux net (EUR)": "{:,.2f}",
                 "Taux défaut %":  "{:.1f}%",
                 "Risk score":     "{:.1f}",
             })
    )

    st.dataframe(styled, use_container_width=True, hide_index=True)

    leg1, leg2, leg3 = st.columns(3)
    leg1.markdown("🟢 **Faible**   — Risk score < 40")
    leg2.markdown("🟡 **Modéré**   — Risk score 40–70")
    leg3.markdown("🔴 **Critique** — Risk score ≥ 70")

st.markdown("---")

# 4. CSV EXPORT
st.subheader("💾 Export de l'analyse risque")

if not df_risque.empty:
    risk_csv = df_risque.to_csv(index=False).encode("utf-8")
    st.download_button(
        label     = f"⬇️  Télécharger l'analyse risque — {len(df_risque)} clients (CSV)",
        data      = risk_csv,
        file_name = f"financecore_risque_{filters['year_min']}_{filters['year_max']}.csv",
        mime      = "text/csv",
    )
else:
    st.info("Aucune donnée à exporter pour les filtres sélectionnés.")