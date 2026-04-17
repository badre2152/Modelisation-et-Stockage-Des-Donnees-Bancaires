"""
pages/2_risk_analysis.py  —  STREAMLIT-DASHBOARD/pages/
─────────────────────────────────────────────────────────
Page 2: Analyse des Risques.

What this page shows and why:

  HEATMAP (correlation matrix):
    Queries vue_taux_defaut (ANALYTICS-SQL/views.sql) and pivots it
    into a segment × categorie_risque matrix of taux_defaut_pct.
    Why a pivot? The view has one row per (segment, risque) pair.
    px.imshow() needs a 2D matrix (rows = segments, cols = risk levels).
    This gives the analyst a visual answer to "which segment + product
    risk combination has the highest default rate?"

  SCATTER PLOT (score crédit vs montant):
    One dot per client from the filtered DataFrame, colored by
    categorie_risque. Bubble size = nb_transactions (activity level).
    Larger bubbles = more data points → more reliable positioning.
    Hovering shows the client's full profile.

  TOP 10 RISK TABLE:
    Reads vue_clients_risque (ANALYTICS-SQL/views.sql), which already
    computes flux_net_eur, profil_flux, and taux_defaut_client_pct.
    We add a composite risk_score in Python (not SQL) because it
    combines three normalised components — easier to tune here than
    in a view.
    pd.Styler.applymap() applies cell-level CSS based on the
    risk_score value → color-coded table without a separate chart.

  CSV EXPORT of the per-client risk DataFrame.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from utils.filters import render_sidebar
from utils.db import get_transactions, get_clients_risque, get_taux_defaut

try:
    st.set_page_config(
        page_title="Analyse des Risques — FinanceCore",
        page_icon="⚠️",
        layout="wide",
    )
except Exception:
    pass

# ── Sidebar ────────────────────────────────────────────────────────
filters = render_sidebar()

# ── Data ───────────────────────────────────────────────────────────
with st.spinner("Chargement des données de risque..."):
    df = get_transactions(
        agences  = tuple(filters["agences"]),
        produits = tuple(filters["produits"]),
        segments = tuple(filters["segments"]),
        year_min = filters["year_min"],
        year_max = filters["year_max"],
    )
    # vue_clients_risque: pre-computed per-client risk metrics
    df_risque = get_clients_risque(
        segments = tuple(filters["segments"]),
        year_min = filters["year_min"],
        year_max = filters["year_max"],
    )
    # vue_taux_defaut: segment × risque produit matrix
    df_defaut = get_taux_defaut()

if df.empty:
    st.warning("Aucune donnée pour les filtres sélectionnés.")
    st.stop()

# ── Header ─────────────────────────────────────────────────────────
st.title("⚠️ Analyse des Risques")
st.caption(
    f"{len(df):,} transactions · "
    f"{int(df['is_anomaly'].sum())} anomalies · "
    f"{int((df['statut'] == 'Rejete').sum())} rejets"
)
st.markdown("---")

# ─────────────────────────────────────────────────────────────────
# 1. HEATMAP — Default rate by segment × product risk
#
# Source: vue_taux_defaut (ANALYTICS-SQL/views.sql).
# The view has one row per (segment_client, categorie_risque) pair
# with taux_defaut_pct already computed.
#
# We pivot into a 2D matrix so px.imshow() can render it as a grid:
#   rows = segment_client  (Standard, Premium, Risque)
#   cols = categorie_risque (Low, Medium, High)
#   values = taux_defaut_pct
#
# color_continuous_scale="RdYlGn_r": green=low risk, red=high risk.
# The _r suffix reverses the scale (default RdYlGn has green at top).
# ─────────────────────────────────────────────────────────────────
st.subheader("🔥 Taux de défaut — Segment × Risque produit")
st.caption("Défaut = is_anomaly = TRUE ou statut = 'Rejete' · Source : vue_taux_defaut")

pivot = df_defaut.pivot(
    index   = "segment_client",
    columns = "categorie_risque",
    values  = "taux_defaut_pct",
).fillna(0)

# Ensure column order is meaningful (Low → Medium → High)
col_order = [c for c in ["Low", "Medium", "High"] if c in pivot.columns]
pivot = pivot[col_order]

fig_heat = px.imshow(
    pivot,
    text_auto            = ".1f",
    color_continuous_scale = "RdYlGn_r",
    zmin                 = 0,
    zmax                 = pivot.values.max() * 1.1,
    labels               = {"color": "Taux défaut %"},
    aspect               = "auto",
)
fig_heat.update_layout(
    xaxis_title = "Risque produit",
    yaxis_title = "Segment client",
    margin      = dict(t=20, b=20, l=20, r=20),
    height      = 300,
)
st.plotly_chart(fig_heat, use_container_width=True)

# Auto-generated interpretation from actual data
worst_seg, worst_risk = pivot.stack().idxmax()
worst_val = pivot.loc[worst_seg, worst_risk]
st.info(
    f"📌 Combinaison la plus risquée : segment **{worst_seg}** × risque **{worst_risk}** "
    f"— taux de défaut **{worst_val:.1f}%**"
)

st.markdown("---")

# ─────────────────────────────────────────────────────────────────
# 2. SCATTER PLOT — Score crédit vs Montant moyen
#
# Aggregated to client level: one dot per unique client.
# color = categorie_risque: immediate visual separation of risk tiers.
# size = nb_transactions: more active clients get bigger dots,
#   signalling that their position is based on more data.
# hover_data: full client profile in the tooltip.
# add_vline at score=500 / add_hline at montant=0:
#   creates four quadrants that are immediately interpretable.
# ─────────────────────────────────────────────────────────────────
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

# ─────────────────────────────────────────────────────────────────
# 3. TOP 10 CLIENTS À RISQUE — color-coded table
#
# Source: vue_clients_risque (ANALYTICS-SQL/views.sql), which already
# provides flux_net_eur, ecart_a_la_moyenne, profil_flux,
# taux_defaut_client_pct.
#
# Composite risk_score computed here in Python (easier to tune than
# in a SQL view). Three normalised components:
#   40% anomaly rate (most direct fraud signal)
#   35% rejection rate (operational risk)
#   25% inverse credit score (bank's own risk rating)
#
# pd.Styler.applymap() applies CSS per cell. _color_risk() returns
# a background+color string based on the score — no extra charting
# library needed for colored cells.
# ─────────────────────────────────────────────────────────────────
st.subheader("🚨 Top 10 clients les plus à risque")

if df_risque.empty:
    st.info("Aucune donnée de risque disponible pour les filtres sélectionnés.")
else:
    risk = df_risque.copy()

    # Compute composite risk score (normalise each component to [0,1])
    def _norm(s: pd.Series) -> pd.Series:
        rng = s.max() - s.min()
        return (s - s.min()) / rng if rng > 0 else pd.Series(0.0, index=s.index)

    risk["risk_score"] = (
        0.40 * _norm(risk["nb_defauts"] / risk["nb_transactions"].clip(lower=1)) +
        0.35 * _norm(risk["taux_defaut_client_pct"]) +
        0.25 * _norm(-risk["score_credit_client"])     # lower score = higher risk
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
        """Cell-level CSS for the Risk score column."""
        if val >= 70:
            return "background-color: #FAECE7; color: #993C1D"   # red — critique
        elif val >= 40:
            return "background-color: #FAEEDA; color: #854F0B"   # amber — modéré
        else:
            return "background-color: #EAF3DE; color: #3B6D11"   # green — faible

    styled = (
        top10.style
             .applymap(_color_risk, subset=["Risk score"])
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

# ─────────────────────────────────────────────────────────────────
# 4. CSV EXPORT
# ─────────────────────────────────────────────────────────────────
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