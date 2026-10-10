"""
pages/1_vue_executive.py  —  STREAMLIT-DASHBOARD/pages/
─────────────────────────────────────────────────────────
Page 1: Vue Exécutive.
"""

import streamlit as st
import plotly.express as px
from database_pipeline_py.dash_app.utils.filters import render_sidebar
from database_pipeline_py.dash_app.utils.db import get_transactions, get_kpi_mensuel, compute_kpis

try:
    st.set_page_config(
        page_title="Vue Exécutive — FinanceCore",
        page_icon="",
        layout="wide",
    )
except Exception:
    pass

# Sidebar
filters = render_sidebar()

# Data
with st.spinner("Chargement..."):
    df = get_transactions(
        agences  = tuple(filters["agences"]),
        produits = tuple(filters["produits"]),
        segments = tuple(filters["segments"]),
        year_min = filters["year_min"],
        year_max = filters["year_max"],
    )
    df_mensuel = get_kpi_mensuel()

if df.empty:
    st.warning("Aucune donnée pour les filtres sélectionnés. Élargissez votre sélection.")
    st.stop()

# Header
st.title(" Vue Exécutive")
st.caption(
    f"{len(df):,} transactions · "
    f"{filters['year_min']}–{filters['year_max']} · "
    f"{len(filters['agences'])} agence(s) · "
    f"{len(filters['segments'])} segment(s)"
)
st.markdown("---")

# 1. KPI CARDS
# FIX: compute_kpis returns nb_clients as a plain int (nunique()),
#      so removed the erroneous .iloc[0] call that caused an
#      AttributeError when the value is already a scalar.
kpis = compute_kpis(df)

c1, c2, c3, c4 = st.columns(4)
c1.metric("💳 Volume total transactions", f"{kpis['nb_transactions']:,}")
c2.metric("💶 CA total (crédits EUR)",    f"{kpis['ca_total']:,.0f} €")
c3.metric("👥 Clients actifs",            f"{kpis['nb_clients']:,}")
c4.metric(
    label       = "📈 Montant moyen EUR",
    value       = f"{kpis['montant_moyen']:,.2f} €",
    delta       = f"{kpis['taux_anomalie']}% anomalies ({kpis['nb_anomalies']})",
    delta_color = "inverse",
)

st.markdown("---")

# 2. LINE CHART — Monthly credits vs debits evolution
st.subheader("📉 Évolution mensuelle — Crédits vs Débits")

mensuel_filtered = df_mensuel[
    (df_mensuel["annee"] >= filters["year_min"]) &
    (df_mensuel["annee"] <= filters["year_max"])
].copy()

mensuel_filtered["periode"] = (
    mensuel_filtered["annee"].astype(str) + "-" +
    mensuel_filtered["mois"].astype(str).str.zfill(2)
)

mensuel_long = mensuel_filtered.melt(
    id_vars    = ["periode", "annee", "mois"],
    value_vars = ["total_credits_eur", "total_debits_eur"],
    var_name   = "flux",
    value_name = "montant_eur",
)
mensuel_long["flux"] = mensuel_long["flux"].map({
    "total_credits_eur": "Crédits",
    "total_debits_eur":  "Débits",
})

fig_line = px.line(
    mensuel_long,
    x       = "periode",
    y       = "montant_eur",
    color   = "flux",
    color_discrete_map = {"Crédits": "#1D9E75", "Débits": "#D85A30"},
    markers = True,
    labels  = {"periode": "Période", "montant_eur": "Montant (EUR)", "flux": ""},
)
fig_line.update_layout(
    xaxis_tickangle   = -45,
    legend_title_text = "",
    margin            = dict(t=20, b=40),
    height            = 380,
)
st.plotly_chart(fig_line, use_container_width=True)

st.markdown("---")

# 3. BAR CHARTS — CA by agence and by produit (side by side)
#
# FIX: replaced the "$4" placeholder with the proper `with col_ag:`
#      block. "$4" was a template variable that was never substituted,
#      causing a SyntaxError at parse time.
st.subheader(" Chiffre d'affaires")

col_ag, col_pr = st.columns(2)

with col_ag:
    st.markdown("**Par agence**")
    ca_agence = (
        df[df["montant_eur"] > 0]
          .groupby("agence", as_index=False)["montant_eur"]
          .sum()
          .sort_values("montant_eur", ascending=True)
    )
    fig_ag = px.bar(
        ca_agence,
        x                      = "montant_eur",
        y                      = "agence",
        orientation            = "h",
        color                  = "montant_eur",
        color_continuous_scale = "teal",
        labels                 = {"montant_eur": "CA (EUR)", "agence": ""},
    )
    fig_ag.update_layout(coloraxis_showscale=False, margin=dict(t=10), height=340)
    st.plotly_chart(fig_ag, use_container_width=True)

with col_pr:
    st.markdown("**Par produit bancaire**")
    ca_produit = (
        df[df["montant_eur"] > 0]
          .groupby("produit", as_index=False)["montant_eur"]
          .sum()
          .sort_values("montant_eur", ascending=True)
    )
    fig_pr = px.bar(
        ca_produit,
        x                      = "montant_eur",
        y                      = "produit",
        orientation            = "h",
        color                  = "montant_eur",
        color_continuous_scale = "blues",
        labels                 = {"montant_eur": "CA (EUR)", "produit": ""},
    )
    fig_pr.update_layout(coloraxis_showscale=False, margin=dict(t=10), height=340)
    st.plotly_chart(fig_pr, use_container_width=True)

st.markdown("---")

# 4. PIE CHART — Client segment distribution
st.subheader("🥧 Répartition des clients par segment")

col_pie, col_tbl = st.columns([1, 1])

with col_pie:
    seg = (
        df.drop_duplicates("client_id")
          .groupby("segment_client", as_index=False)
          .size()
          .rename(columns={"size": "nb_clients"})
    )
    fig_pie = px.pie(
        seg,
        names  = "segment_client",
        values = "nb_clients",
        color  = "segment_client",
        color_discrete_map = {
            "Premium":  "#1D9E75",
            "Standard": "#378ADD",
            "Risque":   "#D85A30",
        },
        hole = 0.4,
    )
    fig_pie.update_traces(textposition="inside", textinfo="percent+label")
    fig_pie.update_layout(showlegend=False, margin=dict(t=20, b=20), height=320)
    st.plotly_chart(fig_pie, use_container_width=True)

with col_tbl:
    st.markdown("**Détail par segment**")
    seg_detail = (
        df.groupby("segment_client")
          .agg(
              nb_clients      = ("client_id",            "nunique"),
              nb_transactions = ("transaction_id",       "count"),
              ca_eur          = ("montant_eur",           lambda x: round(x[x > 0].sum(), 0)),
              score_moyen     = ("score_credit_client",  "mean"),
              anomalies_pct   = ("is_anomaly",            lambda x: round(100 * x.mean(), 1)),
          )
          .reset_index()
          .rename(columns={
              "segment_client":  "Segment",
              "nb_clients":      "Clients",
              "nb_transactions": "Transactions",
              "ca_eur":          "CA (EUR)",
              "score_moyen":     "Score moyen",
              "anomalies_pct":   "Anomalies %",
          })
    )
    seg_detail["Score moyen"] = seg_detail["Score moyen"].round(0).astype(int)
    st.dataframe(seg_detail, use_container_width=True, hide_index=True)

st.markdown("---")

# 5. CSV EXPORT
st.subheader("💾 Export des données filtrées")
csv = df.to_csv(index=False).encode("utf-8")
st.download_button(
    label     = f"⬇  Télécharger {len(df):,} transactions filtrées (CSV)",
    data      = csv,
    file_name = f"financecore_transactions_{filters['year_min']}_{filters['year_max']}.csv",
    mime      = "text/csv",
)