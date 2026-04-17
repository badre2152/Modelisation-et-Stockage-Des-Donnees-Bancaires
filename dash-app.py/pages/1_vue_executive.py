"""
pages/1_vue_executive.py  —  STREAMLIT-DASHBOARD/pages/
─────────────────────────────────────────────────────────
Page 1: Vue Exécutive.

What this page shows and why:
  KPI CARDS — 4 scalar metrics computed from the filtered DataFrame
    via compute_kpis(). Done in Python, not SQL, because the DataFrame
    is already cached: no extra round-trip needed.

  LINE CHART (credits vs debits over time) — uses vue_kpi_mensuel
    (from ANALYTICS-SQL/views.sql) for the full time series, then
    filters it in Python by year_min/year_max. The view already has
    total_credits_eur and total_debits_eur pre-aggregated per month,
    so no groupby needed here.

  BAR CHARTS (CA by agence and by produit) — groupby on the filtered
    df. Horizontal bars because branch/product names are long strings
    that overflow vertical bars.

  PIE/DONUT (segment distribution) — counts distinct clients per
    segment, not transactions. One client with 20 transactions should
    count as one client in a segment breakdown.

  CSV EXPORT — st.download_button() converts the filtered df to CSV
    in memory (no temp file). The filename includes the filter years
    so downloaded files are self-identifying.
"""

import streamlit as st
import plotly.express as px
from utils.filters import render_sidebar
from utils.db import get_transactions, get_kpi_mensuel, compute_kpis

try:
    st.set_page_config(
        page_title="Vue Exécutive — FinanceCore",
        page_icon="📊",
        layout="wide",
    )
except Exception:
    pass  # already set by app.py if the user came through the home page

# ── Sidebar ────────────────────────────────────────────────────────
filters = render_sidebar()

# ── Data ───────────────────────────────────────────────────────────
# Lists → tuples for @st.cache_data hashing.
# The cache key is the tuple of all filter values. If nothing changed
# since the last rerun, the cached DataFrame is returned instantly.
with st.spinner("Chargement..."):
    df = get_transactions(
        agences  = tuple(filters["agences"]),
        produits = tuple(filters["produits"]),
        segments = tuple(filters["segments"]),
        year_min = filters["year_min"],
        year_max = filters["year_max"],
    )
    df_mensuel = get_kpi_mensuel()  # full time series from vue_kpi_mensuel

if df.empty:
    st.warning("Aucune donnée pour les filtres sélectionnés. Élargissez votre sélection.")
    st.stop()

# ── Header ─────────────────────────────────────────────────────────
st.title("📊 Vue Exécutive")
st.caption(
    f"{len(df):,} transactions · "
    f"{filters['year_min']}–{filters['year_max']} · "
    f"{len(filters['agences'])} agence(s) · "
    f"{len(filters['segments'])} segment(s)"
)
st.markdown("---")

# ─────────────────────────────────────────────────────────────────
# 1. KPI CARDS
# st.metric() renders a value + optional delta arrow.
# delta_color="inverse" makes positive anomaly rates red (bad),
# which is the correct semantic for a risk indicator.
# ─────────────────────────────────────────────────────────────────
kpis = compute_kpis(df)

c1, c2, c3, c4 = st.columns(4)
c1.metric("💳 Volume total transactions", f"{kpis['nb_transactions']:,}")
c2.metric("💶 CA total (crédits EUR)",    f"{kpis['ca_total']:,.0f} €")
c3.metric("👥 Clients actifs", f"{kpis['nb_clients'].iloc[0]:,}")
c4.metric(
    label       = "📈 Montant moyen EUR",
    value       = f"{kpis['montant_moyen']:,.2f} €",
    delta       = f"{kpis['taux_anomalie']}% anomalies ({kpis['nb_anomalies']})",
    delta_color = "inverse",
)

st.markdown("---")

# ─────────────────────────────────────────────────────────────────
# 2. LINE CHART — Monthly credits vs debits evolution
#
# Source: vue_kpi_mensuel (ANALYTICS-SQL/views.sql), pre-aggregated
# per month. We filter by the year slider in Python — no extra DB call.
#
# Why not use the filtered df for this chart?
#   The filtered df is already agence/produit/segment filtered. That
#   would distort the time-series shape depending on which agences are
#   selected. The line chart intentionally shows the global time trend
#   filtered only by year — a more useful executive view.
# ─────────────────────────────────────────────────────────────────
st.subheader("📉 Évolution mensuelle — Crédits vs Débits")

mensuel_filtered = df_mensuel[
    (df_mensuel["annee"] >= filters["year_min"]) &
    (df_mensuel["annee"] <= filters["year_max"])
].copy()

# Build a sortable "YYYY-MM" period string for the x-axis
mensuel_filtered["periode"] = (
    mensuel_filtered["annee"].astype(str) + "-" +
    mensuel_filtered["mois"].astype(str).str.zfill(2)
)

# Reshape from wide (one row per month) to long (one row per month×flux)
# so Plotly can map the color aesthetic to flux type.
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
    xaxis_tickangle = -45,
    legend_title_text = "",
    margin = dict(t=20, b=40),
    height = 380,
)
st.plotly_chart(fig_line, use_container_width=True)

st.markdown("---")

# ─────────────────────────────────────────────────────────────────
# 3. BAR CHARTS — CA by agence and by produit (side by side)
#
# "CA" = sum of inflows (montant_eur > 0) only.
# ascending=True on the groupby result so the largest bar is at the top
# of a horizontal bar chart (Plotly renders bottom-to-top).
# ─────────────────────────────────────────────────────────────────
st.subheader("📊 Chiffre d'affaires")

col_ag, col_pr = st.columns(2)

$4
    fig_ag = px.bar(
        ca_agence,
        x                    = "montant_eur",
        y                    = "agence",
        orientation          = "h",
        color                = "montant_eur",
        color_continuous_scale = "teal",
        labels               = {"montant_eur": "CA (EUR)", "agence": ""},
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
        x                    = "montant_eur",
        y                    = "produit",
        orientation          = "h",
        color                = "montant_eur",
        color_continuous_scale = "blues",
        labels               = {"montant_eur": "CA (EUR)", "produit": ""},
    )
    fig_pr.update_layout(coloraxis_showscale=False, margin=dict(t=10), height=340)
    st.plotly_chart(fig_pr, use_container_width=True)

st.markdown("---")

# ─────────────────────────────────────────────────────────────────
# 4. PIE CHART — Client segment distribution
#
# drop_duplicates("client_id") first: we want one count per client,
# not per transaction. A Premium client with 30 transactions should
# appear once in the Premium slice, not inflate it by 30.
# hole=0.4 → donut style: easier to read exact percentages than
# a full pie where slices near 50% are hard to distinguish.
# ─────────────────────────────────────────────────────────────────
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
              nb_clients      = ("client_id",          "nunique"),
              nb_transactions = ("transaction_id",     "count"),
              ca_eur          = ("montant_eur",         lambda x: round(x[x > 0].sum(), 0)),
              score_moyen     = ("score_credit_client", "mean"),
              anomalies_pct   = ("is_anomaly",          lambda x: round(100 * x.mean(), 1)),
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

# ─────────────────────────────────────────────────────────────────
# 5. CSV EXPORT
#
# st.download_button() generates a browser download from in-memory
# bytes — no temp file, no disk I/O needed. The filename encodes
# the active year filter so downloaded files are self-documenting.
# ─────────────────────────────────────────────────────────────────
st.subheader("💾 Export des données filtrées")
csv = df.to_csv(index=False).encode("utf-8")
st.download_button(
    label     = f"⬇️  Télécharger {len(df):,} transactions filtrées (CSV)",
    data      = csv,
    file_name = f"financecore_transactions_{filters['year_min']}_{filters['year_max']}.csv",
    mime      = "text/csv",
)