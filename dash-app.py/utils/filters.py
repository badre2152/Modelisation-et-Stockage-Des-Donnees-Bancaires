"""
utils/filters.py  —  STREAMLIT-DASHBOARD/utils/
─────────────────────────────────────────────────
Renders the sidebar and returns the current filter state.

Why a dedicated module?
  Both pages (1_vue_executive and 2_risk_analysis) need the exact same
  sidebar filters. If each page defined its own sidebar widgets, each
  widget would live under a different key in st.session_state → filters
  would reset every time you switch pages.

  Centralising here means:
    - One set of widget keys → one consistent session_state
    - Both pages import and call render_sidebar() → identical output
    - Filter logic lives in one place → easier to change

How st.session_state persistence works:
  Every st.* widget has a `key` parameter. Streamlit stores its value
  in st.session_state[key] and re-reads it on every rerun. Because the
  key is the same regardless of which page is active, the value persists
  across page navigation.

Empty-selection guard:
  If the user deselects all options in a multiselect, the query would
  receive an empty list → 0 rows → all charts disappear with no
  explanation. We fall back to "all options" in that case, which is
  the least surprising behaviour.
"""

import streamlit as st
from utils.db import get_filter_options


def render_sidebar() -> dict:
    """
    Draws all sidebar filter widgets and returns the selected values.

    Returns:
      {
        "agences":  list[str],   selected branch names
        "produits": list[str],   selected product names
        "segments": list[str],   selected client segments
        "year_min": int,         start year
        "year_max": int,         end year (inclusive)
      }
    """
    opts = get_filter_options()

    st.sidebar.title("🔍 Filtres")
    st.sidebar.markdown("---")

    # ── Agence ───────────────────────────────────────────────────
    agences = st.sidebar.multiselect(
        label   = "Agence",
        options = opts["agences"],
        default = opts["agences"],
        key     = "f_agences",      # persists across page switches
    )

    # ── Segment client ────────────────────────────────────────────
    # Values from DB: 'Standard', 'Premium', 'Risque'
    segments = st.sidebar.multiselect(
        label   = "Segment client",
        options = opts["segments"],
        default = opts["segments"],
        key     = "f_segments",
    )

    # ── Produit ───────────────────────────────────────────────────
    produits = st.sidebar.multiselect(
        label   = "Produit",
        options = opts["produits"],
        default = opts["produits"],
        key     = "f_produits",
    )

    # ── Période (slider d'années) ─────────────────────────────────
    # select_slider is used (not slider) because year values come from
    # the DB and are not guaranteed to be contiguous integers.
    # Returns a tuple (min_selected, max_selected).
    year_min, year_max = st.sidebar.select_slider(
        label   = "Période (année)",
        options = sorted(opts["years"]),
        value   = (min(opts["years"]), max(opts["years"])),
        key     = "f_years",
    )

    st.sidebar.markdown("---")
    st.sidebar.caption("FinanceCore SA · Dashboard v1.0")

    # ── Empty-selection guard ─────────────────────────────────────
    return {
        "agences":  agences  or opts["agences"],
        "produits": produits or opts["produits"],
        "segments": segments or opts["segments"],
        "year_min": year_min,
        "year_max": year_max,
    }