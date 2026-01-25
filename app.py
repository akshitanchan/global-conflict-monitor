import os
import time
import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from datetime import date, datetime

# =========================
# 1) SETTINGS & STYLING
# =========================
st.set_page_config(page_title="Conflict Monitor", layout="wide")

THEME_BG = "#0b0e14"
THEME_PANEL = "#0f172a"
THEME_BORDER = "#1f2937"
THEME_PINK = "#f472b6"
THEME_TEXT = "#f8fafc"

st.markdown(
    f"""
    <style>
      .main {{ background-color: {THEME_BG}; color: {THEME_TEXT}; }}
      .block-container {{ padding-top: 1.2rem; padding-bottom: 2rem; }}

      h1, h2, h3, h4 {{
        color: {THEME_PINK} !important;
        font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
      }}

      section[data-testid="stSidebar"] > div {{
        background: linear-gradient(180deg, #0b0e14 0%, #0a0c11 100%);
        border-right: 1px solid {THEME_BORDER};
      }}

      div[data-testid="stMetric"] {{
        background: rgba(15, 23, 42, 0.75);
        border: 1px solid {THEME_BORDER};
        border-radius: 14px;
        padding: 16px 16px 10px 16px;
        box-shadow: 0 10px 30px rgba(0,0,0,0.25);
      }}
      div[data-testid="stMetricValue"] {{ color: {THEME_PINK} !important; }}
      div[data-testid="stMetricLabel"] {{ color: #cbd5e1 !important; }}

      .stDataFrame {{
        background: rgba(15, 23, 42, 0.65);
        border: 1px solid {THEME_BORDER};
        border-radius: 14px;
        overflow: hidden;
      }}

      hr {{
        border: none;
        border-top: 1px solid rgba(148, 163, 184, 0.15);
        margin: 0.75rem 0;
      }}
    </style>
    """,
    unsafe_allow_html=True,
)

# =========================
# 2) DB CONFIG
# =========================
DB_HOST = os.getenv("DB_HOST", "localhost")  # if running streamlit in docker, set DB_HOST=postgres
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "gdelt")
DB_USER = os.getenv("DB_USER", "flink_user")
DB_PASS = os.getenv("DB_PASS", "flink_pass")


def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        connect_timeout=5,
    )


def yyyymmdd_int(d: date) -> int:
    return int(d.strftime("%Y%m%d"))


def int_to_date(n: int) -> date:
    return datetime.strptime(str(int(n)), "%Y%m%d").date()


@st.cache_data(ttl=2)
def get_bounds():
    """
    Prefer aggregation table bounds. If empty (rare), fall back to gdelt_events.
    """
    conn = get_db_conn()
    try:
        df = pd.read_sql(
            "SELECT MIN(event_date) AS min_d, MAX(event_date) AS max_d FROM daily_event_volume_by_quadclass;",
            conn,
        )
        if not df.empty and df.loc[0, "min_d"] is not None:
            return int(df.loc[0, "min_d"]), int(df.loc[0, "max_d"])

        # fallback
        df2 = pd.read_sql(
            "SELECT MIN(event_date) AS min_d, MAX(event_date) AS max_d FROM gdelt_events;",
            conn,
        )
        if not df2.empty and df2.loc[0, "min_d"] is not None:
            return int(df2.loc[0, "min_d"]), int(df2.loc[0, "max_d"])

        return None, None
    finally:
        conn.close()


@st.cache_data(ttl=2)
def fetch_kpis(start_int: int, end_int: int):
    conn = get_db_conn()
    try:
        return pd.read_sql(
            """
            SELECT
              COALESCE(SUM(total_events),0)::BIGINT                                      AS total_events,
              COALESCE(SUM(total_articles),0)::BIGINT                                    AS total_articles,
              COALESCE(SUM(CASE WHEN quad_class IN (3,4) THEN total_events ELSE 0 END),0)::BIGINT AS conflict_events,
              AVG(avg_goldstein)                                                         AS avg_goldstein_unweighted
            FROM daily_event_volume_by_quadclass
            WHERE event_date BETWEEN %s AND %s;
            """,
            conn,
            params=(start_int, end_int),
        )
    finally:
        conn.close()


@st.cache_data(ttl=2)
def fetch_trend(start_int: int, end_int: int):
    conn = get_db_conn()
    try:
        return pd.read_sql(
            """
            SELECT
              to_date(event_date::text, 'YYYYMMDD') AS day,
              SUM(total_events)::BIGINT AS total_events,
              SUM(CASE WHEN quad_class IN (3,4) THEN total_events ELSE 0 END)::BIGINT AS conflict_events,
              AVG(avg_goldstein) AS avg_goldstein_unweighted
            FROM daily_event_volume_by_quadclass
            WHERE event_date BETWEEN %s AND %s
            GROUP BY 1
            ORDER BY 1;
            """,
            conn,
            params=(start_int, end_int),
        )
    finally:
        conn.close()


@st.cache_data(ttl=2)
def fetch_map(start_int: int, end_int: int):
    """
    Map intensity: use SUM(total_events) per ISO-3 actor code.
    """
    conn = get_db_conn()
    try:
        return pd.read_sql(
            """
            SELECT
              source_actor AS actor,
              SUM(total_events)::BIGINT AS total_events,
              AVG(avg_goldstein) AS avg_goldstein_unweighted
            FROM top_actors
            WHERE event_date BETWEEN %s AND %s
              AND source_actor IS NOT NULL
              AND length(source_actor) = 3
            GROUP BY 1
            ORDER BY total_events DESC;
            """,
            conn,
            params=(start_int, end_int),
        )
    finally:
        conn.close()


@st.cache_data(ttl=2)
def fetch_top_actors(start_int: int, end_int: int):
    conn = get_db_conn()
    try:
        return pd.read_sql(
            """
            SELECT
              source_actor AS actor,
              SUM(total_events)::BIGINT AS total_events,
              SUM(total_articles)::BIGINT AS total_articles,
              AVG(avg_goldstein) AS avg_goldstein_unweighted
            FROM top_actors
            WHERE event_date BETWEEN %s AND %s
              AND source_actor IS NOT NULL
            GROUP BY 1
            ORDER BY total_events DESC
            LIMIT 30;
            """,
            conn,
            params=(start_int, end_int),
        )
    finally:
        conn.close()


@st.cache_data(ttl=2)
def fetch_top_dyads(start_int: int, end_int: int):
    conn = get_db_conn()
    try:
        return pd.read_sql(
            """
            SELECT
              source_actor,
              target_actor,
              SUM(total_events)::BIGINT AS total_events,
              AVG(avg_goldstein) AS avg_goldstein_unweighted
            FROM dyad_interactions
            WHERE event_date BETWEEN %s AND %s
              AND source_actor IS NOT NULL
              AND target_actor IS NOT NULL
            GROUP BY 1,2
            ORDER BY total_events DESC
            LIMIT 60;
            """,
            conn,
            params=(start_int, end_int),
        )
    finally:
        conn.close()


@st.cache_data(ttl=2)
def fetch_cameo(start_int: int, end_int: int):
    conn = get_db_conn()
    try:
        return pd.read_sql(
            """
            SELECT
              cameo_code,
              SUM(total_events)::BIGINT AS total_events,
              SUM(total_articles)::BIGINT AS total_articles,
              AVG(avg_goldstein) AS avg_goldstein_unweighted
            FROM daily_cameo_metrics
            WHERE event_date BETWEEN %s AND %s
              AND cameo_code IS NOT NULL
              AND cameo_code <> ''
            GROUP BY 1
            ORDER BY total_events DESC
            LIMIT 25;
            """,
            conn,
            params=(start_int, end_int),
        )
    finally:
        conn.close()


@st.cache_data(ttl=2)
def fetch_quad_heat(start_int: int, end_int: int):
    conn = get_db_conn()
    try:
        return pd.read_sql(
            """
            SELECT
              to_date(event_date::text, 'YYYYMMDD') AS day,
              quad_class,
              SUM(total_events)::BIGINT AS total_events
            FROM daily_event_volume_by_quadclass
            WHERE event_date BETWEEN %s AND %s
            GROUP BY 1,2
            ORDER BY 1,2;
            """,
            conn,
            params=(start_int, end_int),
        )
    finally:
        conn.close()


# =========================
# 3) SIDEBAR CONTROLS
# =========================
min_int, max_int = get_bounds()

with st.sidebar:
    st.markdown("## Parameters")

    if min_int is None:
        st.error("No data found in your aggregation tables yet.")
        st.stop()

    min_d = int_to_date(min_int)
    max_d = int_to_date(max_int)

    # Default behavior: SHOW ALL DATA unless user enables custom range
    use_custom = st.checkbox("Custom date range", value=False)

    if use_custom:
        picked = st.date_input(
            "Date range",
            value=(min_d, max_d),
            min_value=min_d,
            max_value=date.today(),  # allow 'now' selection even if data isn't there yet
        )
        if isinstance(picked, (tuple, list)) and len(picked) == 2:
            start_d, end_d = picked
        else:
            start_d, end_d = min_d, max_d
    else:
        start_d, end_d = min_d, max_d

    if start_d > end_d:
        start_d, end_d = end_d, start_d

    start_int = yyyymmdd_int(start_d)
    end_int = yyyymmdd_int(end_d)

    st.markdown("---")
    live = st.toggle("Auto refresh", value=True)
    refresh_secs = st.slider("Refresh every (seconds)", 2, 30, 2, disabled=not live)

    if st.button("Refresh now", use_container_width=True):
        st.rerun()

    if st.button("Clear cache", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.caption(f"Data available in tables: **{min_d} → {max_d}**")
    if end_d > max_d:
        st.warning(f"Your tables currently stop at **{max_d}**. Newer dates will show empty until Flink writes them.")


# =========================
# 4) DASHBOARD
# =========================
st.title("Global Conflict Monitor")
st.caption("Powered by Flink → Postgres aggregation tables (minimal app-side logic).")

kpis = fetch_kpis(start_int, end_int)
trend = fetch_trend(start_int, end_int)
map_df = fetch_map(start_int, end_int)
actors_df = fetch_top_actors(start_int, end_int)
dyads_df = fetch_top_dyads(start_int, end_int)
cameo_df = fetch_cameo(start_int, end_int)
heat_df = fetch_quad_heat(start_int, end_int)

# KPIs
if not kpis.empty:
    total_events = int(kpis.loc[0, "total_events"] or 0)
    total_articles = int(kpis.loc[0, "total_articles"] or 0)
    conflict_events = int(kpis.loc[0, "conflict_events"] or 0)
    avg_gold = kpis.loc[0, "avg_goldstein_unweighted"]
    avg_gold = float(avg_gold) if avg_gold is not None else 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Events", f"{total_events:,}")
    c2.metric("Conflict Events (Q3+Q4)", f"{conflict_events:,}")
    c3.metric("Total Articles", f"{total_articles:,}")
    c4.metric("Avg Goldstein (unweighted)", f"{avg_gold:.3f}")

st.markdown("---")

# Row 1: Trend + Map
left, right = st.columns([1.2, 1])

with left:
    st.subheader("Trend Over Time")
    if trend.empty:
        st.info("No trend data for the selected range.")
    else:
        fig_trend = px.line(
            trend,
            x="day",
            y=["total_events", "conflict_events"],
            template="plotly_dark",
        )
        fig_trend.update_layout(
            height=360,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            legend_title_text="",
        )
        st.plotly_chart(fig_trend, use_container_width=True)

with right:
    st.subheader("Global Intensity Map")
    if map_df.empty:
        st.info("No ISO-3 actor codes in this range.")
    else:
        # Use intensity for color (legible), keep pink accents in styling
        fig_map = px.choropleth(
            map_df,
            locations="actor",
            locationmode="ISO-3",
            color="total_events",
            hover_data={
                "actor": True,
                "total_events": True,
                "avg_goldstein_unweighted": ":.3f",
            },
            color_continuous_scale="YlOrRd",
            template="plotly_dark",
        )
        fig_map.update_layout(
            height=360,
            margin=dict(l=0, r=0, t=0, b=0),
            paper_bgcolor="rgba(0,0,0,0)",
            geo=dict(
                bgcolor="rgba(0,0,0,0)",
                landcolor=THEME_PANEL,
                showland=True,
                showcountries=True,
                countrycolor="rgba(148,163,184,0.25)",
            ),
            coloraxis_colorbar=dict(title="Total Events"),
        )
        st.plotly_chart(fig_map, use_container_width=True)

st.markdown("---")

# Row 2: Top Actors + CAMEO
l2, r2 = st.columns([1, 1])

with l2:
    st.subheader("Top Actors")
    if actors_df.empty:
        st.info("No actor data for this range.")
    else:
        fig_actors = px.bar(
            actors_df.sort_values("total_events", ascending=True).tail(15),
            x="total_events",
            y="actor",
            orientation="h",
            color="avg_goldstein_unweighted",
            color_continuous_scale="RdPu",
            template="plotly_dark",
        )
        fig_actors.update_layout(
            height=380,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            coloraxis_colorbar=dict(title="Avg Goldstein"),
        )
        st.plotly_chart(fig_actors, use_container_width=True)

with r2:
    st.subheader("Top CAMEO Codes")
    if cameo_df.empty:
        st.info("No CAMEO data for this range.")
    else:
        fig_cameo = px.bar(
            cameo_df.sort_values("total_events", ascending=True).tail(15),
            x="total_events",
            y="cameo_code",
            orientation="h",
            color="total_events",
            color_continuous_scale="YlOrRd",
            template="plotly_dark",
        )
        fig_cameo.update_layout(
            height=380,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            coloraxis_colorbar=dict(title="Events"),
        )
        st.plotly_chart(fig_cameo, use_container_width=True)

st.markdown("---")

# Row 3: Heatmap + Dyads
l3, r3 = st.columns([1.2, 1])

with l3:
    st.subheader("Quad Class Heatmap")
    if heat_df.empty:
        st.info("No quad data for this range.")
    else:
        pivot = heat_df.pivot(index="quad_class", columns="day", values="total_events").fillna(0)
        fig_hm = px.imshow(
            pivot,
            aspect="auto",
            color_continuous_scale="Magma",
            template="plotly_dark",
        )
        fig_hm.update_layout(
            height=360,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            coloraxis_colorbar=dict(title="Events"),
        )
        st.plotly_chart(fig_hm, use_container_width=True)

with r3:
    st.subheader("Top Dyads")
    if dyads_df.empty:
        st.info("No dyad data for this range.")
    else:
        show = dyads_df.rename(
            columns={
                "source_actor": "source",
                "target_actor": "target",
                "total_events": "events",
                "avg_goldstein_unweighted": "avg_goldstein",
            }
        )
        st.dataframe(show, use_container_width=True, hide_index=True, height=360)

# =========================
# 5) AUTO REFRESH (your original style)
# =========================
if live:
    time.sleep(refresh_secs)
    st.rerun()