import os
import time
import select
from datetime import date, datetime
from typing import Optional, Dict, Any, Tuple

import pandas as pd
import psycopg2
import psycopg2.extensions
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go


# ----------------------------
# CONFIG
# ----------------------------
st.set_page_config(page_title="Global Conflict Monitor", layout="wide")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "gdelt")
DB_USER = os.getenv("DB_USER", "flink_user")
DB_PASS = os.getenv("DB_PASS", "flink_pass")

NOTIFY_CHANNEL = "view_updated"


# ----------------------------
# THEME / CSS (spacing + real Streamlit blocks)
# ----------------------------
st.markdown(
    """
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

      :root{
        --bg0: #070A12;
        --bg1: #0B1020;
        --panel: rgba(17,24,39,0.62);
        --panel2: rgba(30,27,75,0.28);
        --border: rgba(167,139,250,0.22);
        --border2: rgba(99,102,241,0.22);
        --text: #EAF0FF;
        --muted: #94A3B8;
        --accent2: #A78BFA;
        --good: #22C55E;
      }

      html, body, [class*="css"] { font-family: 'Inter', sans-serif !important; }

      .main {
        background: radial-gradient(1200px 600px at 20% 0%, rgba(167,139,250,0.16), transparent 50%),
                    radial-gradient(1200px 600px at 80% 20%, rgba(244,114,182,0.10), transparent 55%),
                    linear-gradient(180deg, var(--bg0), var(--bg1));
        color: var(--text);
      }

      section[data-testid="stSidebar"]{
        background: linear-gradient(180deg, rgba(10,14,26,0.97), rgba(30,27,75,0.55));
        border-right: 1px solid var(--border);
      }

      /* leave room under Streamlit top chrome (Stop/Deploy bar) */
      .block-container {
        padding-top: 3.2rem;
        padding-bottom: 1.6rem;
      }

      /* header row */
      .header-row{
        display:flex;
        align-items:flex-start;
        justify-content:space-between;
        gap: 14px;
        flex-wrap: wrap;
        margin-bottom: 12px;
      }
      .header-left{ min-width: 360px; }
      .header-right{
        display:flex;
        justify-content:flex-end;
        flex: 1;
        min-width: 360px;
      }

      .dash-title{
        font-weight: 900;
        font-size: 2.15rem;
        letter-spacing: -0.04em;
        background: linear-gradient(120deg, #FFFFFF, var(--accent2));
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
        line-height: 1.08;
      }
      .subtitle{
        color: var(--muted);
        margin-top: 6px;
        font-weight: 600;
        font-size: 0.95rem;
      }

      /* chips */
      .chiprow{
        display:flex;
        gap:10px;
        flex-wrap:wrap;
        justify-content:flex-end;
        align-items:flex-start;
      }
      .chip{
        display:inline-flex;
        align-items:center;
        gap:8px;
        background: rgba(167,139,250,0.10);
        border: 1px solid rgba(167,139,250,0.22);
        padding: 8px 12px;
        border-radius: 999px;
        color: var(--text);
        font-weight: 800;
        font-size: 0.85rem;
        white-space: nowrap;
      }
      .chip .k{ color: var(--muted); font-weight: 700; }
      .chip-live{
        background: rgba(34,197,94,0.12);
        border: 1px solid rgba(34,197,94,0.35);
        color: rgba(34,197,94,0.95);
      }
      .dot{
        width: 8px; height: 8px; border-radius: 50%;
        background: rgba(34,197,94,0.95);
        box-shadow: 0 0 0 0 rgba(34,197,94,0.35);
        animation: pulse 1.6s infinite;
      }
      @keyframes pulse {
        0% { box-shadow: 0 0 0 0 rgba(34,197,94,0.35); }
        70% { box-shadow: 0 0 0 10px rgba(34,197,94,0.0); }
        100% { box-shadow: 0 0 0 0 rgba(34,197,94,0.0); }
      }
      @media (max-width: 1100px){
        .header-right{ justify-content:flex-start; }
        .chiprow{ justify-content:flex-start; }
      }

      /* KPI cards */
      .kpi {
        background: linear-gradient(135deg, rgba(244,114,182,0.10), rgba(17,24,39,0.62));
        border: 1px solid var(--border2);
        border-radius: 16px;
        padding: 16px 16px;
        box-shadow: 0 10px 24px rgba(0,0,0,0.30);
        backdrop-filter: blur(14px);
        height: 100%;
      }
      .kpi .label {
        color: var(--muted);
        font-size: 0.72rem;
        font-weight: 900;
        text-transform: uppercase;
        letter-spacing: 0.10em;
      }
      .kpi .value {
        color: var(--text);
        font-size: 1.75rem;
        font-weight: 900;
        margin-top: 8px;
        margin-bottom: 4px;
        line-height: 1.0;
      }
      .kpi .hint { color: var(--muted); font-size: 0.86rem; font-weight: 600; }

      /* spacing helpers */
      .sp-10{ height: 10px; }
      .sp-18{ height: 18px; }
      .sp-26{ height: 26px; }

      /* Make Streamlit charts + tables look like panels (REAL styling) */
      div[data-testid="stPlotlyChart"] > div,
      div[data-testid="stDataFrame"] > div {
        background: linear-gradient(135deg, var(--panel2), var(--panel)) !important;
        border: 1px solid var(--border) !important;
        border-radius: 16px !important;
        box-shadow: 0 10px 26px rgba(0,0,0,0.33) !important;
        padding: 12px 12px !important;
        overflow: hidden !important;
      }

      h2, h3 { margin-top: 0.2rem !important; margin-bottom: 0.6rem !important; }
    </style>
    """,
    unsafe_allow_html=True
)


# ----------------------------
# DB HELPERS
# ----------------------------
def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )


def qdf(sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    conn = get_db_conn()
    try:
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()


def int_yyyymmdd(d: date) -> int:
    return int(d.strftime("%Y%m%d"))


# ----------------------------
# LISTEN/NOTIFY
# ----------------------------
def setup_listener():
    try:
        conn = get_db_conn()
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute(f"LISTEN {NOTIFY_CHANNEL};")
        return conn
    except Exception:
        return None


def check_notifications() -> bool:
    if st.session_state.get("listener_conn") is None:
        st.session_state.listener_conn = setup_listener()
        return False

    conn = st.session_state.listener_conn
    try:
        # non-blocking readiness check
        ready = select.select([conn], [], [], 0)
        if ready == ([conn], [], []):
            conn.poll()

        if conn.notifies:
            conn.notifies.clear()
            return True

        return False

    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        st.session_state.listener_conn = setup_listener()
        return False


if "listener_conn" not in st.session_state:
    st.session_state.listener_conn = setup_listener()

if "data_version" not in st.session_state:
    st.session_state.data_version = 0  # bump this to bust cache

if "last_refresh_time" not in st.session_state:
    st.session_state.last_refresh_time = datetime.now()

if "last_polled_max_date" not in st.session_state:
    st.session_state.last_polled_max_date = None

if "last_poll_check_ts" not in st.session_state:
    st.session_state.last_poll_check_ts = 0.0


# ----------------------------
# META: MIN/MAX DATES
# ----------------------------
meta = qdf("""
    SELECT MIN(event_date) AS min_event_date,
           MAX(event_date) AS max_event_date
    FROM daily_event_volume_by_quadclass;
""")

if meta.empty or pd.isna(meta.loc[0, "min_event_date"]) or pd.isna(meta.loc[0, "max_event_date"]):
    st.error("No data found in daily_event_volume_by_quadclass. Your Flink aggregation tables look empty.")
    st.stop()

min_date_int = int(meta.loc[0, "min_event_date"])
max_date_int = int(meta.loc[0, "max_event_date"])
min_date = pd.to_datetime(str(min_date_int), format="%Y%m%d").date()
max_date = pd.to_datetime(str(max_date_int), format="%Y%m%d").date()
is_live = (date.today() - max_date).days <= 7


# ----------------------------
# SIDEBAR
# ----------------------------
with st.sidebar:
    st.markdown("## Filters")

    show_all = st.toggle("All dates (default)", value=True)

    if show_all:
        start_d, end_d = min_date, max_date
    else:
        picked = st.date_input(
            "Date range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )
        if isinstance(picked, tuple) and len(picked) == 2:
            start_d, end_d = picked
        else:
            start_d, end_d = min_date, max_date

    st.markdown("---")
    st.markdown("## Map")
    map_metric = st.radio("Color by", ["Total Events", "Avg Goldstein"], index=0)

    top_n = st.slider("Top N", 10, 50, 20)

    st.markdown("---")
    st.markdown("## Live updates")

    live_refresh = st.toggle("Live mode", value=True)
    # this is the CHECK interval (how often Streamlit can notice NOTIFY)
    refresh_seconds = st.slider("Check interval (seconds)", 1, 30, 5)
    # fallback poll (max_date) is more expensive; keep slower
    poll_seconds = st.slider("Fallback poll (seconds)", 10, 120, 30)

start_int = int_yyyymmdd(start_d)
end_int = int_yyyymmdd(end_d)


# ----------------------------
# CHANGE DETECTION (NOTIFY + fallback poll)
# ----------------------------
got_notify = check_notifications()

polled_new = False
now_ts = time.time()

# only do the expensive MAX(event_date) poll every poll_seconds
if live_refresh and (now_ts - st.session_state.last_poll_check_ts) >= poll_seconds:
    st.session_state.last_poll_check_ts = now_ts
    try:
        max_now = qdf("SELECT MAX(event_date) AS m FROM daily_event_volume_by_quadclass;")
        cur_max = int(max_now.loc[0, "m"]) if not max_now.empty and pd.notna(max_now.loc[0, "m"]) else None
        if cur_max is not None and st.session_state.last_polled_max_date is not None:
            if cur_max != st.session_state.last_polled_max_date:
                polled_new = True
        st.session_state.last_polled_max_date = cur_max
    except Exception:
        pass

if got_notify or polled_new:
    st.session_state.data_version += 1
    st.session_state.last_refresh_time = datetime.now()


# ----------------------------
# CACHED DATA LOADERS (heavy queries)
# ----------------------------
@st.cache_data(show_spinner=False, ttl=3600)
def load_all(version: int, start_i: int, end_i: int, topn: int) -> Dict[str, pd.DataFrame]:
    # version is only here to bust cache when NOTIFY/poll says “new data”
    out: Dict[str, pd.DataFrame] = {}

    out["kpis"] = qdf(
        """
        SELECT
          SUM(total_events) AS total_events,
          SUM(CASE WHEN quad_class IN (3,4) THEN total_events ELSE 0 END) AS conflict_events,
          AVG(avg_goldstein) AS mean_goldstein
        FROM daily_event_volume_by_quadclass
        WHERE event_date BETWEEN %(s)s AND %(e)s;
        """,
        params={"s": start_i, "e": end_i},
    )

    out["trend"] = qdf(
        """
        SELECT
          to_date(event_date::text, 'YYYYMMDD') AS event_day,
          SUM(total_events) AS total_events,
          SUM(CASE WHEN quad_class IN (3,4) THEN total_events ELSE 0 END) AS conflict_events,
          AVG(avg_goldstein) AS mean_goldstein
        FROM daily_event_volume_by_quadclass
        WHERE event_date BETWEEN %(s)s AND %(e)s
        GROUP BY 1
        ORDER BY 1;
        """,
        params={"s": start_i, "e": end_i},
    )

    out["actors"] = qdf(
        """
        SELECT
          source_actor AS iso3,
          SUM(total_events) AS total_events,
          AVG(avg_goldstein) AS mean_goldstein
        FROM top_actors
        WHERE event_date BETWEEN %(s)s AND %(e)s
          AND source_actor IS NOT NULL
          AND char_length(source_actor) = 3
        GROUP BY 1
        HAVING SUM(total_events) > 0
        ORDER BY total_events DESC
        LIMIT 250;
        """,
        params={"s": start_i, "e": end_i},
    )

    out["dyads"] = qdf(
        """
        SELECT
          source_actor,
          target_actor,
          SUM(total_events) AS total_events,
          AVG(avg_goldstein) AS mean_goldstein
        FROM dyad_interactions
        WHERE event_date BETWEEN %(s)s AND %(e)s
          AND source_actor IS NOT NULL
          AND target_actor IS NOT NULL
        GROUP BY 1,2
        ORDER BY total_events DESC
        LIMIT %(n)s;
        """,
        params={"s": start_i, "e": end_i, "n": topn},
    )

    out["cameo"] = qdf(
        """
        SELECT
          cameo_code,
          SUM(total_events) AS total_events,
          AVG(avg_goldstein) AS mean_goldstein
        FROM daily_cameo_metrics
        WHERE event_date BETWEEN %(s)s AND %(e)s
          AND cameo_code IS NOT NULL
        GROUP BY 1
        ORDER BY total_events DESC
        LIMIT %(n)s;
        """,
        params={"s": start_i, "e": end_i, "n": topn},
    )

    out["quad_dist"] = qdf(
        """
        SELECT
          quad_class,
          SUM(total_events) AS total_events,
          AVG(avg_goldstein) AS avg_goldstein
        FROM daily_event_volume_by_quadclass
        WHERE event_date BETWEEN %(s)s AND %(e)s
        GROUP BY 1
        ORDER BY 1;
        """,
        params={"s": start_i, "e": end_i},
    )

    out["quad_time"] = qdf(
        """
        SELECT
          to_date(event_date::text, 'YYYYMMDD') AS event_day,
          quad_class,
          SUM(total_events) AS total_events
        FROM daily_event_volume_by_quadclass
        WHERE event_date BETWEEN %(s)s AND %(e)s
        GROUP BY 1,2
        ORDER BY 1,2;
        """,
        params={"s": start_i, "e": end_i},
    )

    return out


data = load_all(st.session_state.data_version, start_int, end_int, top_n)

kpis = data["kpis"]
trend = data["trend"]
actors = data["actors"]
dyads = data["dyads"]
cameo = data["cameo"]
quad_dist = data["quad_dist"]
quad_time = data["quad_time"]

total_events = int(kpis.loc[0, "total_events"] or 0)
conflict_events = int(kpis.loc[0, "conflict_events"] or 0)
mean_goldstein = float(kpis.loc[0, "mean_goldstein"] or 0.0)
conflict_rate = (conflict_events / total_events * 100.0) if total_events else 0.0


# ----------------------------
# HEADER
# ----------------------------
chips = [
    f'<div class="chip"><span class="k">Latest</span> {max_date.isoformat()}</div>',
    f'<div class="chip"><span class="k">Refresh</span> {st.session_state.last_refresh_time.strftime("%H:%M:%S")}</div>',
]
if is_live:
    chips.append('<div class="chip chip-live"><span class="dot"></span> LIVE</div>')

st.markdown(
    f"""
    <div class="header-row">
      <div class="header-left">
        <div class="dash-title">Global Conflict Monitor</div>
        <div class="subtitle">{start_d.isoformat()} → {end_d.isoformat()}</div>
      </div>
      <div class="header-right">
        <div class="chiprow">{''.join(chips)}</div>
      </div>
    </div>
    <div class="sp-18"></div>
    """,
    unsafe_allow_html=True
)


# ----------------------------
# KPI ROW
# ----------------------------
def kpi_card(label: str, value: str, hint: str):
    st.markdown(
        f"""
        <div class="kpi">
          <div class="label">{label}</div>
          <div class="value">{value}</div>
          <div class="hint">{hint}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

k1, k2, k3, k4 = st.columns(4)
with k1: kpi_card("Total events", f"{total_events:,}", "All quad classes")
with k2: kpi_card("Conflict events", f"{conflict_events:,}", "Quad 3 & 4")
with k3: kpi_card("Conflict rate", f"{conflict_rate:.1f}%", "Conflict / total")
with k4: kpi_card("Avg Goldstein", f"{mean_goldstein:.2f}", "Tone (unweighted)")

st.markdown('<div class="sp-26"></div>', unsafe_allow_html=True)


# ----------------------------
# TOP VISUALS
# ----------------------------
# ----------------------------
# TOP VISUALS  (MAP bigger, TRENDS smaller)
# ----------------------------
# Was: left(trends) right(map)
# Now: left(map) right(trends)

left, right = st.columns([1.6, 1.0])  # give map more width

with left:
    st.subheader("World Map")

    if actors.empty:
        st.info("No ISO-3 actor rows available in this period.")
    else:
        if map_metric == "Avg Goldstein":
            fig_map = px.choropleth(
                actors,
                locations="iso3",
                locationmode="ISO-3",
                color="mean_goldstein",
                hover_name="iso3",
                hover_data={"total_events": ":,", "mean_goldstein": ":.2f", "iso3": False},
                color_continuous_scale="RdBu_r",
                color_continuous_midpoint=0,
                template="plotly_dark",
            )
        else:
            fig_map = px.choropleth(
                actors,
                locations="iso3",
                locationmode="ISO-3",
                color="total_events",
                hover_name="iso3",
                hover_data={"total_events": ":,", "mean_goldstein": ":.2f", "iso3": False},
                color_continuous_scale="Viridis",
                template="plotly_dark",
            )

        fig_map.update_layout(
            height=350,  # bigger map
            margin=dict(l=0, r=0, t=0, b=0),
            paper_bgcolor="rgba(0,0,0,0)",
            geo=dict(
                bgcolor="rgba(0,0,0,0)",
                landcolor="rgba(30,41,59,0.55)",
                showcountries=True,
                countrycolor="rgba(148,163,184,0.22)",
                showframe=False,
                projection_type="natural earth",
            ),
        )
        st.plotly_chart(fig_map, use_container_width=True)

with right:
    st.subheader("Trends")

    if trend.empty:
        st.info("No data in this range.")
    else:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=trend["event_day"], y=trend["total_events"],
            mode="lines", name="Total",
            fill="tozeroy"
        ))
        fig.add_trace(go.Scatter(
            x=trend["event_day"], y=trend["conflict_events"],
            mode="lines", name="Conflict"
        ))
        fig.add_trace(go.Scatter(
            x=trend["event_day"], y=trend["mean_goldstein"],
            mode="lines", name="Goldstein",
            line=dict(dash="dot"),
            yaxis="y2"
        ))

        fig.update_layout(
            template="plotly_dark",
            height=350,  # smaller trends
            margin=dict(l=16, r=16, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            hovermode="x unified",
            legend=dict(orientation="h", y=1.12),
            yaxis=dict(title="Events"),
            yaxis2=dict(title="Goldstein", overlaying="y", side="right"),
        )
        st.plotly_chart(fig, use_container_width=True)


# ----------------------------
# LOWER CONTENT
# ----------------------------
st.markdown('<div class="sp-18"></div>', unsafe_allow_html=True)
tab1, tab2, tab3 = st.tabs(["Interactions", "QuadClass", "CAMEO"])

with tab1:
    a, b = st.columns([1.2, 1])
    with a:
        st.subheader("Dyad Heatmap")
        if dyads.empty:
            st.info("No dyad data available for this range.")
        else:
            top_actors_list = pd.concat([dyads["source_actor"], dyads["target_actor"]]).value_counts().head(12).index.tolist()
            hm = dyads[dyads["source_actor"].isin(top_actors_list) & dyads["target_actor"].isin(top_actors_list)].copy()
            if hm.empty:
                st.info("Not enough overlap for a heatmap. Try a broader range.")
            else:
                pivot = hm.pivot_table(index="source_actor", columns="target_actor", values="total_events", aggfunc="sum", fill_value=0)
                fig_hm = go.Figure(data=go.Heatmap(
                    z=pivot.values, x=pivot.columns, y=pivot.index,
                    colorscale="Plasma",
                    hovertemplate="From %{y} → %{x}<br>Events: %{z:,}<extra></extra>",
                    colorbar=dict(title="Events")
                ))
                fig_hm.update_layout(
                    template="plotly_dark", height=380,
                    margin=dict(l=16, r=16, t=10, b=10),
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig_hm, use_container_width=True)
    with b:
        st.subheader("Top Dyads")
        st.dataframe(dyads, use_container_width=True, hide_index=True)

with tab2:
    c1, c2 = st.columns([1, 1.4])

    with c1:
        st.subheader("QuadClass Distribution")
        if quad_dist.empty:
            st.info("No quadclass distribution for this range.")
        else:
            quad_labels = {1: "Q1 Verbal Coop", 2: "Q2 Material Coop", 3: "Q3 Verbal Conflict", 4: "Q4 Material Conflict"}
            qd = quad_dist.copy()
            qd["quad_label"] = qd["quad_class"].map(quad_labels).fillna(qd["quad_class"].astype(str))
            fig_qd = px.bar(
                qd, x="quad_label", y="total_events",
                template="plotly_dark",
                hover_data={"total_events": ":,", "avg_goldstein": ":.2f"},
            )
            fig_qd.update_layout(
                height=380,
                margin=dict(l=16, r=16, t=10, b=10),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                xaxis_title="", yaxis_title="Events",
            )
            st.plotly_chart(fig_qd, use_container_width=True)

    with c2:
        st.subheader("QuadClass Heatmap (Time × Class)")
        if quad_time.empty:
            st.info("No quadclass time series for this range.")
        else:
            pivot = quad_time.pivot(index="quad_class", columns="event_day", values="total_events").fillna(0)
            for qc in [1, 2, 3, 4]:
                if qc not in pivot.index:
                    pivot.loc[qc] = 0
            pivot = pivot.sort_index()
            quad_labels = {1: "Q1 Verbal Coop", 2: "Q2 Material Coop", 3: "Q3 Verbal Conflict", 4: "Q4 Material Conflict"}
            y_labels = [quad_labels.get(i, f"Q{i}") for i in pivot.index]
            fig_h = go.Figure(data=go.Heatmap(
                z=pivot.values, x=pivot.columns, y=y_labels,
                colorscale="Magma",
                hovertemplate="%{y}<br>%{x}<br>Events: %{z:,}<extra></extra>",
                colorbar=dict(title="Events"),
            ))
            fig_h.update_layout(
                template="plotly_dark", height=380,
                margin=dict(l=16, r=16, t=10, b=30),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(tickangle=-35), yaxis_title="",
            )
            st.plotly_chart(fig_h, use_container_width=True)

with tab3:
    st.subheader("Top CAMEO Codes")
    if cameo.empty:
        st.info("No CAMEO data available for this range.")
    else:
        fig_bar = px.bar(
            cameo.sort_values("total_events", ascending=True).tail(top_n),
            x="total_events", y="cameo_code", orientation="h",
            template="plotly_dark",
            hover_data={"total_events": ":,", "mean_goldstein": ":.2f"},
        )
        fig_bar.update_layout(
            height=420,
            margin=dict(l=16, r=16, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            xaxis_title="Events", yaxis_title="",
        )
        st.plotly_chart(fig_bar, use_container_width=True)


# ----------------------------
# LIVE LOOP (IMPORTANT)
# ----------------------------
# Streamlit cannot “receive” NOTIFY while idle.
# We rerun periodically to check LISTEN, but heavy queries are cached unless data_version changes.
if live_refresh:
    time.sleep(refresh_seconds)
    st.rerun()