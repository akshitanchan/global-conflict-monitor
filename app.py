import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from datetime import date
import time

# ----------------------------
# 1) PAGE + THEME
# ----------------------------
st.set_page_config(page_title="Conflict Monitor", layout="wide")

st.markdown(
    """
    <style>
      :root{
        --bg: #0b0e14;
        --panel: #121826;
        --panel2: #0f172a;
        --border: #22304a;
        --text: #e5e7eb;
        --muted: #94a3b8;
        --accent: #f472b6; /* pink */
        --accent2:#fb7185; /* rose */
      }

      .main { background-color: var(--bg); color: var(--text); }
      section[data-testid="stSidebar"] { background: #0a0f1a; border-right: 1px solid var(--border); }
      h1,h2,h3,h4 { color: var(--accent) !important; font-family: Inter, ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto; }
      p,div,span,label { font-family: Inter, ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto; }

      .kpi {
        background: linear-gradient(180deg, rgba(18,24,38,1) 0%, rgba(15,23,42,1) 100%);
        border: 1px solid var(--border);
        border-radius: 16px;
        padding: 16px 18px;
        box-shadow: 0 10px 30px rgba(0,0,0,0.25);
        height: 100%;
      }
      .kpi .label { color: var(--muted); font-size: 0.9rem; }
      .kpi .value { color: var(--text); font-size: 1.9rem; font-weight: 700; margin-top: 6px; }
      .kpi .hint  { color: var(--muted); font-size: 0.85rem; margin-top: 2px; }

      .panel {
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 16px;
        padding: 14px 14px 6px 14px;
        box-shadow: 0 10px 30px rgba(0,0,0,0.25);
      }

      div[data-testid="stDataFrame"]{
        border-radius: 14px;
        overflow: hidden;
        border: 1px solid var(--border);
      }

      /* Make plotly background match */
      .stPlotlyChart>div { border-radius: 16px; overflow: hidden; border: 1px solid var(--border); }

      /* tighten padding a bit */
      .block-container { padding-top: 1.2rem; padding-bottom: 1.2rem; }
    </style>
    """,
    unsafe_allow_html=True
)

# ----------------------------
# 2) DB
# ----------------------------
def get_db_conn():
    return psycopg2.connect(
        host="localhost",
        database="gdelt",
        user="flink_user",
        password="flink_pass",
    )

@st.cache_data(ttl=5)
def qdf(sql: str, params=None) -> pd.DataFrame:
    conn = get_db_conn()
    try:
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()

def int_yyyymmdd(d: date) -> int:
    return int(d.strftime("%Y%m%d"))

# ----------------------------
# 3) METADATA (min/max dates from existing aggregations)
# ----------------------------
meta = qdf("""
    SELECT
      MIN(event_date) AS min_event_date,
      MAX(event_date) AS max_event_date
    FROM daily_event_volume_by_quadclass;
""")

if meta.empty or pd.isna(meta.loc[0, "min_event_date"]) or pd.isna(meta.loc[0, "max_event_date"]):
    st.error("I can’t find dates in `daily_event_volume_by_quadclass`. Are the Flink aggregations running and writing to Postgres?")
    st.stop()

min_date_int = int(meta.loc[0, "min_event_date"])
max_date_int = int(meta.loc[0, "max_event_date"])

# Convert YYYYMMDD ints to Python dates
min_date = pd.to_datetime(str(min_date_int), format="%Y%m%d").date()
max_date = pd.to_datetime(str(max_date_int), format="%Y%m%d").date()

# ----------------------------
# 4) SIDEBAR CONTROLS
# ----------------------------
with st.sidebar:
    st.markdown("### Controls")
    use_range = st.toggle("Use custom date range", value=False, help="Off = show all data by default.")
    if use_range:
        picked = st.date_input(
            "Date range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )
        # Streamlit date_input returns either a single date or a tuple depending on selection
        if isinstance(picked, tuple) and len(picked) == 2:
            start_d, end_d = picked
        else:
            start_d, end_d = min_date, max_date
    else:
        start_d, end_d = min_date, max_date

    st.markdown("---")
    live = st.toggle("Auto refresh", value=True)
    refresh_s = st.slider("Refresh every (seconds)", 2, 30, 5)

start_int = int_yyyymmdd(start_d)
end_int = int_yyyymmdd(end_d)

# ----------------------------
# 5) QUERIES (ALL filtering happens in SQL)
# ----------------------------

# KPI + headline totals
kpis = qdf(
    """
    SELECT
      SUM(total_events) AS total_events,
      SUM(CASE WHEN quad_class IN (3,4) THEN total_events ELSE 0 END) AS conflict_events,
      AVG(avg_goldstein) AS mean_goldstein_unweighted
    FROM daily_event_volume_by_quadclass
    WHERE event_date BETWEEN %(s)s AND %(e)s;
    """,
    params={"s": start_int, "e": end_int},
)

total_events = int(kpis.loc[0, "total_events"] or 0)
conflict_events = int(kpis.loc[0, "conflict_events"] or 0)
mean_goldstein = float(kpis.loc[0, "mean_goldstein_unweighted"] or 0.0)

# Trend line (daily totals + conflict + mean goldstein)
trend = qdf(
    """
    SELECT
      to_date(event_date::text, 'YYYYMMDD') AS event_day,
      SUM(total_events) AS total_events,
      SUM(CASE WHEN quad_class IN (3,4) THEN total_events ELSE 0 END) AS conflict_events,
      AVG(avg_goldstein) AS mean_goldstein_unweighted
    FROM daily_event_volume_by_quadclass
    WHERE event_date BETWEEN %(s)s AND %(e)s
    GROUP BY 1
    ORDER BY 1;
    """,
    params={"s": start_int, "e": end_int},
)

# Map + bar: top actors by volume in range (color by avg_goldstein)
actors = qdf(
    """
    SELECT
      source_actor AS iso3,
      SUM(total_events) AS total_events,
      AVG(avg_goldstein) AS mean_goldstein_unweighted
    FROM top_actors
    WHERE event_date BETWEEN %(s)s AND %(e)s
      AND source_actor IS NOT NULL
      AND char_length(source_actor) = 3
    GROUP BY 1
    HAVING SUM(total_events) > 0
    ORDER BY total_events DESC
    LIMIT 200;
    """,
    params={"s": start_int, "e": end_int},
)

# Top dyads table (most interactions)
dyads = qdf(
    """
    SELECT
      source_actor,
      target_actor,
      SUM(total_events) AS total_events,
      AVG(avg_goldstein) AS mean_goldstein_unweighted
    FROM dyad_interactions
    WHERE event_date BETWEEN %(s)s AND %(e)s
      AND source_actor IS NOT NULL
      AND target_actor IS NOT NULL
    GROUP BY 1,2
    ORDER BY total_events DESC
    LIMIT 50;
    """,
    params={"s": start_int, "e": end_int},
)

# Heatmap data: date x quadclass (from existing agg table)
quad = qdf(
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
    params={"s": start_int, "e": end_int},
)

# ----------------------------
# 6) HEADER
# ----------------------------
st.markdown("## Global Conflict Monitor")
st.caption(
    f"Data window: **{start_d.isoformat()} → {end_d.isoformat()}**  |  "
    f"Latest available date in aggregates: **{max_date.isoformat()}**"
)

# ----------------------------
# 7) KPI ROW
# ----------------------------
c1, c2, c3, c4 = st.columns([1.1, 1.1, 1.1, 1.3])

with c1:
    st.markdown(
        f"""
        <div class="kpi">
          <div class="label">Total Events</div>
          <div class="value">{total_events:,}</div>
          <div class="hint">From daily aggregates</div>
        </div>
        """,
        unsafe_allow_html=True
    )

with c2:
    st.markdown(
        f"""
        <div class="kpi">
          <div class="label">Conflict Events (Quad 3+4)</div>
          <div class="value">{conflict_events:,}</div>
          <div class="hint">Verbal + material conflict</div>
        </div>
        """,
        unsafe_allow_html=True
    )

with c3:
    conflict_rate = (conflict_events / total_events) * 100 if total_events else 0.0
    st.markdown(
        f"""
        <div class="kpi">
          <div class="label">Conflict Share</div>
          <div class="value">{conflict_rate:.1f}%</div>
          <div class="hint">Conflict / total events</div>
        </div>
        """,
        unsafe_allow_html=True
    )

with c4:
    st.markdown(
        f"""
        <div class="kpi">
          <div class="label">Mean Goldstein (unweighted)</div>
          <div class="value">{mean_goldstein:.2f}</div>
          <div class="hint">Average of aggregate rows (not weighted)</div>
        </div>
        """,
        unsafe_allow_html=True
    )

st.markdown("")

# ----------------------------
# 8) MAIN VISUALS
# ----------------------------
left, right = st.columns([1.35, 1])

# ---- Trend panel
with left:
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown("### Trend: Total vs Conflict (Daily)")

    if trend.empty:
        st.info("No trend data for this range.")
    else:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=trend["event_day"], y=trend["total_events"],
            mode="lines", name="Total events"
        ))
        fig.add_trace(go.Scatter(
            x=trend["event_day"], y=trend["conflict_events"],
            mode="lines", name="Conflict events"
        ))
        fig.update_layout(
            template="plotly_dark",
            height=340,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            font=dict(color="#e5e7eb"),
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)

# ---- Map + top actors panel
with right:
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown("### Map: Volume (Color) + Goldstein (Hover)")

    if actors.empty:
        st.info("No actor data for this range.")
    else:
        # Color by total volume (more legible), hover shows goldstein.
        # This avoids “total events = bad” confusion by labeling it as volume.
        fig_map = px.choropleth(
            actors,
            locations="iso3",
            locationmode="ISO-3",
            color="total_events",
            hover_name="iso3",
            hover_data={
                "total_events": ":,",
                "mean_goldstein_unweighted": ":.2f",
                "iso3": False,
            },
            color_continuous_scale="Plasma",  # legible + still vibey
            template="plotly_dark",
        )
        fig_map.update_layout(
            height=340,
            margin=dict(l=0, r=0, t=0, b=0),
            paper_bgcolor="rgba(0,0,0,0)",
            geo=dict(bgcolor="rgba(0,0,0,0)", landcolor="#1e293b"),
            coloraxis_colorbar=dict(title="Event volume"),
            font=dict(color="#e5e7eb"),
        )
        st.plotly_chart(fig_map, use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)

# ----------------------------
# 9) SECOND ROW: BAR + HEATMAP
# ----------------------------
c5, c6 = st.columns([1, 1.35])

with c5:
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown("### Top Actors (by volume)")

    if actors.empty:
        st.info("No actor data for this range.")
    else:
        topn = actors.head(15).sort_values("total_events", ascending=True)
        fig_bar = px.bar(
            topn,
            x="total_events",
            y="iso3",
            orientation="h",
            template="plotly_dark",
            color="mean_goldstein_unweighted",
            color_continuous_scale="RdBu",  # conflict vs cooperation feel
            labels={"iso3": "Actor (ISO3)", "total_events": "Total events", "mean_goldstein_unweighted": "Mean Goldstein"},
        )
        fig_bar.update_layout(
            height=360,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            coloraxis_colorbar=dict(title="Goldstein"),
            font=dict(color="#e5e7eb"),
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)

with c6:
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown("### Heatmap: Event Type Intensity (QuadClass over time)")

    if quad.empty:
        st.info("No quadclass data for this range.")
    else:
        pivot = quad.pivot(index="event_day", columns="quad_class", values="total_events").fillna(0)
        # Ensure columns 1..4 exist
        for qc in [1, 2, 3, 4]:
            if qc not in pivot.columns:
                pivot[qc] = 0
        pivot = pivot[[1, 2, 3, 4]]

        fig_hm = px.imshow(
            pivot.T,  # quadclass rows
            aspect="auto",
            labels=dict(x="Date", y="QuadClass", color="Events"),
            color_continuous_scale="RdPu",  # pink-ish but still readable
            template="plotly_dark",
        )
        fig_hm.update_layout(
            height=360,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#e5e7eb"),
        )
        st.plotly_chart(fig_hm, use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)

# ----------------------------
# 10) TABLE: TOP DYADS
# ----------------------------
st.markdown("")
st.markdown('<div class="panel">', unsafe_allow_html=True)
st.markdown("### Top Dyads (Most interactions)")

if dyads.empty:
    st.info("No dyad data for this range.")
else:
    dyads_show = dyads.copy()
    dyads_show["dyad"] = dyads_show["source_actor"].astype(str) + " → " + dyads_show["target_actor"].astype(str)
    dyads_show = dyads_show[["dyad", "total_events", "mean_goldstein_unweighted"]]
    dyads_show = dyads_show.rename(columns={
        "total_events": "total_events",
        "mean_goldstein_unweighted": "mean_goldstein"
    })
    st.dataframe(
        dyads_show,
        use_container_width=True,
        hide_index=True,
        column_config={
            "dyad": st.column_config.TextColumn("Dyad"),
            "total_events": st.column_config.NumberColumn("Total events", format="%,d"),
            "mean_goldstein": st.column_config.NumberColumn("Mean Goldstein", format="%.2f"),
        },
    )

st.markdown("</div>", unsafe_allow_html=True)

# ----------------------------
# 11) AUTO REFRESH (simple + reliable)
# ----------------------------
# This behaves like your earlier version: it will keep rerunning on a timer when enabled.
if live:
    time.sleep(refresh_s)
    st.rerun()
    