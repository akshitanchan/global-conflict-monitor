import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from datetime import date
import time

# ----------------------------
# 1) PAGE + ENHANCED THEME
# ----------------------------
st.set_page_config(page_title="Global Conflict Monitor", layout="wide")

st.markdown(
    """
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
      
      :root{
        --bg: #0a0e1a;
        --panel: rgba(17,24,39,0.7);
        --panel-solid: #111827;
        --border: rgba(99,102,241,0.1);
        --border-glow: rgba(139,92,246,0.15);
        --text: #f3f4f6;
        --muted: #9ca3af;
        --accent: #a78bfa;
        --accent2: #8b5cf6;
        --success: #34d399;
        --warning: #fbbf24;
        --danger: #f87171;
      }

      .main { 
        background: linear-gradient(135deg, #0a0e1a 0%, #1a1f3a 100%);
        color: var(--text); 
      }
      
      section[data-testid="stSidebar"] { 
        background: linear-gradient(180deg, #0f1419 0%, #1a1f2e 100%);
        border-right: 1px solid var(--border-glow);
        backdrop-filter: blur(10px);
      }
      
      h1,h2,h3,h4 { 
        color: var(--text) !important; 
        font-family: 'Inter', sans-serif;
        font-weight: 600;
        letter-spacing: -0.02em;
      }
      
      h1 {
        font-size: 1.75rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
      }
      
      h3 {
        font-size: 0.95rem;
        font-weight: 600;
        color: var(--muted) !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 1rem;
      }
      
      p,div,span,label { 
        font-family: 'Inter', sans-serif; 
      }

      /* Glassmorphism KPI Cards */
      .kpi {
        background: linear-gradient(135deg, rgba(17,24,39,0.8) 0%, rgba(30,41,59,0.6) 100%);
        border: 1px solid var(--border-glow);
        border-radius: 12px;
        padding: 20px 22px;
        box-shadow: 0 8px 32px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.05);
        backdrop-filter: blur(10px);
        height: 100%;
        transition: all 0.3s ease;
        position: relative;
        overflow: hidden;
      }
      
      .kpi::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 2px;
        background: linear-gradient(90deg, var(--accent) 0%, var(--accent2) 100%);
        opacity: 0.5;
      }
      
      .kpi:hover {
        transform: translateY(-2px);
        box-shadow: 0 12px 40px rgba(139,92,246,0.2), inset 0 1px 0 rgba(255,255,255,0.1);
        border-color: rgba(139,92,246,0.3);
      }
      
      .kpi .label { 
        color: var(--muted); 
        font-size: 0.75rem; 
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 8px;
      }
      
      .kpi .value { 
        color: var(--text); 
        font-size: 1.85rem; 
        font-weight: 700; 
        line-height: 1;
        margin-bottom: 6px;
      }
      
      .kpi .hint { 
        color: var(--muted); 
        font-size: 0.75rem; 
        opacity: 0.7;
      }

      /* Enhanced Panels */
      .panel {
        background: linear-gradient(135deg, rgba(17,24,39,0.6) 0%, rgba(30,41,59,0.4) 100%);
        border: 1px solid var(--border-glow);
        border-radius: 12px;
        padding: 20px;
        box-shadow: 0 8px 32px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.03);
        backdrop-filter: blur(10px);
        margin-bottom: 20px;
      }

      div[data-testid="stDataFrame"]{
        border-radius: 12px;
        overflow: hidden;
        border: 1px solid var(--border-glow);
        box-shadow: 0 4px 16px rgba(0,0,0,0.2);
      }

      .stPlotlyChart>div { 
        border-radius: 12px; 
        overflow: hidden;
      }

      .block-container { 
        padding-top: 1.5rem; 
        padding-bottom: 1.5rem; 
        max-width: 100%;
      }
      
      /* Sidebar styling */
      .css-1d391kg, .css-1cypcdb {
        background: transparent;
      }
      
      /* Status bar */
      .status-bar {
        background: rgba(17,24,39,0.5);
        border: 1px solid var(--border);
        border-radius: 8px;
        padding: 10px 16px;
        margin: 12px 0 20px 0;
        font-size: 0.85rem;
        color: var(--muted);
        display: flex;
        gap: 24px;
      }
      
      .status-item {
        display: flex;
        align-items: center;
        gap: 8px;
      }
      
      .status-label {
        color: var(--muted);
        font-weight: 500;
      }
      
      .status-value {
        color: var(--text);
        font-weight: 600;
      }
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
# 3) METADATA
# ----------------------------
meta = qdf("""
    SELECT
      MIN(event_date) AS min_event_date,
      MAX(event_date) AS max_event_date
    FROM daily_event_volume_by_quadclass;
""")

if meta.empty or pd.isna(meta.loc[0, "min_event_date"]) or pd.isna(meta.loc[0, "max_event_date"]):
    st.error("No data found in daily_event_volume_by_quadclass. Please verify Flink aggregations.")
    st.stop()

min_date_int = int(meta.loc[0, "min_event_date"])
max_date_int = int(meta.loc[0, "max_event_date"])
min_date = pd.to_datetime(str(min_date_int), format="%Y%m%d").date()
max_date = pd.to_datetime(str(max_date_int), format="%Y%m%d").date()

# ----------------------------
# 4) SIDEBAR CONTROLS
# ----------------------------
with st.sidebar:
    st.markdown("### Filters")
    use_range = st.toggle("Custom Date Range", value=False)
    if use_range:
        picked = st.date_input(
            "Select Period",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )
        if isinstance(picked, tuple) and len(picked) == 2:
            start_d, end_d = picked
        else:
            start_d, end_d = min_date, max_date
    else:
        start_d, end_d = min_date, max_date

    st.markdown("---")
    st.markdown("### Display Options")
    show_dual_axis = st.checkbox("Show Goldstein Trend", value=True)
    map_metric = st.radio("Map Metric", ["Goldstein Score", "Event Volume"], index=0)
    
    st.markdown("---")
    st.markdown("### System")
    live = st.toggle("Auto Refresh", value=True)
    refresh_s = st.slider("Refresh Interval (sec)", 2, 30, 5)

start_int = int_yyyymmdd(start_d)
end_int = int_yyyymmdd(end_d)

# ----------------------------
# 5) QUERIES
# ----------------------------
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
st.markdown("# Global Conflict Monitor")
st.markdown(
    f"""
    <div class="status-bar">
      <div class="status-item">
        <span class="status-label">Period:</span>
        <span class="status-value">{start_d.isoformat()} — {end_d.isoformat()}</span>
      </div>
      <div class="status-item">
        <span class="status-label">Last Updated:</span>
        <span class="status-value">{max_date.isoformat()}</span>
      </div>
    </div>
    """,
    unsafe_allow_html=True
)

# ----------------------------
# 7) KPI ROW
# ----------------------------
c1, c2, c3, c4 = st.columns(4)

with c1:
    st.markdown(
        f"""
        <div class="kpi">
          <div class="label">Total Events</div>
          <div class="value">{total_events:,}</div>
          <div class="hint">All recorded events</div>
        </div>
        """,
        unsafe_allow_html=True
    )

with c2:
    st.markdown(
        f"""
        <div class="kpi">
          <div class="label">Conflict Events</div>
          <div class="value">{conflict_events:,}</div>
          <div class="hint">Quad Class 3 & 4</div>
        </div>
        """,
        unsafe_allow_html=True
    )

with c3:
    conflict_rate = (conflict_events / total_events) * 100 if total_events else 0.0
    st.markdown(
        f"""
        <div class="kpi">
          <div class="label">Conflict Rate</div>
          <div class="value">{conflict_rate:.1f}%</div>
          <div class="hint">Percentage of total</div>
        </div>
        """,
        unsafe_allow_html=True
    )

with c4:
    st.markdown(
        f"""
        <div class="kpi">
          <div class="label">Avg Goldstein</div>
          <div class="value">{mean_goldstein:.2f}</div>
          <div class="hint">Mean sentiment score</div>
        </div>
        """,
        unsafe_allow_html=True
    )

st.markdown("<br>", unsafe_allow_html=True)

# ----------------------------
# 8) MAIN VISUALS
# ----------------------------
left, right = st.columns([1.4, 1])

# ---- Event Volume Trends
with left:
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown("### Event Volume Trends")

    if trend.empty:
        st.info("No trend data available for this range.")
    else:
        fig = go.Figure()
        
        # Total events area
        fig.add_trace(go.Scatter(
            x=trend["event_day"], 
            y=trend["total_events"],
            mode="lines",
            name="Total Events",
            fill='tozeroy',
            line=dict(color='rgba(167,139,250,0.8)', width=2),
            fillcolor='rgba(167,139,250,0.1)',
        ))
        
        # Conflict events line
        fig.add_trace(go.Scatter(
            x=trend["event_day"], 
            y=trend["conflict_events"],
            mode="lines",
            name="Conflict Events",
            line=dict(color='rgba(248,113,113,0.9)', width=3),
        ))
        
        if show_dual_axis and "mean_goldstein_unweighted" in trend.columns:
            # Goldstein on secondary axis
            fig.add_trace(go.Scatter(
                x=trend["event_day"],
                y=trend["mean_goldstein_unweighted"],
                mode="lines",
                name="Goldstein Score",
                line=dict(color='rgba(52,211,153,0.7)', width=2, dash='dot'),
                yaxis="y2"
            ))
        
        fig.update_layout(
            template="plotly_dark",
            height=380,
            margin=dict(l=20, r=20, t=20, b=20),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(17,24,39,0.3)",
            legend=dict(
                orientation="h", 
                yanchor="bottom", 
                y=1.02, 
                xanchor="right", 
                x=1,
                bgcolor="rgba(17,24,39,0.5)",
                bordercolor="rgba(167,139,250,0.3)",
                borderwidth=1
            ),
            font=dict(color="#f3f4f6", family="Inter", size=11),
            hovermode="x unified",
            yaxis=dict(
                title="Events",
                gridcolor="rgba(99,102,241,0.1)",
                zerolinecolor="rgba(99,102,241,0.2)"
            ),
            yaxis2=dict(
                title="Goldstein",
                overlaying="y",
                side="right",
                gridcolor="rgba(52,211,153,0.1)",
                zerolinecolor="rgba(52,211,153,0.2)"
            ) if show_dual_axis else None,
            xaxis=dict(
                gridcolor="rgba(99,102,241,0.1)",
                zerolinecolor="rgba(99,102,241,0.2)"
            )
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)

# ---- Geographic Distribution
with right:
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown("### Geographic Distribution")

    if actors.empty:
        st.info("No actor data available for this range.")
    else:
        if map_metric == "Goldstein Score":
            # Color by Goldstein (diverging: red=conflict, blue=cooperation)
            fig_map = px.choropleth(
                actors,
                locations="iso3",
                locationmode="ISO-3",
                color="mean_goldstein_unweighted",
                hover_name="iso3",
                hover_data={
                    "total_events": ":,",
                    "mean_goldstein_unweighted": ":.2f",
                    "iso3": False,
                },
                color_continuous_scale="RdBu_r",
                color_continuous_midpoint=0,
                range_color=[-10, 10],
                template="plotly_dark",
            )
            colorbar_title = "Goldstein"
        else:
            # Color by volume
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
                color_continuous_scale="Viridis",
                template="plotly_dark",
            )
            colorbar_title = "Events"
            
        fig_map.update_layout(
            height=380,
            margin=dict(l=0, r=0, t=0, b=0),
            paper_bgcolor="rgba(0,0,0,0)",
            geo=dict(
                bgcolor="rgba(0,0,0,0)", 
                landcolor="rgba(30,41,59,0.5)",
                coastlinecolor="rgba(99,102,241,0.3)",
                showcountries=True,
                countrycolor="rgba(99,102,241,0.2)",
                showframe=False,
                projection_type="natural earth"
            ),
            coloraxis_colorbar=dict(
                title=colorbar_title,
                bgcolor="rgba(17,24,39,0.7)",
                bordercolor="rgba(167,139,250,0.3)",
                borderwidth=1,
                tickfont=dict(color="#f3f4f6", size=10)
            ),
            font=dict(color="#f3f4f6", family="Inter", size=11),
        )
        st.plotly_chart(fig_map, use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)

# ----------------------------
# 9) SECOND ROW: BAR + AREA
# ----------------------------
c5, c6 = st.columns([1, 1.4])

with c5:
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown("### Top Actors by Volume")

    if actors.empty:
        st.info("No actor data available for this range.")
    else:
        topn = actors.head(15).sort_values("total_events", ascending=True)
        
        fig_bar = go.Figure()
        
        # Create color scale based on Goldstein
        colors = topn["mean_goldstein_unweighted"].values
        
        fig_bar.add_trace(go.Bar(
            x=topn["total_events"],
            y=topn["iso3"],
            orientation="h",
            marker=dict(
                color=colors,
                colorscale="RdBu_r",
                cmin=-10,
                cmax=10,
                colorbar=dict(
                    title="Goldstein",
                    bgcolor="rgba(17,24,39,0.7)",
                    bordercolor="rgba(167,139,250,0.3)",
                    borderwidth=1,
                    tickfont=dict(color="#f3f4f6", size=10)
                ),
                line=dict(color="rgba(167,139,250,0.3)", width=1)
            ),
            hovertemplate="<b>%{y}</b><br>Events: %{x:,}<br>Goldstein: %{marker.color:.2f}<extra></extra>"
        ))
        
        fig_bar.update_layout(
            template="plotly_dark",
            height=400,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(17,24,39,0.3)",
            font=dict(color="#f3f4f6", family="Inter", size=11),
            xaxis=dict(
                title="Total Events",
                gridcolor="rgba(99,102,241,0.1)",
                zerolinecolor="rgba(99,102,241,0.2)"
            ),
            yaxis=dict(
                title="",
                gridcolor="rgba(99,102,241,0.1)"
            )
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)

with c6:
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown("### Event Classification Over Time")

    if quad.empty:
        st.info("No quadclass data available for this range.")
    else:
        pivot = quad.pivot(index="event_day", columns="quad_class", values="total_events").fillna(0)
        for qc in [1, 2, 3, 4]:
            if qc not in pivot.columns:
                pivot[qc] = 0
        pivot = pivot[[1, 2, 3, 4]]
        
        # Better labels
        quad_labels = {
            1: "Verbal Cooperation",
            2: "Material Cooperation", 
            3: "Verbal Conflict",
            4: "Material Conflict"
        }
        
        fig_hm = go.Figure()
        
        for qc in [1, 2, 3, 4]:
            fig_hm.add_trace(go.Scatter(
                x=pivot.index,
                y=pivot[qc],
                mode='lines',
                name=quad_labels[qc],
                stackgroup='one',
                line=dict(width=0.5),
                fillcolor=['rgba(52,211,153,0.6)', 'rgba(96,165,250,0.6)', 
                          'rgba(251,191,36,0.6)', 'rgba(248,113,113,0.6)'][qc-1]
            ))
        
        fig_hm.update_layout(
            template="plotly_dark",
            height=400,
            margin=dict(l=10, r=10, t=10, b=40),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(17,24,39,0.3)",
            font=dict(color="#f3f4f6", family="Inter", size=11),
            hovermode="x unified",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.25,
                xanchor="center",
                x=0.5,
                bgcolor="rgba(17,24,39,0.5)",
                bordercolor="rgba(167,139,250,0.3)",
                borderwidth=1
            ),
            xaxis=dict(
                title="",
                gridcolor="rgba(99,102,241,0.1)",
                zerolinecolor="rgba(99,102,241,0.2)"
            ),
            yaxis=dict(
                title="Event Count",
                gridcolor="rgba(99,102,241,0.1)",
                zerolinecolor="rgba(99,102,241,0.2)"
            )
        )
        st.plotly_chart(fig_hm, use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)

# ----------------------------
# 10) TABLE: TOP DYADS
# ----------------------------
st.markdown('<div class="panel">', unsafe_allow_html=True)
st.markdown("### Dyadic Interactions")

if dyads.empty:
    st.info("No dyad data available for this range.")
else:
    dyads_show = dyads.copy()
    dyads_show["dyad"] = dyads_show["source_actor"].astype(str) + " → " + dyads_show["target_actor"].astype(str)
    dyads_show = dyads_show[["dyad", "total_events", "mean_goldstein_unweighted"]]
    dyads_show = dyads_show.rename(columns={
        "total_events": "events",
        "mean_goldstein_unweighted": "goldstein"
    })
    
    st.dataframe(
        dyads_show,
        use_container_width=True,
        hide_index=True,
        column_config={
            "dyad": st.column_config.TextColumn("Dyad", width="medium"),
            "events": st.column_config.NumberColumn("Events", format="%,d", width="small"),
            "goldstein": st.column_config.NumberColumn("Goldstein", format="%.2f", width="small"),
        },
        height=400
    )

st.markdown("</div>", unsafe_allow_html=True)

# ----------------------------
# 11) AUTO REFRESH
# ----------------------------
if live:
    time.sleep(refresh_s)
    st.rerun()