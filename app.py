import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from datetime import datetime, timedelta
import time

# --- 1. SETTINGS & STYLING ---
st.set_page_config(page_title="Conflict Monitor", layout="wide")

# Deep slate and rose theme
st.markdown("""
    <style>
    .main { background-color: #0b0e14; color: #f8fafc; }
    .stMetric { background: #1e293b; border-radius: 8px; padding: 20px; border: 1px solid #334155; }
    div[data-testid="stMetricValue"] { color: #f472b6; }
    h1, h2, h3 { color: #f472b6 !important; font-family: 'Inter', sans-serif; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. DB UTILITIES ---
def get_db_conn():
    return psycopg2.connect(
        host="localhost", 
        database="gdelt", 
        user="flink_user", 
        password="flink_pass"
    )

@st.cache_data(ttl=1)
def fetch_ivm_data(days_back):
    # GDELT uses YYYYMMDD integers. We calculate the cutoff in Python.
    # Adjusting base date to 1979-01-31 to match your current data state.
    cutoff_date = (datetime(1979, 1, 31) - timedelta(days=days_back)).strftime('%Y%m%d')
    date_int = int(cutoff_date)

    conn = get_db_conn()
    try:
        # Map and Actor intensity
        # Note: Using 'source_actor' and 'goldstein' per your table schema
        df_actors = pd.read_sql(f"""
            SELECT source_actor as actor, COUNT(*) as events, AVG(goldstein) as stability 
            FROM gdelt_events 
            WHERE event_date >= {date_int}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 50""", conn)
        
        # Recent activity stream
        df_feed = pd.read_sql(f"""
            SELECT event_date, source_actor, target_actor, goldstein 
            FROM gdelt_events 
            ORDER BY globaleventid DESC LIMIT 10""", conn)
            
        conn.close()
        return df_actors, df_feed
    except Exception as e:
        st.error(f"Database Error: {e}")
        return pd.DataFrame(), pd.DataFrame()

# --- 3. SIDEBAR CONTROLS ---
with st.sidebar:
    st.header("Parameters")
    lookback = st.slider("Window (Days)", 1, 30, 7)
    live = st.toggle("Active Refresh", True)

# --- 4. EXECUTION ---
df_map, df_feed = fetch_ivm_data(lookback)

st.title("Global Conflict Monitor")
st.caption("IVM Engine | 87.2M Record Load | Real-time Stream")

if not df_map.empty:
    # Key Indicators
    m1, m2, m3 = st.columns(3)
    m1.metric("Total Rows", "87.2M")
    m2.metric("Filtered Volume", f"{df_map['events'].sum():,}")
    m3.metric("System Stability", f"{df_map['stability'].mean():.2f}")

    st.markdown("---")

    # Primary Visuals
    left, right = st.columns([2, 1])
    
    with left:
        st.subheader("Geopolitical Heatmap")
        fig = px.choropleth(
            df_map, 
            locations="actor", 
            locationmode="ISO-3", 
            color="stability", 
            color_continuous_scale="RdPu_r", 
            template="plotly_dark"
        )
        fig.update_layout(
            margin=dict(l=0,r=0,t=0,b=0), 
            paper_bgcolor='rgba(0,0,0,0)', 
            geo=dict(bgcolor='rgba(0,0,0,0)', landcolor='#1e293b')
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with right:
        st.subheader("Top Signal Sources")
        fig_bar = px.bar(
            df_map.head(10), 
            x='events', 
            y='actor', 
            orientation='h', 
            template="plotly_dark",
            color='stability',
            color_continuous_scale="RdPu_r"
        )
        fig_bar.update_layout(
            margin=dict(l=0,r=0,t=0,b=0), 
            paper_bgcolor='rgba(0,0,0,0)', 
            plot_bgcolor='rgba(0,0,0,0)',
            showlegend=False
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    st.subheader("Recent Signal Feed")
    st.dataframe(df_feed, use_container_width=True, hide_index=True)

# --- 5. REFRESH LOOP ---
if live:
    time.sleep(2)
    st.rerun()