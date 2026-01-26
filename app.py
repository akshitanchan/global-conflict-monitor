import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import subprocess

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="Global Conflict Monitor", 
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CUSTOM STYLING ---
st.markdown("""
    <style>
    .main { 
        background-color: #0b0e14; 
        color: #f8fafc; 
    }
    .stMetric { 
        background: linear-gradient(135deg, #1e293b 0%, #334155 100%); 
        border-radius: 12px; 
        padding: 20px; 
        border: 1px solid #475569;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
    }
    div[data-testid="stMetricValue"] { 
        color: #f472b6; 
        font-size: 2rem; 
        font-weight: 700;
    }
    div[data-testid="stMetricDelta"] { 
        color: #a78bfa; 
    }
    h1 { 
        color: #f472b6 !important; 
        font-family: 'Inter', sans-serif;
        text-align: center;
        font-size: 3rem;
        margin-bottom: 0.5rem;
    }
    h2, h3 { 
        color: #f472b6 !important; 
        font-family: 'Inter', sans-serif; 
    }
    .subtitle {
        text-align: center;
        color: #94a3b8;
        font-size: 1.1rem;
        margin-bottom: 2rem;
    }
    .stNumberInput>div>div>input { 
        background-color: #1e293b; 
        color: #f8fafc; 
    }
    </style>
    """, unsafe_allow_html=True)

# --- DATABASE CONNECTION ---
@st.cache_resource
def get_db_conn():
    """Create cached database connection"""
    return psycopg2.connect(
        host="localhost", 
        port=5432,
        database="gdelt", 
        user="flink_user", 
        password="flink_pass",
        connect_timeout=10
    )

conn = get_db_conn()

# --- DATA FETCHING (USING FLINK RESULT TABLES!) ---
@st.cache_data(ttl=2)
def fetch_data():
    """
    Fetch from Flink's pre-computed result tables.
    This is FAST because Flink maintains these incrementally!
    """
    try:
        # Query 1: Top actors (from Flink result table)
        df_actors = pd.read_sql("""
            SELECT 
                source_actor as actor,
                SUM(total_events) as events,
                AVG(avg_goldstein) as stability
            FROM top_actors
            GROUP BY source_actor
            ORDER BY SUM(total_events) DESC
            LIMIT 50
        """, conn)
        
        # Query 2: Conflict trends (from Flink result table)
        df_trends = pd.read_sql("""
            SELECT 
                event_date,
                quad_class,
                total_events
            FROM daily_event_volume_by_quadclass
            ORDER BY event_date DESC, quad_class
            LIMIT 200
        """, conn)
        
        # Query 3: Top interactions (from Flink result table)
        df_dyads = pd.read_sql("""
            SELECT 
                source_actor,
                target_actor,
                SUM(total_events) as interactions,
                AVG(avg_goldstein) as sentiment
            FROM dyad_interactions
            GROUP BY source_actor, target_actor
            ORDER BY SUM(total_events) DESC
            LIMIT 30
        """, conn)
        
        # Query 4: Event types (from Flink result table)
        df_cameo = pd.read_sql("""
            SELECT 
                cameo_code,
                SUM(total_events) as events
            FROM daily_cameo_metrics
            GROUP BY cameo_code
            ORDER BY SUM(total_events) DESC
            LIMIT 10
        """, conn)
        
        # Query 5: Recent events (from source - small query)
        df_recent = pd.read_sql("""
            SELECT 
                event_date, 
                source_actor, 
                target_actor, 
                goldstein, 
                cameo_code,
                num_events
            FROM gdelt_events
            ORDER BY globaleventid DESC
            LIMIT 15
        """, conn)
        
        # Query 6: System stats
        stats = pd.read_sql("""
            SELECT 
                (SELECT COUNT(*) FROM gdelt_events) as total_source_rows,
                (SELECT SUM(total_events) FROM top_actors) as total_events_processed,
                (SELECT COUNT(DISTINCT source_actor) FROM top_actors) as active_countries
        """, conn)
        
        return df_actors, df_trends, df_dyads, df_cameo, df_recent, stats
        
    except Exception as e:
        st.error(f"Database Error: {e}")
        import traceback
        st.code(traceback.format_exc())
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), 
                pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

# --- SIDEBAR CONTROLS ---
with st.sidebar:
    st.header("⚙️ Controls")
    
    # Time range selector (currently not used, but available)
    time_range = st.selectbox(
        "Time Window", 
        ["Real-time View", "Last 7 Days", "Last 30 Days", "All Time"]
    )
    
    # Auto-refresh toggle
    auto_refresh = st.checkbox("🔄 Live Updates (2s)", value=True)
    
    st.markdown("---")
    st.header("Data Simulation")
    st.caption("Test incremental processing speed")
    
    # Initialize session state for processing metrics
    if 'processing_time' not in st.session_state:
        st.session_state.processing_time = None
    if 'last_batch_size' not in st.session_state:
        st.session_state.last_batch_size = 0
    if 'last_operation' not in st.session_state:
        st.session_state.last_operation = "None"
    
    # INSERT
    st.subheader("➕ Insert Events")
    col1, col2 = st.columns([3, 1])
    with col1:
        insert_count = st.number_input(
            "Count", 
            min_value=100, 
            max_value=10000000000, 
            value=1000, 
            step=100, 
            key="ins_n",
            label_visibility="collapsed"
        )
    with col2:
        if st.button("Insert", use_container_width=True, type="primary", key="ins_btn"):
            # Measure end-to-end processing time
            start_time = time.time()
            
            # Get timestamp before insert
            before_ts = pd.read_sql("SELECT MAX(last_updated) FROM top_actors", conn).iloc[0, 0]
            
            with st.spinner(f"Processing {insert_count} events..."):
                # Run insert
                subprocess.run(
                    ["python3", "scripts/simulate-changes.py", "--insert", str(insert_count)],
                    check=True,
                    capture_output=True
                )
                
                # Poll until Flink updates timestamp (max 15 seconds)
                poll_start = time.time()
                max_wait = 15
                
                while (time.time() - poll_start) < max_wait:
                    after_ts = pd.read_sql("SELECT MAX(last_updated) FROM top_actors", conn).iloc[0, 0]
                    if after_ts != before_ts:
                        break
                    time.sleep(0.3)
                
                processing_time = time.time() - start_time
                
                # Store metrics
                st.session_state.processing_time = processing_time
                st.session_state.last_batch_size = insert_count
                st.session_state.last_operation = "INSERT"
            
            st.success(f"✅ Inserted {insert_count:,} events in {processing_time:.2f}s!")
            time.sleep(1)
            st.rerun()
    
    # UPDATE
    st.subheader("✏️ Update Events")
    col1, col2 = st.columns([3, 1])
    with col1:
        update_count = st.number_input(
            "Count", 
            min_value=10, 
            max_value=10000000000, 
            value=50, 
            step=10, 
            key="upd_n",
            label_visibility="collapsed"
        )
    with col2:
        if st.button("Update", use_container_width=True, key="upd_btn"):
            start_time = time.time()
            before_ts = pd.read_sql("SELECT MAX(last_updated) FROM top_actors", conn).iloc[0, 0]
            
            with st.spinner(f"Updating {update_count} events..."):
                subprocess.run(
                    ["python3", "scripts/simulate-changes.py", "--update", str(update_count)],
                    check=True,
                    capture_output=True
                )
                
                # Poll for update
                poll_start = time.time()
                while (time.time() - poll_start) < 15:
                    after_ts = pd.read_sql("SELECT MAX(last_updated) FROM top_actors", conn).iloc[0, 0]
                    if after_ts != before_ts:
                        break
                    time.sleep(0.3)
                
                processing_time = time.time() - start_time
                st.session_state.processing_time = processing_time
                st.session_state.last_batch_size = update_count
                st.session_state.last_operation = "UPDATE"
            
            st.success(f"✅ Updated {update_count:,} events in {processing_time:.2f}s!")
            time.sleep(1)
            st.rerun()
    
    # DELETE
    st.subheader("🗑️ Delete Events")
    col1, col2 = st.columns([3, 1])
    with col1:
        delete_count = st.number_input(
            "Count", 
            min_value=10, 
            max_value=10000000000, 
            value=20, 
            step=10, 
            key="del_n",
            label_visibility="collapsed"
        )
    with col2:
        if st.button("Delete", use_container_width=True, key="del_btn"):
            start_time = time.time()
            before_ts = pd.read_sql("SELECT MAX(last_updated) FROM top_actors", conn).iloc[0, 0]
            
            with st.spinner(f"Deleting {delete_count} events..."):
                subprocess.run(
                    ["python3", "scripts/simulate-changes.py", "--delete", str(delete_count)],
                    check=True,
                    capture_output=True
                )
                
                # Poll for update
                poll_start = time.time()
                while (time.time() - poll_start) < 15:
                    after_ts = pd.read_sql("SELECT MAX(last_updated) FROM top_actors", conn).iloc[0, 0]
                    if after_ts != before_ts:
                        break
                    time.sleep(0.3)
                
                processing_time = time.time() - start_time
                st.session_state.processing_time = processing_time
                st.session_state.last_batch_size = delete_count
                st.session_state.last_operation = "DELETE"
            
            st.warning(f"⚠️ Deleted {delete_count:,} events in {processing_time:.2f}s!")
            time.sleep(1)
            st.rerun()
    
    st.markdown("---")
    
    # Performance info box
    if st.session_state.processing_time:
        st.info(f"""
        **Last Operation:** {st.session_state.last_operation}  
        **Batch Size:** {st.session_state.last_batch_size:,} events  
        **Processing Time:** {st.session_state.processing_time:.2f}s  
        **Throughput:** {st.session_state.last_batch_size / st.session_state.processing_time:.0f} events/sec
        """)
    
    st.markdown("---")
    st.caption("🔧 Apache Flink CDC + Debezium")

# --- FETCH DATA ---
df_actors, df_trends, df_dyads, df_cameo, df_recent, stats = fetch_data()

# --- HEADER ---
st.markdown('<h1>🌍 Global Conflict Monitor</h1>', unsafe_allow_html=True)
st.markdown(f'<p class="subtitle">Real-time Incremental Analytics Powered by Group 21</p>', unsafe_allow_html=True)

# --- METRICS ROW ---

if not df_actors.empty:
    col1, col2, col3, col4 = st.columns(4)
    
    # Get source row count
    source_rows = pd.read_sql("SELECT COUNT(*) FROM gdelt_events", conn).iloc[0, 0]
    
    with col1:
        # Show ROWS (actual database rows)
        if 'prev_rows' not in st.session_state:
            st.session_state.prev_rows = source_rows
        
        if source_rows > st.session_state.prev_rows:
            # Animate
            placeholder = st.empty()
            delta = source_rows - st.session_state.prev_rows
            
            for i in range(11):
                interp = st.session_state.prev_rows + int(delta * i / 10)
                with placeholder:
                    st.metric(
                        "📝 Total Rows", 
                        f"{interp:,}",
                        delta=f"+{delta:,}" if i == 10 else None
                    )
                time.sleep(0.03)
            
            st.session_state.prev_rows = source_rows
        else:
            st.metric("📝 Total Rows", f"{source_rows:,}")
    
    with col2:
        # Show EVENTS (sum of num_events field)
        total_events = int(df_actors['events'].sum())
        st.metric(
            "📊 Total Events", 
            f"{total_events:,}",
            help="Sum of num_events field from aggregated data"
        )
    
    with col3:
        countries = len(df_actors)
        avg_stab = df_actors['stability'].mean()
        stability_label = "Cooperation" if avg_stab > 0 else "Conflict"
        st.metric(
            "📈 Avg Stability", 
            f"{avg_stab:.2f}",
            delta=stability_label
        )
    
    with col4:
        if st.session_state.processing_time is not None:
            proc_time = st.session_state.processing_time
            rows_inserted = st.session_state.last_batch_size
            
            # Calculate ROWS per second (not events!)
            rows_per_sec = int(rows_inserted / proc_time) if proc_time > 0 else 0
            
            st.metric(
                "⚡ Processing Speed", 
                f"{proc_time:.2f}s",
                delta=f"{rows_per_sec:,} rows/sec",
                help=f"Last batch: {rows_inserted:,} rows in {proc_time:.2f}s"
            )
        else:
            st.metric(
                "⚡ Processing Speed", 
                "Ready",
                delta="Insert to measure"
            )

# if not df_actors.empty:
#     col1, col2, col3, col4 = st.columns(4)
    
#     with col1:
#         total = int(df_actors['events'].sum())
        
#         # Animation on value change
#         if 'prev_total' not in st.session_state:
#             st.session_state.prev_total = total
        
#         if total > st.session_state.prev_total:
#             # Animate the counter
#             placeholder = st.empty()
#             delta = total - st.session_state.prev_total
            
#             for i in range(11):
#                 interpolated = st.session_state.prev_total + int(delta * i / 10)
#                 with placeholder:
#                     st.metric(
#                         "📊 Total Events", 
#                         f"{interpolated:,}",
#                         delta=f"+{delta:,}" if i == 10 else None
#                     )
#                 time.sleep(0.03)
            
#             st.session_state.prev_total = total
#         else:
#             st.metric("📊 Total Events", f"{total:,}")
    
#     with col2:
#         countries = len(df_actors)
#         st.metric("🌍 Active Countries", f"{countries}")
    
#     with col3:
#         avg_stab = df_actors['stability'].mean()
#         stability_label = "Cooperation" if avg_stab > 0 else "Conflict"
#         st.metric(
#             "📈 Avg Stability", 
#             f"{avg_stab:.2f}",
#             delta=stability_label
#         )
    
#     with col4:
#         if st.session_state.processing_time is not None:
#             proc_time = st.session_state.processing_time
#             batch_size = st.session_state.last_batch_size
#             throughput = int(batch_size / proc_time) if proc_time > 0 else 0
            
#             st.metric(
#                 "⚡ Processing Speed", 
#                 f"{proc_time:.2f}s",
#                 delta=f"{throughput:,} events/sec"
#             )
#         else:
#             st.metric(
#                 "⚡ Processing Speed", 
#                 "Ready",
#                 delta="Insert to measure"
#             )
    
#     st.markdown("---")
    
    # --- MAIN VISUALIZATIONS ---
    
    # Row 1: Map + Bar Chart
    col_left, col_right = st.columns([2, 1])
    
    with col_left:
        st.subheader("🗺️ Global Conflict Intensity Heatmap")
        
        fig_map = px.choropleth(
            df_actors,
            locations="actor",
            locationmode="ISO-3",
            color="stability",
            hover_name="actor",
            hover_data={"events": ":,", "stability": ":.2f"},
            color_continuous_scale="RdYlGn",
            color_continuous_midpoint=0,
            range_color=[-10, 10],
            template="plotly_dark",
            labels={"stability": "Goldstein Score", "actor": "Country"}
        )
        fig_map.update_geos(
            projection_type="natural earth",
            showcoastlines=True,
            coastlinecolor="white",
            showland=True,
            landcolor="#1e293b",
            bgcolor='rgba(0,0,0,0)'
        )
        fig_map.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            paper_bgcolor='rgba(0,0,0,0)',
            height=500
        )
        st.plotly_chart(fig_map, use_container_width=True)
    
    with col_right:
        st.subheader("Top 10 Active Countries")
        
        df_top10 = df_actors.head(10)
        fig_bar = px.bar(
            df_top10,
            x='events',
            y='actor',
            orientation='h',
            template="plotly_dark",
            color='stability',
            color_continuous_scale="RdPu_r",
            labels={'events': 'Total Events', 'actor': 'Country'}
        )
        fig_bar.update_layout(
            margin=dict(l=0, r=0, t=20, b=0),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            showlegend=False,
            height=500,
            yaxis={'categoryorder': 'total ascending'}
        )
        st.plotly_chart(fig_bar, use_container_width=True)
    
    st.markdown("---")
    
    # Row 2: Time Series Trends
    if not df_trends.empty:
        st.subheader("📈 Conflict vs Cooperation Trends Over Time")
        
        # Convert date and pivot
        df_trends['date'] = pd.to_datetime(df_trends['event_date'], format='%Y%m%d')
        df_pivot = df_trends.pivot(index='date', columns='quad_class', values='total_events').fillna(0)
        
        # Ensure all 4 quad classes exist
        for i in range(1, 5):
            if i not in df_pivot.columns:
                df_pivot[i] = 0
        
        df_pivot = df_pivot[[1, 2, 3, 4]]
        df_pivot.columns = ['Verbal Coop', 'Material Coop', 'Verbal Conflict', 'Material Conflict']
        
        fig_area = px.area(
            df_pivot,
            x=df_pivot.index,
            y=df_pivot.columns,
            template="plotly_dark",
            color_discrete_map={
                'Verbal Coop': '#4ade80',
                'Material Coop': '#22c55e',
                'Verbal Conflict': '#fb923c',
                'Material Conflict': '#dc2626'
            },
            labels={'value': 'Events', 'variable': 'Event Type'}
        )
        fig_area.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            xaxis_title="Date",
            yaxis_title="Number of Events",
            hovermode='x unified',
            height=400,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        st.plotly_chart(fig_area, use_container_width=True)
    
    st.markdown("---")
    
    # Row 3: Interaction Heatmap + Event Types
    col_left, col_right = st.columns([2, 1])
    
    with col_left:
        if not df_dyads.empty:
            st.subheader("🔗 Country Interaction Matrix")
            
            # Get top 12 countries for cleaner visualization
            top_countries = df_actors.head(12)['actor'].tolist()
            df_dyads_filtered = df_dyads[
                df_dyads['source_actor'].isin(top_countries) & 
                df_dyads['target_actor'].isin(top_countries)
            ]
            
            if not df_dyads_filtered.empty:
                # Create pivot table for heatmap
                df_matrix = df_dyads_filtered.pivot_table(
                    index='source_actor',
                    columns='target_actor',
                    values='interactions',
                    fill_value=0
                )
                
                fig_heat = px.imshow(
                    df_matrix,
                    labels=dict(x="Target Country", y="Source Country", color="Interactions"),
                    color_continuous_scale="Reds",
                    template="plotly_dark",
                    aspect="auto"
                )
                fig_heat.update_layout(
                    margin=dict(l=0, r=0, t=0, b=0),
                    paper_bgcolor='rgba(0,0,0,0)',
                    height=400
                )
                st.plotly_chart(fig_heat, use_container_width=True)
    
    with col_right:
        if not df_cameo.empty:
            st.subheader("🎯 Top Event Types")
            
            # Map CAMEO codes to descriptions
            cameo_map = {
                '010': 'Make statement',
                '020': 'Appeal',
                '043': 'Consult',
                '050': 'Cooperate',
                '112': 'Accuse',
                '120': 'Reject',
                '130': 'Threaten',
                '140': 'Protest',
                '172': 'Impose embargo',
                '173': 'Coerce',
                '190': 'Military force',
                '193': 'Fight',
                '0841': 'Express intent',
                '084': 'Provide aid'
            }
            
            df_cameo['description'] = df_cameo['cameo_code'].map(
                lambda x: cameo_map.get(x, f"Code {x}")
            )
            
            fig_cameo = px.bar(
                df_cameo.head(10),
                x='events',
                y='description',
                orientation='h',
                template="plotly_dark",
                color='events',
                color_continuous_scale="Purples"
            )
            fig_cameo.update_layout(
                margin=dict(l=0, r=0, t=0, b=0),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                showlegend=False,
                height=400,
                yaxis={'categoryorder': 'total ascending'},
                xaxis_title="Events",
                yaxis_title=""
            )
            st.plotly_chart(fig_cameo, use_container_width=True)
    
    st.markdown("---")
    
    # Row 4: Recent Activity Feed
    st.subheader("📡 Real-time Event Stream")
    
    if not df_recent.empty:
        # Format for display
        df_recent['date'] = pd.to_datetime(df_recent['event_date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
        
        # Map CAMEO for recent events
        cameo_map_short = {
            '010': 'Statement', '020': 'Appeal', '043': 'Consult', '050': 'Cooperate',
            '112': 'Accuse', '120': 'Reject', '130': 'Threaten', '140': 'Protest',
            '172': 'Embargo', '173': 'Coerce', '190': 'Military', '193': 'Fight',
            '0841': 'Intent', '084': 'Aid'
        }
        df_recent['event_type'] = df_recent['cameo_code'].map(
            lambda x: cameo_map_short.get(x, x)
        )
        
        df_display = df_recent[['date', 'source_actor', 'target_actor', 'event_type', 'goldstein', 'num_events']]
        df_display.columns = ['Date', 'Source', 'Target', 'Event', 'Goldstein', 'Count']
        
        st.dataframe(
            df_display,
            use_container_width=True,
            hide_index=True,
            height=300
        )
    
    # --- FOOTER INFO ---
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if not stats.empty:
            source_rows = stats['total_source_rows'].iloc[0]
            st.caption(f"💾 **Source Rows:** {source_rows:,}")
    
    with col2:
        st.caption("🚀 **Query Time:** <0.1s (pre-computed)")
    
    with col3:
        st.caption("⚙️ **Tech:** PostgreSQL + Flink CDC + Streamlit")

else:
    # --- NO DATA STATE ---
    st.warning("⚠️ No data available in result tables.")
    st.info("""
    **Troubleshooting:**
    1. Check Flink jobs are running: `curl -s http://localhost:8081/jobs/overview`
    2. Verify data loaded: `docker exec gdelt-postgres psql -U flink_user -d gdelt -c "SELECT COUNT(*) FROM gdelt_events;"`
    3. Check result tables: `docker exec gdelt-postgres psql -U flink_user -d gdelt -c "SELECT COUNT(*) FROM top_actors;"`
    4. Restart pipeline: `./scripts/start-flink-aggregations.sh`
    """)

# --- AUTO-REFRESH ---
if auto_refresh:
    time.sleep(2)
    st.rerun()


# import streamlit as st
# import pandas as pd
# import psycopg2
# import plotly.express as px
# import plotly.graph_objects as go
# from datetime import datetime, timedelta
# import time

# # --- PAGE CONFIG ---
# st.set_page_config(page_title="Global Conflict Monitor", layout="wide")

# # --- STYLING ---
# st.markdown("""
#     <style>
#     .main { background-color: #0b0e14; color: #f8fafc; }
#     .stMetric { 
#         background: linear-gradient(135deg, #1e293b 0%, #334155 100%); 
#         border-radius: 12px; 
#         padding: 20px; 
#         border: 1px solid #475569;
#     }
#     div[data-testid="stMetricValue"] { color: #f472b6; font-size: 2rem; }
#     h1, h2, h3 { color: #f472b6 !important; font-family: 'Inter', sans-serif; }
#     </style>
#     """, unsafe_allow_html=True)

# # --- DATABASE CONNECTION ---
# @st.cache_resource
# def get_db_conn():
#     return psycopg2.connect(
#         host="localhost", 
#         database="gdelt", 
#         user="flink_user", 
#         password="flink_pass"
#     )

# conn = get_db_conn()

# # --- DATA FETCHING (USING FLINK RESULT TABLES!) ---

# @st.cache_data(ttl=2)
# def fetch_data(days_back):
#     """Simplified - no complex date filtering"""
    
#     conn = None
#     try:
#         conn = psycopg2.connect(
#             host="localhost", 
#             database="gdelt", 
#             user="flink_user", 
#             password="flink_pass"
#         )
        
#         # Query 1: Top actors (no WHERE clause)
#         df_actors = pd.read_sql("""
#             SELECT 
#                 source_actor as actor,
#                 SUM(total_events) as events,
#                 AVG(avg_goldstein) as stability
#             FROM top_actors
#             GROUP BY source_actor
#             ORDER BY SUM(total_events) DESC
#             LIMIT 50
#         """, conn)
        
#         # Query 2: Trends (no WHERE clause)
#         df_trends = pd.read_sql("""
#             SELECT event_date, quad_class, total_events
#             FROM daily_event_volume_by_quadclass
#             ORDER BY event_date, quad_class
#         """, conn)
        
#         # Query 3: Dyads (no WHERE clause)
#         df_dyads = pd.read_sql("""
#             SELECT source_actor, target_actor, SUM(total_events) as interactions
#             FROM dyad_interactions
#             GROUP BY source_actor, target_actor
#             ORDER BY SUM(total_events) DESC
#             LIMIT 20
#         """, conn)
        
#         # Query 4: Recent
#         df_recent = pd.read_sql("""
#             SELECT event_date, source_actor, target_actor, goldstein, cameo_code
#             FROM gdelt_events
#             ORDER BY globaleventid DESC
#             LIMIT 10
#         """, conn)
        
#         # Query 5: Latency (in PostgreSQL)
#         latency_data = pd.read_sql("""
#             SELECT 
#                 EXTRACT(EPOCH FROM (NOW() - MAX(last_updated))) as latency_seconds,
#                 MAX(last_updated) as last_update
#             FROM top_actors
#         """, conn)
        
#         latency = latency_data['latency_seconds'].iloc[0] if not latency_data.empty else 0
#         last_update = latency_data['last_update'].iloc[0] if not latency_data.empty else datetime.now()
        
#         conn.close()
#         return df_actors, df_trends, df_dyads, df_recent, latency, last_update
        
#     except Exception as e:
#         if conn:
#             conn.close()
#         st.error(f"Database Error: {e}")
#         import traceback
#         st.code(traceback.format_exc())
#         return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), 0, datetime.now()

# # --- SIDEBAR ---
# with st.sidebar:
#     st.header("⚙️ Controls")
    
#     time_range = st.selectbox("Time Window", ["Last 7 Days", "Last 30 Days", "All Time"])
#     days_map = {"Last 7 Days": 7, "Last 30 Days": 30, "All Time": 9999}
#     lookback = days_map[time_range]
    
#     auto_refresh = st.checkbox("🔄 Live Updates", value=True)
    
#     st.markdown("---")
#     st.header("🎮 Simulation")
    
#     if st.button("➕ Insert 1000 Events", use_container_width=True):
#         import subprocess
#         subprocess.run(["python3", "scripts/simulate-changes.py", "--insert", "1000"])
#         st.success("Inserted 1000 events!")
#         time.sleep(2)
#         st.rerun()

# # --- FETCH DATA ---
# df_actors, df_trends, df_dyads, df_recent, latency, last_update = fetch_data(lookback)

# # --- HEADER ---
# st.title("🌍 Global Conflict Monitor")
# st.caption(f"Real-time Incremental Analytics | {time_range}")

# # --- METRICS ---
# if not df_actors.empty:
#     col1, col2, col3, col4 = st.columns(4)
    
#     with col1:
#         total = df_actors['events'].sum()
#         st.metric("📊 Total Events", f"{total:,.0f}")
    
#     with col2:
#         countries = len(df_actors)
#         st.metric("🌍 Active Countries", f"{countries}")
    
#     with col3:
#         avg_stab = df_actors['stability'].mean()
#         st.metric("📈 Avg Stability", f"{avg_stab:.2f}")
    
#     with col4:
#         if latency is not None and latency < 3600:  # Less than 1 hour
#             st.metric("⚡ Update Latency", f"{latency:.1f}s", delta="Incremental!")
#         else:
#             st.metric("⚡ Update Latency", "N/A")
    
#     st.markdown("---")
    
#     # --- VISUALIZATIONS ---
#     col_left, col_right = st.columns([2, 1])
    
#     with col_left:
#         st.subheader("🗺️ Geopolitical Heatmap")
#         fig_map = px.choropleth(
#             df_actors,
#             locations="actor",
#             locationmode="ISO-3",
#             color="stability",
#             color_continuous_scale="RdYlGn",
#             color_continuous_midpoint=0,
#             range_color=[-10, 10],
#             template="plotly_dark",
#             hover_data={"events": ":,", "stability": ":.2f"}
#         )
#         fig_map.update_geos(
#             projection_type="natural earth",
#             showland=True,
#             landcolor="#1e293b"
#         )
#         fig_map.update_layout(height=500, margin=dict(l=0,r=0,t=0,b=0))
#         st.plotly_chart(fig_map, use_container_width=True)
    
#     with col_right:
#         st.subheader("Top 10 Countries")
#         df_top = df_actors.head(10)
#         fig_bar = px.bar(
#             df_top,
#             x='events',
#             y='actor',
#             orientation='h',
#             template="plotly_dark",
#             color='stability',
#             color_continuous_scale="RdPu_r"
#         )
#         fig_bar.update_layout(
#             height=500,
#             margin=dict(l=0,r=0,t=0,b=0),
#             yaxis={'categoryorder': 'total ascending'}
#         )
#         st.plotly_chart(fig_bar, use_container_width=True)
    
#     # --- TIME SERIES ---
#     if not df_trends.empty:
#         st.markdown("---")
#         st.subheader("📈 Conflict vs Cooperation Over Time")
        
#         df_trends['date'] = pd.to_datetime(df_trends['event_date'], format='%Y%m%d')
#         df_pivot = df_trends.pivot(index='date', columns='quad_class', values='total_events').fillna(0)
#         df_pivot.columns = ['Verbal Coop', 'Material Coop', 'Verbal Conflict', 'Material Conflict']
        
#         fig_area = px.area(
#             df_pivot,
#             template="plotly_dark",
#             color_discrete_map={
#                 'Verbal Coop': '#4ade80',
#                 'Material Coop': '#22c55e',
#                 'Verbal Conflict': '#fb923c',
#                 'Material Conflict': '#dc2626'
#             }
#         )
#         fig_area.update_layout(height=400, hovermode='x unified')
#         st.plotly_chart(fig_area, use_container_width=True)
    
#     # --- RECENT FEED ---
#     st.markdown("---")
#     st.subheader("📡 Recent Events")
#     st.dataframe(df_recent, use_container_width=True, hide_index=True)
    
#     # --- FOOTER ---
#     st.caption(f"💡 Powered by Apache Flink CDC + Debezium | Query Time: <0.1s")

# else:
#     st.warning("⚠️ No data available. Check Flink jobs are running.")

# # --- AUTO-REFRESH ---
# if auto_refresh:
#     time.sleep(2)
#     st.rerun()

