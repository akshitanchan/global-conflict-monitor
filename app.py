import os
import sys
import time
import select
import subprocess
from uuid import uuid4
from datetime import date, datetime
from typing import Optional, Dict, Any

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

# marker actors used to detect batch boundaries
MARKER_PREFIX = "__batch_"


# ----------------------------
# THEME / CSS
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

      .block-container { padding-top: 3.0rem; padding-bottom: 1.6rem; }

      .header-row{
        display:flex;
        align-items:flex-start;
        justify-content:space-between;
        gap: 14px;
        flex-wrap: wrap;
        margin-bottom: 10px;
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
        font-size: 2.05rem;
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

      .chiprow{
        display:flex;
        gap:10px;
        flex-wrap:wrap;
        justify-content:flex-end;
        align-items:center;
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
        font-size: 1.70rem;
        font-weight: 900;
        margin-top: 8px;
        margin-bottom: 4px;
        line-height: 1.0;
      }
      .kpi .hint { color: var(--muted); font-size: 0.86rem; font-weight: 600; }

      .sp-18{ height: 18px; }
      .sp-26{ height: 26px; }

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


def marker_actor(batch_id: str) -> str:
    return f"{MARKER_PREFIX}{batch_id}__"


def wait_for_marker(actor: str, marker_date: int, timeout_s: int = 600, poll_s: float = 0.25) -> Optional[float]:
    start = time.time()
    deadline = start + timeout_s
    while time.time() < deadline:
        try:
            conn = get_db_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT 1
                        FROM top_actors
                        WHERE event_date = %s AND source_actor = %s
                        LIMIT 1;
                        """,
                        (int(marker_date), actor),
                    )
                    if cur.fetchone() is not None:
                        return time.time() - start
            finally:
                conn.close()
        except Exception:
            pass
        time.sleep(poll_s)
    return None


def measure_baseline_full_recompute() -> Dict[str, float]:
    """measure the time of full recompute-style queries (without materializing results).

    uses COUNT(*) wrappers so postgres must execute the full GROUP BY, but we don't fetch huge result sets.
    """
    queries = {
        "daily_event_volume_by_quadclass": """
            SELECT COUNT(*) FROM (
              SELECT event_date, quad_class,
                     SUM(num_events)::bigint AS total_events,
                     SUM(num_articles)::bigint AS total_articles,
                     AVG(goldstein) AS avg_goldstein
              FROM gdelt_events
              GROUP BY event_date, quad_class
            ) t;
        """,
        "top_actors": """
            SELECT COUNT(*) FROM (
              SELECT event_date, source_actor,
                     SUM(num_events)::bigint AS total_events,
                     SUM(num_articles)::bigint AS total_articles,
                     AVG(goldstein) AS avg_goldstein
              FROM gdelt_events
              GROUP BY event_date, source_actor
            ) t;
        """,
        "daily_cameo_metrics": """
            SELECT COUNT(*) FROM (
              SELECT event_date, cameo_code,
                     SUM(num_events)::bigint AS total_events,
                     SUM(num_articles)::bigint AS total_articles,
                     AVG(goldstein) AS avg_goldstein
              FROM gdelt_events
              GROUP BY event_date, cameo_code
            ) t;
        """,
        "dyad_interactions": """
            SELECT COUNT(*) FROM (
              SELECT event_date, source_actor, target_actor,
                     SUM(num_events)::bigint AS total_events,
                     AVG(goldstein) AS avg_goldstein
              FROM gdelt_events
              GROUP BY event_date, source_actor, target_actor
            ) t;
        """,
    }

    out: Dict[str, float] = {}
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            for name, sql in queries.items():
                t0 = time.time()
                cur.execute(sql)
                cur.fetchone()
                out[name] = time.time() - t0
    finally:
        conn.close()

    out["total"] = sum(out.values())
    return out


def compute_correctness_summary(dates: list[int], topk: int = 20) -> Dict[str, Any]:
    """lightweight correctness checks comparing sinks to source recompute for a few impacted dates."""
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            # totals per day (events + articles)
            cur.execute(
                """
                SELECT event_date,
                       COALESCE(SUM(num_events),0)::bigint AS src_events,
                       COALESCE(SUM(num_articles),0)::bigint AS src_articles
                FROM gdelt_events
                WHERE event_date = ANY(%s)
                GROUP BY event_date
                ORDER BY event_date;
                """,
                (dates,),
            )
            src_totals = pd.DataFrame(cur.fetchall(), columns=["event_date", "src_events", "src_articles"])

            cur.execute(
                """
                SELECT event_date,
                       COALESCE(SUM(total_events),0)::bigint AS sink_events,
                       COALESCE(SUM(total_articles),0)::bigint AS sink_articles
                FROM daily_event_volume_by_quadclass
                WHERE event_date = ANY(%s)
                GROUP BY event_date
                ORDER BY event_date;
                """,
                (dates,),
            )
            sink_totals = pd.DataFrame(cur.fetchall(), columns=["event_date", "sink_events", "sink_articles"])

            totals = src_totals.merge(sink_totals, on="event_date", how="outer").fillna(0)
            totals["abs_events_diff"] = (totals["src_events"] - totals["sink_events"]).abs()
            totals["abs_articles_diff"] = (totals["src_articles"] - totals["sink_articles"]).abs()

            # quadclass per day max diff across 4 buckets
            cur.execute(
                """
                SELECT event_date, quad_class, COALESCE(SUM(num_events),0)::bigint AS src_events
                FROM gdelt_events
                WHERE event_date = ANY(%s)
                GROUP BY event_date, quad_class;
                """,
                (dates,),
            )
            src_q = pd.DataFrame(cur.fetchall(), columns=["event_date", "quad_class", "src_events"])

            cur.execute(
                """
                SELECT event_date, quad_class, COALESCE(SUM(total_events),0)::bigint AS sink_events
                FROM daily_event_volume_by_quadclass
                WHERE event_date = ANY(%s)
                GROUP BY event_date, quad_class;
                """,
                (dates,),
            )
            sink_q = pd.DataFrame(cur.fetchall(), columns=["event_date", "quad_class", "sink_events"])

            quad = src_q.merge(sink_q, on=["event_date", "quad_class"], how="outer").fillna(0)
            quad["abs_diff"] = (quad["src_events"] - quad["sink_events"]).abs()

            # top-k cameo match (per date)
            cameo_rows = []
            actor_rows = []
            for d in dates:
                cur.execute(
                    """
                    SELECT cameo_code
                    FROM (
                      SELECT cameo_code, SUM(num_events)::bigint AS total_events
                      FROM gdelt_events
                      WHERE event_date=%s AND cameo_code IS NOT NULL
                      GROUP BY cameo_code
                      ORDER BY total_events DESC, cameo_code ASC
                      LIMIT %s
                    ) t;
                    """,
                    (int(d), int(topk)),
                )
                base_cameo = [r[0] for r in cur.fetchall()]

                cur.execute(
                    """
                    SELECT cameo_code
                    FROM (
                      SELECT cameo_code, SUM(total_events)::bigint AS total_events
                      FROM daily_cameo_metrics
                      WHERE event_date=%s AND cameo_code IS NOT NULL
                      GROUP BY cameo_code
                      ORDER BY total_events DESC, cameo_code ASC
                      LIMIT %s
                    ) t;
                    """,
                    (int(d), int(topk)),
                )
                sink_cameo = [r[0] for r in cur.fetchall()]
                inter = len(set(base_cameo).intersection(set(sink_cameo)))
                cameo_rows.append({"event_date": d, "topk": topk, "match": inter, "match_rate": inter / topk if topk else None})

                # top-k actors match (exclude batch markers)
                cur.execute(
                    """
                    SELECT source_actor
                    FROM (
                      SELECT source_actor, SUM(num_events)::bigint AS total_events
                      FROM gdelt_events
                      WHERE event_date=%s
                        AND source_actor IS NOT NULL
                        AND source_actor NOT LIKE %s
                      GROUP BY source_actor
                      ORDER BY total_events DESC, source_actor ASC
                      LIMIT %s
                    ) t;
                    """,
                    (int(d), f"{MARKER_PREFIX}%", int(topk)),
                )
                base_actor = [r[0] for r in cur.fetchall()]

                cur.execute(
                    """
                    SELECT source_actor
                    FROM (
                      SELECT source_actor, SUM(total_events)::bigint AS total_events
                      FROM top_actors
                      WHERE event_date=%s
                        AND source_actor IS NOT NULL
                        AND source_actor NOT LIKE %s
                      GROUP BY source_actor
                      ORDER BY total_events DESC, source_actor ASC
                      LIMIT %s
                    ) t;
                    """,
                    (int(d), f"{MARKER_PREFIX}%", int(topk)),
                )
                sink_actor = [r[0] for r in cur.fetchall()]
                inter_a = len(set(base_actor).intersection(set(sink_actor)))
                actor_rows.append({"event_date": d, "topk": topk, "match": inter_a, "match_rate": inter_a / topk if topk else None})

    finally:
        conn.close()

    return {
        "totals": totals,
        "quad": quad,
        "cameo_topk": pd.DataFrame(cameo_rows),
        "actor_topk": pd.DataFrame(actor_rows),
    }


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


# ----------------------------
# SESSION STATE INIT
# ----------------------------
if "listener_conn" not in st.session_state:
    st.session_state.listener_conn = setup_listener()

if "data_version" not in st.session_state:
    st.session_state.data_version = 0

if "last_refresh_time" not in st.session_state:
    st.session_state.last_refresh_time = datetime.now()

if "last_polled_max_date" not in st.session_state:
    st.session_state.last_polled_max_date = None

if "last_poll_check_ts" not in st.session_state:
    st.session_state.last_poll_check_ts = 0.0

if "processing_time" not in st.session_state:
    st.session_state.processing_time = None
if "last_batch_size" not in st.session_state:
    st.session_state.last_batch_size = 0
if "last_operation" not in st.session_state:
    st.session_state.last_operation = "None"
if "last_throughput" not in st.session_state:
    st.session_state.last_throughput = None

if "last_benchmark" not in st.session_state:
    st.session_state.last_benchmark = None


# ----------------------------
# META: MIN/MAX DATES
# ----------------------------
meta = qdf("""
    SELECT MIN(event_date) AS min_event_date,
           MAX(event_date) AS max_event_date
    FROM daily_event_volume_by_quadclass;
""")

using_sink_meta = True
if meta.empty or pd.isna(meta.loc[0, "min_event_date"]) or pd.isna(meta.loc[0, "max_event_date"]):
    using_sink_meta = False
    meta = qdf("""
        SELECT MIN(event_date) AS min_event_date,
               MAX(event_date) AS max_event_date
        FROM gdelt_events;
    """)

if meta.empty or pd.isna(meta.loc[0, "min_event_date"]) or pd.isna(meta.loc[0, "max_event_date"]):
    st.error("No data found in gdelt_events. load data first (bulk load) and then start the flink pipeline.")
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
    refresh_seconds = st.slider("Check interval (seconds)", 1, 30, 1)
    poll_seconds = st.slider("Fallback poll (seconds)", 10, 120, 30)

    st.markdown("---")
    st.markdown("## batch + benchmark")

    ins_n = st.number_input("delta inserts", min_value=0, max_value=5_000_000, value=20000, step=1000)
    upd_n = st.number_input("delta updates", min_value=0, max_value=5_000_000, value=50, step=10)
    del_n = st.number_input("delta deletes", min_value=0, max_value=5_000_000, value=20, step=10)

    late = st.toggle("late arrivals", value=False)
    late_date = None
    if late:
        late_date = st.selectbox("late date", options=[19790101, 19800101, 19900101, 20010101, 20150101], index=0)

    topk = st.slider("top-k (match rate)", 5, 100, 20)
    run_baseline = st.toggle("run baseline recompute (slow)", value=True)

    if st.button("run batch", use_container_width=True, type="primary"):
        batch_id = uuid4().hex[:12]
        marker = marker_actor(batch_id)
        # keep batches inside the current dataset range (default: latest date in filters)
        mdate = int_yyyymmdd(end_d)

        cmd = [sys.executable, "scripts/simulate-changes.py"]
        if ins_n:
            cmd += ["--insert", str(int(ins_n))]
        if upd_n:
            cmd += ["--update", str(int(upd_n))]
        if del_n:
            cmd += ["--delete", str(int(del_n))]
        if late:
            cmd += ["--late", "--late-date", str(int(late_date))]
        cmd += ["--base-date", str(mdate)]
        cmd += ["--marker", marker, "--marker-date", str(mdate)]

        t0 = time.time()
        with st.spinner("applying batch changes..."):
            try:
                r = subprocess.run(cmd, check=True, capture_output=True, text=True)
            except subprocess.CalledProcessError as e:
                st.error("batch workload failed")
                st.code((e.stdout or "")[:5000])
                st.code((e.stderr or "")[:5000])
                st.stop()

        apply_s = time.time() - t0

        with st.spinner("waiting for flink to catch up..."):
            wait_s = wait_for_marker(marker, mdate, timeout_s=900, poll_s=0.25)

        incremental_total_s = None
        if wait_s is not None:
            incremental_total_s = apply_s + wait_s
        else:
            st.warning("timed out waiting for flink to process the batch marker. is the pipeline job running?")

        baseline = None
        if run_baseline:
            with st.spinner("running baseline full recompute queries (this can be slow)..."):
                baseline = measure_baseline_full_recompute()

        impacted_dates = [mdate]
        if late and late_date is not None:
            impacted_dates.append(int(late_date))

        with st.spinner("checking correctness (impacted dates only)..."):
            correctness = compute_correctness_summary(impacted_dates, topk=int(topk))

        baseline_total_s = float(baseline["total"]) if baseline is not None else None
        speedup = None
        if baseline_total_s is not None and incremental_total_s is not None and incremental_total_s > 0:
            speedup = baseline_total_s / incremental_total_s

        st.session_state.last_benchmark = {
            "batch_id": batch_id,
            "marker": marker,
            "marker_date": mdate,
            "impacted_dates": impacted_dates,
            "inputs": {"insert": int(ins_n), "update": int(upd_n), "delete": int(del_n), "late": bool(late), "late_date": late_date},
            "apply_s": apply_s,
            "catchup_s": wait_s,
            "incremental_total_s": incremental_total_s,
            "baseline": baseline,
            "baseline_total_s": baseline_total_s,
            "speedup": speedup,
            "correctness": correctness,
            "ran_at": datetime.now().isoformat(timespec="seconds"),
            "stdout": (r.stdout or "")[:5000],
        }

        # keep the existing KPI card behavior
        if incremental_total_s is not None:
            st.session_state.processing_time = incremental_total_s
            st.session_state.last_batch_size = int(ins_n + upd_n + del_n)
            st.session_state.last_operation = "BATCH"
            tp = (int(ins_n + upd_n + del_n) / incremental_total_s) if incremental_total_s > 0 else None
            st.session_state.last_throughput = tp

        st.session_state.data_version += 1
        st.session_state.last_refresh_time = datetime.now()

    st.markdown("---")
    if st.session_state.last_benchmark is not None:
        lb = st.session_state.last_benchmark
        inc = lb.get("incremental_total_s")
        base = lb.get("baseline_total_s")
        sp = lb.get("speedup")
        inc_txt = f"{inc:.2f}s" if inc is not None else "—"
        base_txt = f"{base:.2f}s" if base is not None else "—"
        sp_txt = f"{sp:.2f}x" if sp is not None else "—"
        st.info(f"last batch: {lb.get('batch_id')}\n\ninc: {inc_txt} | baseline: {base_txt} | speedup: {sp_txt}")

start_int = int_yyyymmdd(start_d)
end_int = int_yyyymmdd(end_d)


# ----------------------------
# CACHED DATA LOADERS
# ----------------------------
@st.cache_data(show_spinner=False, ttl=3600)
def load_all(version: int, start_i: int, end_i: int, topn: int) -> Dict[str, pd.DataFrame]:
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


# ----------------------------
# HEADER (STATIC)
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


# ----------------------------
# BENCHMARK + ADMIN VIEWS
# ----------------------------
def render_benchmarks_tab(start_i: int, end_i: int):
    st.subheader("benchmarks")

    lb = st.session_state.last_benchmark
    if lb is None:
        st.info("run a batch from the sidebar to populate benchmark results.")
    else:
        c1, c2, c3, c4 = st.columns(4)
        inc = lb.get("incremental_total_s")
        base = lb.get("baseline_total_s")
        sp = lb.get("speedup")
        with c1:
            st.metric("batch id", lb.get("batch_id", "—"))
        with c2:
            st.metric("incremental catch-up", f"{inc:.2f}s" if inc is not None else "—")
        with c3:
            st.metric("baseline recompute", f"{base:.2f}s" if base is not None else "—")
        with c4:
            st.metric("speedup", f"{sp:.2f}x" if sp is not None else "—")

        st.caption(f"ran at: {lb.get('ran_at','—')} • impacted dates: {', '.join(map(str, lb.get('impacted_dates', [])))}")

        b = lb.get("baseline") or {}
        if b:
            st.markdown("#### baseline breakdown")
            dfb = pd.DataFrame([{k: v for k, v in b.items() if k != "total"}]).T.reset_index()
            dfb.columns = ["view", "seconds"]
            dfb = dfb.sort_values("seconds", ascending=False)
            st.dataframe(dfb, use_container_width=True)

        corr = lb.get("correctness")
        if corr is not None:
            st.markdown("#### correctness summary (impacted dates only)")
            totals = corr.get("totals")
            quad = corr.get("quad")
            cameo_topk = corr.get("cameo_topk")
            actor_topk = corr.get("actor_topk")
            if isinstance(totals, pd.DataFrame) and not totals.empty:
                st.markdown("**totals**")
                st.dataframe(totals, use_container_width=True)
            if isinstance(quad, pd.DataFrame) and not quad.empty:
                st.markdown("**quad_class diffs**")
                st.dataframe(quad.sort_values(["event_date", "quad_class"]), use_container_width=True)
            if isinstance(cameo_topk, pd.DataFrame) and not cameo_topk.empty:
                st.markdown("**top-k match rate: cameo**")
                st.dataframe(cameo_topk, use_container_width=True)
            if isinstance(actor_topk, pd.DataFrame) and not actor_topk.empty:
                st.markdown("**top-k match rate: actors**")
                st.dataframe(actor_topk, use_container_width=True)

        with st.expander("batch logs"):
            st.code(lb.get("stdout", ""))

    st.markdown("---")
    st.subheader("baseline vs incremental (current filters)")
    mode = st.radio("mode", ["incremental (sinks)", "baseline (recompute)"], horizontal=True)

    def _time_df(sql: str, params: dict) -> tuple[pd.DataFrame, float]:
        t0 = time.time()
        df = qdf(sql, params=params)
        return df, time.time() - t0

    if mode.startswith("incremental"):
        df, t_s = _time_df(
            """
            SELECT
              SUM(total_events) AS total_events,
              SUM(CASE WHEN quad_class IN (3,4) THEN total_events ELSE 0 END) AS conflict_events,
              AVG(avg_goldstein) AS mean_goldstein
            FROM daily_event_volume_by_quadclass
            WHERE event_date BETWEEN %(s)s AND %(e)s;
            """,
            {"s": start_i, "e": end_i},
        )
        st.metric("query time", f"{t_s:.3f}s")
        st.dataframe(df, use_container_width=True)
    else:
        df, t_s = _time_df(
            """
            SELECT
              COALESCE(SUM(num_events),0)::BIGINT AS total_events,
              COALESCE(SUM(CASE WHEN quad_class IN (3,4) THEN num_events ELSE 0 END),0)::BIGINT AS conflict_events,
              AVG(goldstein) AS mean_goldstein
            FROM gdelt_events
            WHERE event_date BETWEEN %(s)s AND %(e)s;
            """,
            {"s": start_i, "e": end_i},
        )
        st.metric("query time", f"{t_s:.3f}s")
        st.dataframe(df, use_container_width=True)


def render_admin_tab():
    st.subheader("admin")
    st.markdown("this is intentionally separate. the dashboard is not your loader.")

    st.markdown("#### initial bulk load (one-time)")
    st.info(
        "recommended: do the bulk load from cli, then start the flink pipeline. "
        "this keeps benchmarks honest (avoids streamlit overhead)."
    )
    st.code(
        """
./scripts/setup/load-gdelt-copy.sh data/GDELT.MASTERREDUCEDV2.TXT
./scripts/setup/start-flink-aggregations.sh
        """.strip()
    )

    with st.expander("run bulk load from here (danger)"):
        file_path = st.text_input("file path", value="data/GDELT.MASTERREDUCEDV2.TXT")
        small_lines = st.number_input("SMALL_LOAD_LINES (optional)", min_value=0, max_value=500_000_000, value=0, step=100000)
        confirm = st.checkbox("i understand this truncates gdelt_events", value=False)
        if st.button("run bulk load", disabled=not confirm):
            env = os.environ.copy()
            if int(small_lines) > 0:
                env["SMALL_LOAD_LINES"] = str(int(small_lines))
            else:
                env.pop("SMALL_LOAD_LINES", None)
            with st.spinner("running bulk load script..."):
                try:
                    r = subprocess.run(
                        ["bash", "./scripts/setup/load-gdelt-copy.sh", file_path],
                        check=True,
                        capture_output=True,
                        text=True,
                        env=env,
                    )
                except subprocess.CalledProcessError as e:
                    st.error("bulk load failed")
                    st.code((e.stdout or "")[:5000])
                    st.code((e.stderr or "")[:5000])
                    return
            st.success("bulk load complete. start flink jobs next.")
            with st.expander("loader output"):
                st.code((r.stdout or "")[:10000])


# ----------------------------
# LIVE UPDATE ENGINE
# ----------------------------
def maybe_bump_version_from_live_checks():
    got_notify = check_notifications()
    polled_new = False
    now_ts = time.time()

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


# If fragments exist: no full-page rerun loop.
# Only these blocks refresh on a timer.
if hasattr(st, "fragment"):

    @st.fragment(run_every=None)
    def render_dashboard():
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

        # KPI row (stable layout)
        k1, k2, k3, k4 = st.columns(4)
        with k1: kpi_card("Total events", f"{total_events:,}", "All quad classes")
        with k2: kpi_card("Conflict events", f"{conflict_events:,}", "Quad 3 and 4")
        with k3: kpi_card("Conflict rate", f"{conflict_rate:.1f}%", "Conflict / total")
        with k4:
            if st.session_state.processing_time is None:
                kpi_card("Processing time", "—", "Run a workload from the sidebar")
            else:
                tp = st.session_state.last_throughput
                tp_txt = f"{tp:,.0f} rows/sec" if tp is not None else "—"
                kpi_card(
                    "Processing time",
                    f"{st.session_state.processing_time:.2f}s",
                    f"{st.session_state.last_operation} • {st.session_state.last_batch_size:,} rows • {tp_txt}",
                )

        st.markdown('<div class="sp-26"></div>', unsafe_allow_html=True)

        # TOP VISUALS (MAP bigger, TRENDS smaller)
        left, right = st.columns([1.6, 1.0])

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
                    height=350,
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
                    height=350,
                    margin=dict(l=16, r=16, t=10, b=10),
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    hovermode="x unified",
                    legend=dict(orientation="h", y=1.12),
                    yaxis=dict(title="Events"),
                    yaxis2=dict(title="Goldstein", overlaying="y", side="right"),
                )
                st.plotly_chart(fig, use_container_width=True)

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

    @st.fragment(run_every="1s")
    def live_tick():
        if not live_refresh:
            return
        maybe_bump_version_from_live_checks()

    tab_dash, tab_bench, tab_admin = st.tabs(["dashboard", "benchmarks", "admin"])
    with tab_dash:
        live_tick()
        render_dashboard()
    with tab_bench:
        render_benchmarks_tab(start_int, end_int)
    with tab_admin:
        render_admin_tab()

else:
    # Fallback (older Streamlit): you cannot avoid full reruns without fragments.
    # This keeps behavior correct, but the map will redraw on each rerun.
    maybe_bump_version_from_live_checks()

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

    k1, k2, k3, k4 = st.columns(4)
    with k1: kpi_card("Total events", f"{total_events:,}", "All quad classes")
    with k2: kpi_card("Conflict events", f"{conflict_events:,}", "Quad 3 and 4")
    with k3: kpi_card("Conflict rate", f"{conflict_rate:.1f}%", "Conflict / total")
    with k4:
        if st.session_state.processing_time is None:
            kpi_card("Processing time", "—", "Run a workload from the sidebar")
        else:
            tp = st.session_state.last_throughput
            tp_txt = f"{tp:,.0f} rows/sec" if tp is not None else "—"
            kpi_card(
                "Processing time",
                f"{st.session_state.processing_time:.2f}s",
                f"{st.session_state.last_operation} • {st.session_state.last_batch_size:,} rows • {tp_txt}",
            )

    st.markdown('<div class="sp-26"></div>', unsafe_allow_html=True)

    left, right = st.columns([1.6, 1.0])
    with left:
        st.subheader("World Map")
        if actors.empty:
            st.info("No ISO-3 actor rows available in this period.")
        else:
            if map_metric == "Avg Goldstein":
                fig_map = px.choropleth(
                    actors, locations="iso3", locationmode="ISO-3",
                    color="mean_goldstein",
                    hover_name="iso3",
                    hover_data={"total_events": ":,", "mean_goldstein": ":.2f", "iso3": False},
                    color_continuous_scale="RdBu_r", color_continuous_midpoint=0,
                    template="plotly_dark",
                )
            else:
                fig_map = px.choropleth(
                    actors, locations="iso3", locationmode="ISO-3",
                    color="total_events",
                    hover_name="iso3",
                    hover_data={"total_events": ":,", "mean_goldstein": ":.2f", "iso3": False},
                    color_continuous_scale="Viridis",
                    template="plotly_dark",
                )
            fig_map.update_layout(height=350, margin=dict(l=0, r=0, t=0, b=0), paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_map, use_container_width=True)

    with right:
        st.subheader("Trends")
        if trend.empty:
            st.info("No data in this range.")
        else:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=trend["event_day"], y=trend["total_events"], mode="lines", name="Total", fill="tozeroy"))
            fig.add_trace(go.Scatter(x=trend["event_day"], y=trend["conflict_events"], mode="lines", name="Conflict"))
            fig.add_trace(go.Scatter(x=trend["event_day"], y=trend["mean_goldstein"], mode="lines", name="Goldstein", line=dict(dash="dot"), yaxis="y2"))
            fig.update_layout(template="plotly_dark", height=350, hovermode="x unified",
                              yaxis=dict(title="Events"), yaxis2=dict(title="Goldstein", overlaying="y", side="right"))
            st.plotly_chart(fig, use_container_width=True)

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
                    fig_hm = go.Figure(data=go.Heatmap(z=pivot.values, x=pivot.columns, y=pivot.index, colorscale="Plasma"))
                    fig_hm.update_layout(template="plotly_dark", height=380)
                    st.plotly_chart(fig_hm, use_container_width=True)
        with b:
            st.subheader("Top Dyads")
            st.dataframe(dyads, use_container_width=True, hide_index=True)
    with tab2:
        st.subheader("QuadClass")
        st.dataframe(quad_dist, use_container_width=True, hide_index=True)
    with tab3:
        st.subheader("CAMEO")
        st.dataframe(cameo, use_container_width=True, hide_index=True)

    st.markdown("---")
    render_benchmarks_tab(start_int, end_int)
    st.markdown("---")
    render_admin_tab()

    if live_refresh:
        time.sleep(refresh_seconds)
        st.rerun()