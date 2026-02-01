# benchmark_comparison.py
import time
import psycopg2
import statistics
from tabulate import tabulate

DB_URL = "postgresql://flink_user:flink_pass@localhost:5432/gdelt"

def format_time(seconds):
    if seconds >= 1.0:
        return f"{seconds:.3f}s"
    elif seconds >= 0.001:
        return f"{seconds * 1000:.2f}ms"
    else:
        return f"{seconds * 1_000_000:.2f}µs"

print("=" * 70)
print(" POSTGRES-ONLY vs FLINK: QUERY PERFORMANCE COMPARISON")
print("=" * 70)

# Queries for baseline
queries_baseline = [
    ("Daily Events", """
        SELECT
          event_date,
          quad_class,
          SUM(CAST(num_events AS BIGINT)) AS total_events,
          AVG(goldstein) AS avg_goldstein
        FROM gdelt_events
        WHERE event_date BETWEEN 19790101 AND 20260131
        GROUP BY event_date, quad_class
    """),

    ("Top Actors", """
        SELECT
          source_actor,
          SUM(CAST(num_events AS BIGINT)) AS total_events,
          AVG(goldstein) AS avg_goldstein
        FROM gdelt_events
        WHERE event_date BETWEEN 19790101 AND 20260131
          AND source_actor IS NOT NULL
          AND char_length(source_actor) = 3
        GROUP BY source_actor
        ORDER BY total_events DESC
        LIMIT 250
    """),

    ("Dyad Interactions", """
        SELECT
          source_actor,
          target_actor,
          SUM(CAST(num_events AS BIGINT)) AS total_events,
          AVG(goldstein) AS avg_goldstein
        FROM gdelt_events
        WHERE event_date BETWEEN 19790101 AND 20260131
          AND source_actor IS NOT NULL
          AND target_actor IS NOT NULL
        GROUP BY source_actor, target_actor
        ORDER BY total_events DESC
        LIMIT 50
    """),

    ("CAMEO Codes", """
        SELECT
          cameo_code,
          SUM(CAST(num_events AS BIGINT)) AS total_events,
          AVG(goldstein) AS avg_goldstein
        FROM gdelt_events
        WHERE event_date BETWEEN 19790101 AND 20260131
          AND cameo_code IS NOT NULL
        GROUP BY cameo_code
        ORDER BY total_events DESC
        LIMIT 50
    """)
]


# Queries for flink
queries_flink = [
    ("Daily Events", """
        SELECT
          event_date,
          quad_class,
          SUM(total_events) AS total_events,
          AVG(avg_goldstein) AS avg_goldstein
        FROM daily_event_volume_by_quadclass
        WHERE event_date BETWEEN 19790101 AND 20260131
        GROUP BY event_date, quad_class
    """),

    ("Top Actors", """
        SELECT
          source_actor,
          SUM(total_events) AS total_events,
          AVG(avg_goldstein) AS avg_goldstein
        FROM top_actors
        WHERE event_date BETWEEN 19790101 AND 20260131
          AND source_actor IS NOT NULL
          AND char_length(source_actor) = 3
        GROUP BY source_actor
        ORDER BY total_events DESC
        LIMIT 250
    """),

    ("Dyad Interactions", """
        SELECT
          source_actor,
          target_actor,
          SUM(total_events) AS total_events,
          AVG(avg_goldstein) AS avg_goldstein
        FROM dyad_interactions
        WHERE event_date BETWEEN 19790101 AND 20260131
          AND source_actor IS NOT NULL
          AND target_actor IS NOT NULL
        GROUP BY source_actor, target_actor
        ORDER BY total_events DESC
        LIMIT 50
    """),

    ("CAMEO Codes", """
        SELECT
          cameo_code,
          SUM(total_events) AS total_events,
          AVG(avg_goldstein) AS avg_goldstein
        FROM daily_cameo_metrics
        WHERE event_date BETWEEN 19790101 AND 20260131
          AND cameo_code IS NOT NULL
        GROUP BY cameo_code
        ORDER BY total_events DESC
        LIMIT 50
    """)
]


conn = psycopg2.connect(DB_URL)
comparison_table = []
ITERATIONS = 10

for (name_baseline, q_baseline), (_, q_flink) in zip(queries_baseline, queries_flink):
    print(f"\nBenchmarking: {name_baseline}...")

    # Baseline time measurements
    times_baseline = []
    rows_baseline = None
    for i in range(ITERATIONS):
        cur = conn.cursor()
        start = time.perf_counter()
        cur.execute(q_baseline)
        rows_baseline = cur.fetchall()
        elapsed = time.perf_counter() - start
        times_baseline.append(elapsed)
        cur.close()
        print(f"  Postgres-only run {i+1}/{ITERATIONS}: {format_time(elapsed)}")

    # Aggregate time measurements
    times_flink = []
    rows_flink = None
    for i in range(ITERATIONS):
        cur = conn.cursor()
        start = time.perf_counter()
        cur.execute(q_flink)
        rows_flink = cur.fetchall()
        elapsed = time.perf_counter() - start
        times_flink.append(elapsed)
        cur.close()
        print(f"  Aggregate-table run {i+1}/{ITERATIONS}: {format_time(elapsed)}")

    avg_baseline = statistics.mean(times_baseline)
    avg_flink = statistics.mean(times_flink)
    median_baseline = statistics.median(times_baseline)
    median_flink = statistics.median(times_flink)
    speedup = avg_baseline / avg_flink if avg_flink > 0 else 0

    comparison_table.append([
        name_baseline,
        format_time(avg_baseline),
        format_time(avg_flink),
        format_time(median_baseline),
        format_time(median_flink),
        f"{speedup:.1f}x",
        len(rows_baseline) if rows_baseline is not None else 0,
    ])

conn.close()

print("\n" + "=" * 70)
print(" RESULTS")
print("=" * 70)
print("\n" + tabulate(
    comparison_table,
    headers=["Query Type", "Postgres Avg", "Agg Avg", "Postgres Median", "Agg Median", "Speedup", "Rows"],
    tablefmt="grid"
))

print("\n📊 Analysis:")
print(f"  • Each query ran {ITERATIONS} times")
print("  • Baseline = raw table GROUP BY (SUM(num_events))")
print("  • Agg = query on precomputed aggregate tables")
