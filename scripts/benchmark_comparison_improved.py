# benchmark_comparison_improved.py
import time
import psycopg2
import statistics
from tabulate import tabulate

DB_URL = "postgresql://flink_user:flink_pass@localhost:5432/gdelt"

def format_time(seconds):
    """Format time with appropriate units"""
    if seconds >= 1.0:
        return f"{seconds:.3f}s"
    elif seconds >= 0.001:
        return f"{seconds * 1000:.2f}ms"
    else:
        return f"{seconds * 1_000_000:.2f}µs"

print("=" * 70)
print(" POSTGRES-ONLY vs FLINK: QUERY PERFORMANCE COMPARISON")
print("=" * 70)

# Use full date range to make queries work harder
queries_baseline = [
    ("Daily Events", """
        SELECT event_date, quad_class, 
               COUNT(*) as total_events,
               AVG(goldstein) as avg_goldstein
        FROM gdelt_events
        WHERE event_date BETWEEN 19790101 AND 20260131
        GROUP BY event_date, quad_class
    """),
    
    ("Top Actor Pairs", """
        SELECT source_actor, target_actor, 
               COUNT(*) as total_events
        FROM gdelt_events
        WHERE event_date BETWEEN 19790101 AND 20260131
          AND source_actor IS NOT NULL 
          AND target_actor IS NOT NULL
        GROUP BY source_actor, target_actor
        ORDER BY 3 DESC
        LIMIT 50
    """),
    
    ("CAMEO Codes", """
        SELECT cameo_code,
               COUNT(*) as total_events,
               AVG(goldstein) as avg_goldstein
        FROM gdelt_events
        WHERE event_date BETWEEN 19790101 AND 20260131
          AND cameo_code IS NOT NULL
        GROUP BY cameo_code
        ORDER BY 2 DESC
        LIMIT 50
    """)
]

queries_flink = [
    ("Daily Events", """
        SELECT event_date, quad_class, 
               SUM(total_events),
               AVG(avg_goldstein)
        FROM daily_event_volume_by_quadclass
        WHERE event_date BETWEEN 19790101 AND 20260131
        GROUP BY event_date, quad_class
    """),
    
    ("Top Actor Pairs", """
        SELECT source_actor, target_actor,
               SUM(total_events)
        FROM dyad_interactions
        WHERE event_date BETWEEN 19790101 AND 20260131
        GROUP BY source_actor, target_actor
        ORDER BY 3 DESC
        LIMIT 50
    """),
    
    ("CAMEO Codes", """
        SELECT cameo_code,
               SUM(total_events),
               AVG(avg_goldstein)
        FROM daily_cameo_metrics
        WHERE event_date BETWEEN 19790101 AND 20260131
        GROUP BY cameo_code
        ORDER BY 2 DESC
        LIMIT 50
    """)
]

conn = psycopg2.connect(DB_URL)
comparison_table = []

# Run 10 iterations instead of 3 for more accurate timing
ITERATIONS = 10

for (name_baseline, q_baseline), (name_flink, q_flink) in zip(queries_baseline, queries_flink):
    print(f"\nBenchmarking: {name_baseline}...")
    
    # Measure baseline (Postgres-only)
    times_baseline = []
    for i in range(ITERATIONS):
        cur = conn.cursor()
        start = time.perf_counter()  # More precise than time.time()
        cur.execute(q_baseline)
        rows_baseline = cur.fetchall()
        elapsed = time.perf_counter() - start
        times_baseline.append(elapsed)
        cur.close()
        print(f"  Postgres-only run {i+1}/{ITERATIONS}: {format_time(elapsed)}")
    
    # Measure Flink
    times_flink = []
    for i in range(ITERATIONS):
        cur = conn.cursor()
        start = time.perf_counter()
        cur.execute(q_flink)
        rows_flink = cur.fetchall()
        elapsed = time.perf_counter() - start
        times_flink.append(elapsed)
        cur.close()
        print(f"  Flink run {i+1}/{ITERATIONS}: {format_time(elapsed)}")
    
    # Calculate statistics
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
        len(rows_baseline)
    ])

conn.close()

print("\n" + "=" * 70)
print(" RESULTS")
print("=" * 70)
print("\n" + tabulate(
    comparison_table,
    headers=["Query Type", "Postgres Avg", "Flink Avg", "Postgres Median", "Flink Median", "Speedup", "Rows"],
    tablefmt="grid"
))

print("\n📊 Analysis:")
print(f"  • Each query ran {ITERATIONS} times to ensure accuracy")
print("  • Times shown: avg = average, median = middle value (more robust)")
print("  • Speedup = how many times faster Flink is compared to Postgres-only")
