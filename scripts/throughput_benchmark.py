#!/usr/bin/env python3
#compare batch processing time for postgresql aggregation vs flink incremental updates

import psycopg2
import time
import sys
from typing import List, Dict

# db connection parameters
DB_HOST = "localhost"
DB_NAME = "gdelt"
DB_USER = "flink_user"
DB_PASS = "flink_pass"

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def get_table_columns(table_name):
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'public' 
        AND table_name = '{table_name}'
        ORDER BY ordinal_position;
    """)
    
    columns = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    
    return columns

def measure_postgres_aggregation(batch_size, columns):
    print(f"\n{'='*70}")
    print(f"BASELINE: PostgreSQL Full Aggregation ({batch_size:,} rows)")
    print(f"{'='*70}")
    
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    
    # get max globaleventid to avoid duplicates
    cur.execute("SELECT COALESCE(MAX(globaleventid), 0) FROM gdelt_events;")
    max_id = cur.fetchone()[0]
    
    # build insert query dynamically using actual columns
    columns_without_id = [c for c in columns if c != 'globaleventid']
    columns_str = ', '.join(columns_without_id)
    
    # insert batch
    print(f"Inserting {batch_size:,} rows into source table...")
    insert_start = time.time()
    
    cur.execute(f"""
        INSERT INTO gdelt_events (globaleventid, {columns_str})
        SELECT 
            globaleventid + {max_id + 1000000},
            {columns_str}
        FROM gdelt_events 
        ORDER BY RANDOM() 
        LIMIT {batch_size};
    """)
    
    insert_time = time.time() - insert_start
    print(f"Insert completed in {insert_time:.2f}s")
    
    # run aggregation queries (simulating full recomputation)
    print("Running aggregation queries (full table scans)...")
    agg_start = time.time()
    
    aggregations = {
    # daily event volume by quadclass
    'daily_event_volume_by_quadclass': """
        SELECT
            event_date,
            quad_class,
            SUM(CAST(num_events AS BIGINT)) AS total_events,
            AVG(goldstein) AS avg_goldstein
        FROM gdelt_events
        GROUP BY event_date, quad_class;
    """,

    # dyadic interactions (source -> target)
    'dyad_interactions': """
        SELECT
            event_date,
            source_actor,
            target_actor,
            SUM(CAST(num_events AS BIGINT)) AS total_events,
            AVG(goldstein) AS avg_goldstein
        FROM gdelt_events
        WHERE source_actor IS NOT NULL
          AND target_actor IS NOT NULL
        GROUP BY event_date, source_actor, target_actor;
    """,

    # top actors (source actor only)
    'top_actors': """
        SELECT
            event_date,
            source_actor,
            SUM(CAST(num_events AS BIGINT)) AS total_events,
            AVG(goldstein) AS avg_goldstein
        FROM gdelt_events
        WHERE source_actor IS NOT NULL
        GROUP BY event_date, source_actor;
    """,

    # cameo metrics
    'daily_cameo_metrics': """
        SELECT
            event_date,
            cameo_code,
            SUM(CAST(num_events AS BIGINT)) AS total_events,
            AVG(goldstein) AS avg_goldstein
        FROM gdelt_events
        WHERE cameo_code IS NOT NULL
        GROUP BY event_date, cameo_code;
    """
    }
    
    for name, query in aggregations.items():
        cur.execute(query)
    
    agg_time = time.time() - agg_start
    total_time = insert_time + agg_time
    
    print(f"Aggregation completed in {agg_time:.2f}s")
    print(f"\nBASELINE RESULTS:")
    print(f"  Insert time:       {insert_time:.2f}s")
    print(f"  Aggregation time:  {agg_time:.2f}s")
    print(f"  Total time:        {total_time:.2f}s")
    print(f"  Throughput:        {batch_size / total_time:,.0f} rows/sec")
    
    cur.close()
    conn.close()
    
    return {
        'insert_time': insert_time,
        'aggregation_time': agg_time,
        'total_time': total_time,
        'throughput': batch_size / total_time
    }

def measure_flink_incremental(batch_size, columns):
    print(f"\n{'='*70}")
    print(f"INCREMENTAL: Flink CDC Processing ({batch_size:,} rows)")
    print(f"{'='*70}")
    
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    
    # record last_updated timestamps before insert from all 4 aggregate tables
    aggregate_tables = [
        'daily_event_volume_by_quadclass',
        'dyad_interactions',
        'top_actors',
        'daily_cameo_metrics'
    ]
    
    baseline_timestamps = {}
    for table in aggregate_tables:
        try:
            cur.execute(f"SELECT MAX(last_updated) FROM {table};")
            baseline_timestamps[table] = cur.fetchone()[0]
        except Exception as e:
            print(f"  Warning: Could not query {table}: {e}")
            baseline_timestamps[table] = None
    
    # get max id to avoid duplicates
    cur.execute("SELECT COALESCE(MAX(globaleventid), 0) FROM gdelt_events;")
    max_id = cur.fetchone()[0]
    
    # build insert query dynamically
    columns_without_id = [c for c in columns if c != 'globaleventid']
    columns_str = ', '.join(columns_without_id)
    
    # insert batch
    print(f"Inserting {batch_size:,} rows into source table...")
    insert_start = time.time()
    
    cur.execute(f"""
        INSERT INTO gdelt_events (globaleventid, {columns_str})
        SELECT 
            globaleventid + {max_id + 2000000},
            {columns_str}
        FROM gdelt_events 
        ORDER BY RANDOM() 
        LIMIT {batch_size};
    """)
    
    insert_time = time.time() - insert_start
    print(f"Insert completed in {insert_time:.2f}s")
    
    # wait for flink to propagate changes to all aggregate tables
    print("Waiting for Flink CDC to update all 4 aggregate tables...")
    propagation_start = time.time()
    
    max_wait = 120  # max 2 minutes wait
    check_interval = 0.5  # check every 500ms
    
    updated_tables = set()
    
    while (time.time() - propagation_start) < max_wait:
        for table in aggregate_tables:
            if table in updated_tables:
                continue
                
            try:
                cur.execute(f"SELECT MAX(last_updated) FROM {table};")
                current_timestamp = cur.fetchone()[0]
                
                if (baseline_timestamps[table] and current_timestamp and 
                    current_timestamp > baseline_timestamps[table]):
                    updated_tables.add(table)
                    print(f"  {table} updated")
            except Exception:
                pass
        
        if len(updated_tables) == len(aggregate_tables):
            propagation_time = time.time() - propagation_start
            print(f"\nAll aggregates updated in {propagation_time:.2f}s")
            break
        
        time.sleep(check_interval)
    else:
        propagation_time = time.time() - propagation_start
        print(f"\nTimeout: Only {len(updated_tables)}/{len(aggregate_tables)} tables updated")
        print(f"    Updated: {', '.join(updated_tables)}")
        print(f"    Missing: {', '.join(set(aggregate_tables) - updated_tables)}")
    
    total_time = insert_time + propagation_time
    
    print(f"\nINCREMENTAL RESULTS:")
    print(f"  Insert time:       {insert_time:.2f}s")
    print(f"  CDC propagation:   {propagation_time:.2f}s")
    print(f"  Total time:        {total_time:.2f}s")
    print(f"  Throughput:        {batch_size / total_time:,.0f} rows/sec")
    
    cur.close()
    conn.close()
    
    return {
        'insert_time': insert_time,
        'propagation_time': propagation_time,
        'total_time': total_time,
        'throughput': batch_size / total_time,
        'updated_tables': len(updated_tables)
    }

def print_results_table(results: List[Dict]):
    print("\n" + "="*100)
    print("AGGREGATE MAINTENANCE THROUGHPUT COMPARISON")
    print("="*100)
    
    # header
    print(f"+{'-'*12}+{'-'*18}+{'-'*18}+{'-'*18}+{'-'*18}+{'-'*12}+")
    print(f"| {'Batch Size':<10} | {'Baseline Total':<16} | {'Flink Total':<16} | "
          f"{'Baseline Tput':<16} | {'Flink Tput':<16} | {'Speedup':<10} |")
    print(f"+{'='*12}+{'='*18}+{'='*18}+{'='*18}+{'='*18}+{'='*12}+")
    
    # data rows
    for r in results:
        batch_size = f"{r['batch_size']:,}"
        baseline_time = f"{r['baseline']['total_time']:.2f}s"
        flink_time = f"{r['incremental']['total_time']:.2f}s"
        baseline_tput = f"{r['baseline']['throughput']:,.0f} r/s"
        flink_tput = f"{r['incremental']['throughput']:,.0f} r/s"
        speedup = f"{r['speedup']:.1f}x"
        
        print(f"| {batch_size:>10} | {baseline_time:>16} | {flink_time:>16} | "
              f"{baseline_tput:>16} | {flink_tput:>16} | {speedup:>10} |")
        print(f"+{'-'*12}+{'-'*18}+{'-'*18}+{'-'*18}+{'-'*18}+{'-'*12}+")
    
    # calculate averages
    avg_speedup = sum(r['speedup'] for r in results) / len(results)
    avg_baseline_tput = sum(r['baseline']['throughput'] for r in results) / len(results)
    avg_flink_tput = sum(r['incremental']['throughput'] for r in results) / len(results)
    
    print(f"\n{'SUMMARY STATISTICS':<20}")
    print(f"{'-'*50}")
    print(f"  Average baseline throughput:     {avg_baseline_tput:>10,.0f} rows/sec")
    print(f"  Average incremental throughput:  {avg_flink_tput:>10,.0f} rows/sec")
    print(f"  Average speedup:                 {avg_speedup:>10.1f}x")
    print(f"  Throughput improvement:          {((avg_flink_tput/avg_baseline_tput - 1) * 100):>10.1f}%")
    print("="*100 + "\n")

def compare_throughput(batch_sizes=[1000, 5000, 10000]):
    print("\n" + "="*70)
    print("THROUGHPUT BENCHMARK: PostgreSQL Aggregation vs Flink CDC")
    print("="*70)
    
    # get table columns dynamically
    print("\nDetecting table schema...")
    columns = get_table_columns('gdelt_events')
    print(f"   Found {len(columns)} columns in gdelt_events")
    
    print("\nThis benchmark measures:")
    print("  BASELINE:     INSERT + Full aggregation (4 GROUP BY queries)")
    print("  INCREMENTAL:  INSERT + Flink CDC propagation to 4 aggregate tables")
    print("="*70)
    
    results = []
    
    for batch_size in batch_sizes:
        print(f"\n\n{'#'*70}")
        print(f"Testing batch size: {batch_size:,} rows")
        print(f"{'#'*70}")
        
        try:
            # baseline approach
            baseline = measure_postgres_aggregation(batch_size, columns)
            
            # wait between tests
            print("\nWaiting 5 seconds before next test...")
            time.sleep(5)
            
            # incremental approach  
            incremental = measure_flink_incremental(batch_size, columns)
            
            # calculate speedup
            speedup = baseline['total_time'] / incremental['total_time']
            
            results.append({
                'batch_size': batch_size,
                'baseline': baseline,
                'incremental': incremental,
                'speedup': speedup
            })
            
            print(f"\n{'='*70}")
            print(f"COMPARISON for {batch_size:,} rows:")
            print(f"{'='*70}")
            print(f"  Baseline (full agg):  {baseline['total_time']:.2f}s ({baseline['throughput']:,.0f} rows/sec)")
            print(f"  Incremental (CDC):    {incremental['total_time']:.2f}s ({incremental['throughput']:,.0f} rows/sec)")
            print(f"  SPEEDUP:              {speedup:.1f}x faster")
            print(f"{'='*70}")
            
        except Exception as e:
            print(f"\nError during batch {batch_size}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    if not results:
        print("\nNo successful tests completed")
        return []
    
    # print formatted results table
    print_results_table(results)
    
    return results

if __name__ == "__main__":
    # default test sizes
    batch_sizes = [1000, 5000, 10000]
    
    if len(sys.argv) > 1:
        # allow custom batch sizes from command line
        batch_sizes = [int(x) for x in sys.argv[1:]]
    
    print("\nStarting throughput benchmark...")
    print(f"   Test batch sizes: {', '.join(str(x) for x in batch_sizes)}")
    print(f"   Note: Dynamically detects table schema to avoid column errors\n")
    
    results = compare_throughput(batch_sizes)
    
    if results:
        print("Benchmark complete!")
    else:
        print("\nBenchmark completed with errors")