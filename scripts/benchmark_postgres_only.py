# Create this as: benchmark_postgres_only.py
import time
import psycopg2

conn = psycopg2.connect("postgresql://flink_user:flink_pass@localhost:5432/gdelt")

# Measure aggregation query time (what would run on every dashboard refresh)
queries = [
    """SELECT event_date, SUM(num_events), AVG(goldstein) 
       FROM gdelt_events 
       WHERE event_date BETWEEN 20200101 AND 20230101
       GROUP BY event_date""",
    
    """SELECT source_actor, target_actor, COUNT(*) 
       FROM gdelt_events 
       WHERE event_date BETWEEN 20200101 AND 20230101
       GROUP BY source_actor, target_actor 
       LIMIT 20""",
]

for query in queries:
    start = time.time()
    cur = conn.cursor()
    cur.execute(query)
    cur.fetchall()
    elapsed = time.time() - start
    print(f"Query took {elapsed:.2f}s")
