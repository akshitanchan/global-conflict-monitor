#!/usr/bin/env python3
import os, random, time
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

DB_URL = os.getenv("DB_URL", "postgresql://flink_user:flink_pass@localhost:5432/gdelt")

COUNTRIES = ["USA","CHN","RUS","IND","GBR","FRA","DEU","JPN","BRA","ZAF"]
CAMEO = ["010","043","050","112","172","173","190","193","0841","084"]
QUAD = [1,2,3,4]

def connect():
    return psycopg2.connect(DB_URL)

def today_int():
    return int(datetime.utcnow().strftime("%Y%m%d"))

def rand_date(late=False):
    if not late:
        return today_int()
    # late arrival: pick a past date in a reasonable range
    return random.choice([19790101, 19800101, 19900101, 20010101, 20150101])

def insert_events(conn, n=100, late=False):
    rows=[]
    for _ in range(n):
        rows.append((
            rand_date(late=late),
            random.choice(COUNTRIES),
            random.choice(COUNTRIES),
            random.choice(CAMEO),
            random.randint(1,3),                 # num_events
            random.randint(1,10),                # num_articles
            random.choice(QUAD),
            round(random.uniform(-10, 10), 2),   # goldstein
        ))
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO public.gdelt_events
              (event_date, source_actor, target_actor, cameo_code, num_events, num_articles, quad_class, goldstein)
            VALUES %s
        """, rows)
    conn.commit()

def update_events(conn, n=50):
    with conn.cursor() as cur:
        cur.execute("SELECT globaleventid FROM public.gdelt_events ORDER BY random() LIMIT %s", (n,))
        ids=[r[0] for r in cur.fetchall()]
        for gid in ids:
            cur.execute(
                "UPDATE public.gdelt_events SET goldstein=%s, num_events=%s WHERE globaleventid=%s",
                (round(random.uniform(-10,10),2), random.randint(1,3), gid)
            )
    conn.commit()

def delete_events(conn, n=20):
    with conn.cursor() as cur:
        cur.execute("SELECT globaleventid FROM public.gdelt_events ORDER BY random() LIMIT %s", (n,))
        ids=[r[0] for r in cur.fetchall()]
        execute_values(cur, "DELETE FROM public.gdelt_events WHERE globaleventid IN %s", [(tuple(ids),)], template=None)
    conn.commit()

def main():
    import argparse
    ap=argparse.ArgumentParser(description="simulate inserts/updates/deletes for gdelt_events")
    ap.add_argument("--insert", type=int, default=0)
    ap.add_argument("--update", type=int, default=0)
    ap.add_argument("--delete", type=int, default=0)
    ap.add_argument("--late", action="store_true", help="inserts use past dates (late arrivals)")
    args=ap.parse_args()

    conn=connect()
    try:
        if args.insert:
            insert_events(conn, args.insert, late=args.late)
            print(f"inserted {args.insert} rows{' (late)' if args.late else ''}")
        if args.update:
            update_events(conn, args.update)
            print(f"updated {args.update} rows")
        if args.delete:
            delete_events(conn, args.delete)
            print(f"deleted {args.delete} rows")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
