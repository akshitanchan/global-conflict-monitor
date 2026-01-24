#!/usr/bin/env python3
import os, random, time
import psycopg2
import psycopg2.extras
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
        cur.execute(
            "DELETE FROM public.gdelt_events WHERE globaleventid = ANY(%s)",
            (ids,)
        )
    conn.commit()

def main():
    import argparse
    ap=argparse.ArgumentParser(description="simulate inserts/updates/deletes for gdelt_events")
    # allow both styles:
    #   --insert 50
    #   --insert --n 50
    # (same for --update / --delete)
    ap.add_argument("--n", type=int, default=None, help="default count when using --insert/--update/--delete as flags")
    ap.add_argument("--insert", nargs="?", const=-1, type=int, default=0, help="number of inserts")
    ap.add_argument("--update", nargs="?", const=-1, type=int, default=0, help="number of updates")
    ap.add_argument("--delete", nargs="?", const=-1, type=int, default=0, help="number of deletes")
    ap.add_argument("--late", action="store_true", help="inserts use past dates (late arrivals)")
    args=ap.parse_args()

    def _resolve_count(val: int) -> int:
        if val == -1:
            if args.n is None:
                raise SystemExit("error: you used --insert/--update/--delete without a value; pass --n <int> too")
            return args.n
        return val

    ins=_resolve_count(args.insert)
    upd=_resolve_count(args.update)
    dele=_resolve_count(args.delete)

    conn=connect()
    try:
        if ins:
            insert_events(conn, ins, late=args.late)
            print(f"inserted {ins} rows{' (late)' if args.late else ''}")
        if upd:
            update_events(conn, upd)
            print(f"updated {upd} rows")
        if dele:
            delete_events(conn, dele)
            print(f"deleted {dele} rows")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
