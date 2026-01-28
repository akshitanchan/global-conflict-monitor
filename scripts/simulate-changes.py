#!/usr/bin/env python3
import os
import random
from datetime import datetime
from typing import Optional

import psycopg2
from psycopg2.extras import execute_values

DB_URL = os.getenv("DB_URL", "postgresql://flink_user:flink_pass@localhost:5432/gdelt")

COUNTRIES = ["USA", "CHN", "RUS", "IND", "GBR", "FRA", "DEU", "JPN", "BRA", "ZAF"]
CAMEO = ["010", "043", "050", "112", "172", "173", "190", "193", "0841", "084"]
QUAD = [1, 2, 3, 4]
LATE_DATES = [19790101, 19800101, 19900101, 20010101, 20150101]


def connect():
    return psycopg2.connect(DB_URL)


def today_int() -> int:
    return int(datetime.utcnow().strftime("%Y%m%d"))


def get_max_event_date(conn) -> Optional[int]:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(event_date) FROM public.gdelt_events;")
            r = cur.fetchone()
            if r and r[0] is not None:
                return int(r[0])
    except Exception:
        pass
    return None


def resolve_insert_date(late: bool, base_date: int, late_date: Optional[int]) -> int:
    if late:
        return int(late_date) if late_date is not None else int(random.choice(LATE_DATES))
    return int(base_date)


def insert_events(conn, n: int = 100, event_date: int = 0):
    rows = []
    for _ in range(n):
        rows.append(
            (
                int(event_date),
                random.choice(COUNTRIES),
                random.choice(COUNTRIES),
                random.choice(CAMEO),
                random.randint(1, 3),  # num_events
                random.randint(1, 10),  # num_articles
                random.choice(QUAD),
                round(random.uniform(-10, 10), 2),  # goldstein
            )
        )
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO public.gdelt_events
              (event_date, source_actor, target_actor, cameo_code, num_events, num_articles, quad_class, goldstein)
            VALUES %s
            """,
            rows,
        )
    conn.commit()


def insert_marker(conn, marker_actor: str, marker_date: int):
    """Insert a marker row so we can detect when Flink has processed a batch.

    marker row uses 0 num_events/num_articles so it doesn't change totals.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.gdelt_events
              (event_date, source_actor, target_actor, cameo_code, num_events, num_articles, quad_class, goldstein)
            VALUES (%s, %s, %s, %s, 0, 0, 1, 0.0)
            """,
            (int(marker_date), marker_actor, random.choice(COUNTRIES), random.choice(CAMEO)),
        )
    conn.commit()


def update_events(conn, n: int = 50):
    with conn.cursor() as cur:
        cur.execute("SELECT globaleventid FROM public.gdelt_events ORDER BY random() LIMIT %s", (n,))
        ids = [r[0] for r in cur.fetchall()]
        for gid in ids:
            cur.execute(
                "UPDATE public.gdelt_events SET goldstein=%s, num_events=%s WHERE globaleventid=%s",
                (round(random.uniform(-10, 10), 2), random.randint(1, 3), gid),
            )
    conn.commit()


def delete_events(conn, n: int = 20):
    with conn.cursor() as cur:
        cur.execute("SELECT globaleventid FROM public.gdelt_events ORDER BY random() LIMIT %s", (n,))
        ids = [r[0] for r in cur.fetchall()]
        cur.execute("DELETE FROM public.gdelt_events WHERE globaleventid = ANY(%s)", (ids,))
    conn.commit()


def main():
    import argparse

    ap = argparse.ArgumentParser(description="simulate inserts/updates/deletes for gdelt_events")

    # allow both styles:
    #   --insert 50
    #   --insert --n 50
    # (same for --update / --delete)
    ap.add_argument("--n", type=int, default=None, help="default count when using --insert/--update/--delete as flags")
    ap.add_argument("--insert", nargs="?", const=-1, type=int, default=0, help="number of inserts")
    ap.add_argument("--update", nargs="?", const=-1, type=int, default=0, help="number of updates")
    ap.add_argument("--delete", nargs="?", const=-1, type=int, default=0, help="number of deletes")

    ap.add_argument(
        "--base-date",
        type=int,
        default=None,
        help="YYYYMMDD to use for normal inserts (defaults to max(event_date) in table)",
    )
    ap.add_argument("--late", action="store_true", help="inserts use a past date (late arrivals)")
    ap.add_argument("--late-date", type=int, default=None, help="force inserts to use a specific late YYYYMMDD date")

    ap.add_argument(
        "--marker",
        type=str,
        default=None,
        help="insert a marker row with source_actor='__batch_<id>__' after workload",
    )
    ap.add_argument("--marker-date", type=int, default=None, help="YYYYMMDD date for marker row (defaults to base date)")

    args = ap.parse_args()

    def _resolve_count(val: int) -> int:
        if val == -1:
            if args.n is None:
                raise SystemExit("error: you used --insert/--update/--delete without a value; pass --n <int> too")
            return args.n
        return val

    ins = _resolve_count(args.insert)
    upd = _resolve_count(args.update)
    dele = _resolve_count(args.delete)

    conn = connect()
    try:
        base_date = int(args.base_date) if args.base_date is not None else (get_max_event_date(conn) or today_int())
        insert_date = resolve_insert_date(args.late, base_date, args.late_date)

        if ins:
            insert_events(conn, ins, event_date=insert_date)
            if args.late:
                print(f"inserted {ins} rows (late={insert_date})")
            else:
                print(f"inserted {ins} rows (date={insert_date})")

        if upd:
            update_events(conn, upd)
            print(f"updated {upd} rows")

        if dele:
            delete_events(conn, dele)
            print(f"deleted {dele} rows")

        if args.marker:
            marker_date = int(args.marker_date) if args.marker_date is not None else insert_date
            insert_marker(conn, marker_actor=args.marker, marker_date=marker_date)
            print(f"marker inserted: {args.marker} date={marker_date}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
