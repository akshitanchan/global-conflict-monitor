#!/usr/bin/env python3
"""
UPDATE and DELETE operations on real event IDs
(Use bash script for INSERT - it's faster)
"""
import os
import sys
import random
import psycopg2
from psycopg2.extras import execute_batch

# Database config
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "gdelt")
DB_USER = os.getenv("DB_USER", "flink_user")
DB_PASS = os.getenv("DB_PASS", "flink_pass")


def get_conn():
    """Get database connection"""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )


def update_real_events(num_rows):
    """
    Update REAL existing events by their actual globaleventid
    Updates goldstein and num_events to random values
    """
    print(f"[update] updating {num_rows} real events")
    
    conn = get_conn()
    cur = conn.cursor()
    
    try:
        # Check table size
        cur.execute("SELECT COUNT(*) FROM public.gdelt_events")
        total = cur.fetchone()[0]
        
        if total == 0:
            print("[update] table is empty, nothing to update")
            return
        
        if num_rows > total:
            num_rows = total
            print(f"[update] adjusting to {num_rows} (table size)")
        
        # Get random REAL event IDs from the table
        print(f"[update] selecting {num_rows} random event IDs...")
        cur.execute("""
            SELECT globaleventid 
            FROM public.gdelt_events 
            ORDER BY RANDOM()
            LIMIT %s
        """, (num_rows,))
        
        event_ids = [row[0] for row in cur.fetchall()]
        
        if not event_ids:
            print("[update] no events found to update")
            return
        
        print(f"[update] updating {len(event_ids)} events...")
        
        # Update each event with random values
        updates = []
        for event_id in event_ids:
            new_goldstein = round(random.uniform(-10, 10), 2)
            new_num_events = random.randint(1, 3)
            updates.append((new_goldstein, new_num_events, event_id))
        
        # Batch update
        execute_batch(cur, """
            UPDATE public.gdelt_events 
            SET goldstein = %s, num_events = %s 
            WHERE globaleventid = %s
        """, updates, page_size=1000)
        
        conn.commit()
        print(f"[update] done: {len(event_ids)} events updated")
        
        # Show sample of what was updated
        if event_ids:
            print(f"[update] sample event IDs: {event_ids[:5]}")
    
    except Exception as e:
        conn.rollback()
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


def delete_real_events(num_rows):
    """
    Delete REAL events by their actual globaleventid
    Deletes random events from the table
    """
    print(f"[delete] deleting {num_rows} real events")
    
    conn = get_conn()
    cur = conn.cursor()
    
    try:
        # Check table size
        cur.execute("SELECT COUNT(*) FROM public.gdelt_events")
        total = cur.fetchone()[0]
        
        if total == 0:
            print("[delete] table is empty, nothing to delete")
            return
        
        if num_rows > total:
            num_rows = total
            print(f"[delete] adjusting to {num_rows} (table size)")
        
        # Get random REAL event IDs
        print(f"[delete] selecting {num_rows} random event IDs to delete...")
        cur.execute("""
            SELECT globaleventid 
            FROM public.gdelt_events 
            ORDER BY RANDOM()
            LIMIT %s
        """, (num_rows,))
        
        event_ids = [row[0] for row in cur.fetchall()]
        
        if not event_ids:
            print("[delete] no events found to delete")
            return
        
        print(f"[delete] deleting {len(event_ids)} events...")
        print(f"[delete] sample event IDs: {event_ids[:5]}")
        
        # Delete by IDs
        cur.execute("""
            DELETE FROM public.gdelt_events 
            WHERE globaleventid = ANY(%s)
        """, (event_ids,))
        
        deleted = cur.rowcount
        conn.commit()
        
        print(f"[delete] done: {deleted} events deleted")
    
    except Exception as e:
        conn.rollback()
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python workload.py update --rows N")
        print("  python workload.py delete --rows N")
        print("")
        print("For INSERT: Use the bash script - it's much faster!")
        print("  SMALL_LOAD_LINES=20000 ./scripts/load-gdelt-copy.sh data/file.txt")
        sys.exit(1)
    
    cmd = sys.argv[1]
    
    if cmd == "update":
        rows = None
        
        i = 2
        while i < len(sys.argv):
            if sys.argv[i] == "--rows":
                rows = int(sys.argv[i+1])
                i += 2
            else:
                i += 1
        
        if rows is None:
            print("ERROR: --rows required")
            sys.exit(1)
        
        update_real_events(rows)
    
    elif cmd == "delete":
        rows = None
        
        i = 2
        while i < len(sys.argv):
            if sys.argv[i] == "--rows":
                rows = int(sys.argv[i+1])
                i += 2
            else:
                i += 1
        
        if rows is None:
            print("ERROR: --rows required")
            sys.exit(1)
        
        delete_real_events(rows)
    
    else:
        print(f"ERROR: unknown command: {cmd}")
        print("Use 'update' or 'delete'")
        print("For INSERT, use the bash script instead - it's faster!")
        sys.exit(1)


if __name__ == "__main__":
    main()
