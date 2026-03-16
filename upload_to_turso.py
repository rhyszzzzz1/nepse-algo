"""
upload_to_turso.py
------------------
Uploads the local nepse.db to Turso, EXCLUDING the raw floor_sheet table
(18.9M rows, ~3 GB) since it's only needed for pipeline runs — not for
the API / signals / backtester which is what Turso serves.

All derived tables are preserved:
  broker_summary, daily_price, price_history, clean_price_history,
  signals, nepse_signals, optimizer_*, backtest_*, companies,
  instruments, sector_index, market_summary, trading_rules, etc.

How it works:
  1. Opens local sqlite (read-only)
  2. For each table (except floor_sheet), recreates schema on Turso
     and batch-inserts rows
Usage:
  python upload_to_turso.py
"""

import os, sqlite3, sys, time
import libsql
from dotenv import load_dotenv

load_dotenv()

LOCAL_DB  = r"c:\nepse-algo\data\nepse.db"
URL   = os.environ.get("TURSO_DATABASE_URL")
TOKEN = os.environ.get("TURSO_AUTH_TOKEN")

# Tables to SKIP uploading (too large / only needed for local pipeline)
SKIP_TABLES = {"floor_sheet", "sqlite_sequence"}

BATCH_SIZE = 500   # rows per INSERT batch

# ── helpers ──────────────────────────────────────────────────────────────────

def get_tables(conn):
    return [r[0] for r in
            conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]

def get_create_sql(conn, table):
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row[0] if row else None

def get_indexes(conn, table):
    return conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL",
        (table,)
    ).fetchall()

def drop_table_turso(tconn, table):
    try:
        tconn.execute(f'DROP TABLE IF EXISTS "{table}"')
        tconn.commit()
    except Exception as e:
        print(f"  [warn] drop {table}: {e}")

def upload_table(src_conn, tconn, table):
    create_sql = get_create_sql(src_conn, table)
    if not create_sql:
        print(f"  [skip] no DDL for {table}")
        return 0

    # Drop & recreate
    drop_table_turso(tconn, table)
    try:
        tconn.execute(create_sql)
        tconn.commit()
    except Exception as e:
        print(f"  [error] create {table}: {e}")
        return 0

    # Count rows
    total = src_conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    if total == 0:
        print(f"  {table}: 0 rows (skipped insert)")
        return 0

    # Fetch column names
    cols = [r[1] for r in src_conn.execute(f'PRAGMA table_info("{table}")').fetchall()]
    placeholders = ", ".join(["?" for _ in cols])
    col_list = ", ".join([f'"{c}"' for c in cols])
    insert_sql = f'INSERT OR IGNORE INTO "{table}" ({col_list}) VALUES ({placeholders})'

    inserted = 0
    batch = []
    cur = src_conn.execute(f'SELECT * FROM "{table}"')
    t0 = time.time()

    while True:
        rows = cur.fetchmany(BATCH_SIZE)
        if not rows:
            break
        batch = [tuple(r) for r in rows]
        try:
            tconn.executemany(insert_sql, batch)
            tconn.commit()
        except Exception as e:
            print(f"  [error] inserting into {table}: {e}")
            # Try row by row
            for row in batch:
                try:
                    tconn.execute(insert_sql, row)
                    tconn.commit()
                    inserted += 1
                except Exception as e2:
                    pass  # skip bad rows silently
            continue
        inserted += len(batch)
        elapsed = time.time() - t0
        pct = inserted / total * 100
        rate = inserted / elapsed if elapsed > 0 else 0
        print(f"  {table}: {inserted:>8,}/{total:,}  ({pct:.1f}%)  {rate:.0f} rows/s", end="\r")

    elapsed = time.time() - t0
    print(f"  {table}: {inserted:,} rows uploaded in {elapsed:.1f}s{' '*20}")

    # Recreate indexes
    for (idx_sql,) in get_indexes(src_conn, table):
        try:
            tconn.execute(idx_sql)
            tconn.commit()
        except Exception as e:
            print(f"  [warn] index on {table}: {e}")

    return inserted

# ── main ─────────────────────────────────────────────────────────────────────

def main():
    if not URL or not TOKEN:
        print("ERROR: TURSO_DATABASE_URL / TURSO_AUTH_TOKEN not set in .env")
        sys.exit(1)

    print(f"Connecting to local DB: {LOCAL_DB}")
    src = sqlite3.connect(LOCAL_DB)

    print(f"Connecting to Turso:    {URL}\n")
    tconn = libsql.connect(URL, auth_token=TOKEN)

    tables = get_tables(src)
    todo   = [t for t in tables if t not in SKIP_TABLES]

    print(f"Tables to upload : {len(todo)}")
    print(f"Skipping         : {SKIP_TABLES}\n")

    grand_total = 0
    for i, table in enumerate(todo, 1):
        print(f"[{i}/{len(todo)}] {table}")
        n = upload_table(src, tconn, table)
        grand_total += n

    src.close()
    tconn.close()

    print(f"\nDone! {grand_total:,} total rows uploaded to Turso.")
    print("(floor_sheet kept local-only for pipeline re-runs)")

if __name__ == "__main__":
    main()
