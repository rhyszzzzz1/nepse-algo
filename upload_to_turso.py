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

import math, os, sqlite3, sys, time
import libsql_client
from dotenv import load_dotenv

load_dotenv()

LOCAL_DB  = r"c:\nepse-algo\data\nepse.db"
URL   = os.environ.get("TURSO_DATABASE_URL")
TOKEN = os.environ.get("TURSO_AUTH_TOKEN")

# Tables to SKIP uploading (too large / only needed for local pipeline)
SKIP_TABLES = {"floor_sheet", "sqlite_sequence"}

BATCH_SIZE = 300   # rows per INSERT batch

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

# ── Turso client helpers ─────────────────────────────────────────────────────

def _sanitize_value(v):
    if isinstance(v, float) and not math.isfinite(v):
        return None
    if isinstance(v, bool):
        return 1 if v else 0
    return v


class TursoConn:
    """libsql sync client wrapper used by this uploader."""
    def __init__(self, url, token):
        # libsql_client supports http/https/ws/wss. Convert libsql:// accordingly.
        client_url = url.replace("libsql://", "https://")
        self.client = libsql_client.create_client_sync(client_url, auth_token=token)

    def execute(self, sql, params=None):
        if params:
            self.client.execute((sql, list(params)))
        else:
            self.client.execute(sql)

    def insert_many(self, table, col_list, rows):
        # Build one INSERT statement with multiple VALUES tuples to reduce round trips.
        row_placeholders = "(" + ", ".join(["?"] * len(col_list)) + ")"
        values_sql = ", ".join([row_placeholders] * len(rows))
        cols_sql = ", ".join([f'"{c}"' for c in col_list])
        sql = f'INSERT OR IGNORE INTO "{table}" ({cols_sql}) VALUES {values_sql}'

        flat_params = []
        for row in rows:
            flat_params.extend(_sanitize_value(v) for v in row)

        self.execute(sql, flat_params)

    def commit(self):
        pass  # autocommit mode

    def close(self):
        self.client.close()


def drop_table_turso(tconn, table):
    try:
        tconn.execute(f'DROP TABLE IF EXISTS "{table}"')
    except Exception as e:
        print(f"  [warn] drop {table}: {e}")

def insert_rows(tconn, table, col_list, rows):
    if not rows:
        return 0

    try:
        tconn.insert_many(table, col_list, rows)
        tconn.commit()
        return len(rows)
    except Exception as e:
        if len(rows) == 1:
            print(f"  [warn] skipping 1 row in {table}: {e}")
            return 0

        mid = len(rows) // 2
        left = insert_rows(tconn, table, col_list, rows[:mid])
        right = insert_rows(tconn, table, col_list, rows[mid:])
        return left + right

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
    inserted = 0
    batch = []
    cur = src_conn.execute(f'SELECT * FROM "{table}"')
    t0 = time.time()

    while True:
        rows = cur.fetchmany(BATCH_SIZE)
        if not rows:
            break
        batch = [tuple(r) for r in rows]
        inserted += insert_rows(tconn, table, cols, batch)
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
    tconn = TursoConn(URL, TOKEN)

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
