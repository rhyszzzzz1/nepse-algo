"""
make_slim_db.py
---------------
Creates a slim SQLite database (slim_nepse.db) from the local nepse.db,
excluding floor_sheet (18.9M rows) and also dropping fetched_at columns
from all tables to reduce size further.

This slim file is then uploaded to Turso via:
  wsl turso db create nepsedb2 --from-file /mnt/c/nepse-algo/data/slim_nepse.db
"""

import sqlite3
import os
import shutil
import time

SRC_DB  = r"c:\nepse-algo\data\nepse.db"
SLIM_DB = r"c:\nepse-algo\data\slim_nepse.db"

# Tables to completely skip
SKIP_TABLES = {"floor_sheet", "sqlite_sequence"}

# Columns to DROP from tables (wasted metadata, not used by API)
DROP_COLS = {"fetched_at", "cleaned_at", "calculated_at", "signal_date"}

def slim_table_cols(conn, table):
    """Return column names for a table, excluding DROP_COLS."""
    all_cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{table}")').fetchall()]
    return [c for c in all_cols if c.lower() not in DROP_COLS]

def make_create_sql(src_conn, table, keep_cols):
    """Build CREATE TABLE SQL using only keep_cols."""
    # Get original DDL
    orig_sql = src_conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()[0]
    
    # If no cols were dropped, return as-is
    orig_cols = [r[1] for r in src_conn.execute(f'PRAGMA table_info("{table}")').fetchall()]
    if set(orig_cols) == set(keep_cols):
        return orig_sql
    
    # Parse column definitions from DDL
    lines = orig_sql.split("\n")
    header = lines[0]  # CREATE TABLE "name" (
    
    col_info = {r[1]: r for r in src_conn.execute(f'PRAGMA table_info("{table}")').fetchall()}
    
    # Rebuild from column info (simpler than parsing DDL)
    col_defs = []
    for col in keep_cols:
        if col not in col_info:
            continue
        info = col_info[col]
        # info: (cid, name, type, notnull, dflt_value, pk)
        cid, name, typ, notnull, dflt, pk = info
        defn = f'    "{name}" {typ}'
        if notnull:
            defn += " NOT NULL"
        if dflt is not None:
            defn += f" DEFAULT {dflt}"
        col_defs.append(defn)
    
    # Find primary key cols
    pks = [(r[5], r[1]) for r in src_conn.execute(f'PRAGMA table_info("{table}")').fetchall()
           if r[5] > 0 and r[1] in keep_cols]
    pks.sort()
    if len(pks) > 1:  # composite PK
        pk_cols = ", ".join([f'"{p[1]}"' for p in pks])
        col_defs.append(f"    PRIMARY KEY ({pk_cols})")
    
    body = ",\n".join(col_defs)
    return f'CREATE TABLE IF NOT EXISTS "{table}" (\n{body}\n)'

def copy_table(src_conn, dst_conn, table):
    keep_cols = slim_table_cols(src_conn, table)
    
    # Create table in destination
    create_sql = make_create_sql(src_conn, table, keep_cols)
    try:
        dst_conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        dst_conn.execute(create_sql)
        dst_conn.commit()
    except Exception as e:
        print(f"  [error] CREATE {table}: {e}")
        print(f"  SQL: {create_sql[:200]}")
        return 0

    # Count source rows
    total = src_conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    if total == 0:
        print(f"  {table}: 0 rows")
        return 0

    # Copy data in chunks
    col_str = ", ".join([f'"{c}"' for c in keep_cols])
    placeholders = ", ".join(["?" for _ in keep_cols])
    insert_sql = f'INSERT OR IGNORE INTO "{table}" ({col_str}) VALUES ({placeholders})'
    
    CHUNK = 10000
    inserted = 0
    t0 = time.time()
    cur = src_conn.execute(f'SELECT {col_str} FROM "{table}"')
    
    while True:
        rows = cur.fetchmany(CHUNK)
        if not rows:
            break
        dst_conn.executemany(insert_sql, rows)
        dst_conn.commit()
        inserted += len(rows)
    
    elapsed = time.time() - t0
    dropped = [c for c in [r[1] for r in src_conn.execute(f'PRAGMA table_info("{table}")').fetchall()] 
               if c.lower() in DROP_COLS]
    drop_note = f" [dropped cols: {dropped}]" if dropped else ""
    print(f"  {table}: {inserted:>10,} rows  {elapsed:5.1f}s{drop_note}")

    # Copy indexes
    for (idx_sql,) in src_conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL",
        (table,)
    ).fetchall():
        # Only keep index if all its columns still exist
        try:
            dst_conn.execute(idx_sql)
            dst_conn.commit()
        except:
            pass

    return inserted


def main():
    if os.path.exists(SLIM_DB):
        os.remove(SLIM_DB)
        print(f"Removed old {SLIM_DB}")

    print(f"Source : {SRC_DB}  ({os.path.getsize(SRC_DB)/1e9:.2f} GB)")
    print(f"Target : {SLIM_DB}\n")

    src = sqlite3.connect(SRC_DB)
    dst = sqlite3.connect(SLIM_DB)
    dst.execute("PRAGMA journal_mode=WAL")
    dst.execute("PRAGMA synchronous=NORMAL")
    dst.execute("PRAGMA cache_size=-524288")  # 512 MB cache

    tables = [r[0] for r in src.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()]

    todo = [t for t in tables if t not in SKIP_TABLES]
    print(f"Copying {len(todo)} tables (skipping: {SKIP_TABLES})\n")

    grand = 0
    t_start = time.time()
    for i, table in enumerate(todo, 1):
        print(f"[{i}/{len(todo)}] {table}")
        n = copy_table(src, dst, table)
        grand += n

    # Final VACUUM to minimize file size
    print("\nVACUUMing slim DB to minimize size...")
    t_vac = time.time()
    dst.execute("VACUUM")
    print(f"VACUUM done in {time.time()-t_vac:.1f}s")

    src.close()
    dst.close()

    slim_size = os.path.getsize(SLIM_DB)
    orig_size = os.path.getsize(SRC_DB)
    elapsed = time.time() - t_start
    
    print(f"\n{'='*60}")
    print(f"Original DB : {orig_size/1e9:.2f} GB")
    print(f"Slim DB     : {slim_size/1e6:.1f} MB  ({slim_size/1e9:.3f} GB)")
    print(f"Reduction   : {(1 - slim_size/orig_size)*100:.1f}%")
    print(f"Total rows  : {grand:,}")
    print(f"Time        : {elapsed:.1f}s")
    print(f"\nNext step - run in PowerShell:")
    print(f'  wsl -e bash -ilc "turso db destroy nepsedb --yes && turso db create nepsedb --from-file /mnt/c/nepse-algo/data/slim_nepse.db"')

if __name__ == "__main__":
    main()
