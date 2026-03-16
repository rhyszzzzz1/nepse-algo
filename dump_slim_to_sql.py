"""
dump_slim_to_sql.py
-------------------
Dumps slim_nepse.db to a SQL file that can be piped into:
  wsl turso db shell nepsedb < slim_nepse.sql
"""

import sqlite3
import os
import time

SLIM_DB  = r"c:\nepse-algo\data\slim_nepse.db"
SQL_FILE = r"c:\nepse-algo\data\slim_nepse.sql"

conn = sqlite3.connect(SLIM_DB)

print(f"Dumping {SLIM_DB} -> {SQL_FILE}")
t0 = time.time()

with open(SQL_FILE, "w", encoding="utf-8") as f:
    for line in conn.iterdump():
        f.write(line + "\n")

conn.close()

size = os.path.getsize(SQL_FILE)
elapsed = time.time() - t0
print(f"Done! {size/1e6:.1f} MB in {elapsed:.1f}s")
print(f"\nNext: run in PowerShell:")
print(f'  wsl -e bash -ilc "turso db create nepsedb --from-dump /mnt/c/nepse-algo/data/slim_nepse.sql"')
