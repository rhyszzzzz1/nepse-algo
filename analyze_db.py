import sqlite3

DB_PATH = r"c:\nepse-algo\data\nepse.db"
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = [row[0] for row in cursor.fetchall()]

print(f"Total tables: {len(tables)}\n")
print(f"{'Table':<45} {'Rows':>12}")
print("-" * 60)

total_rows = 0
for table in sorted(tables):
    try:
        cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
        count = cursor.fetchone()[0]
        total_rows += count
        print(f"{table:<45} {count:>12,}")
    except Exception as e:
        print(f"{table:<45} ERROR: {e}")

print("-" * 60)
print(f"{'TOTAL':<45} {total_rows:>12,}")

# Also show schema of floorsheets - biggest suspected table
print("\n--- Schema of tables ---")
for table in tables:
    cursor.execute(f'PRAGMA table_info("{table}")')
    cols = cursor.fetchall()
    print(f"\n{table}: {[c[1] for c in cols]}")

conn.close()
