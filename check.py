import sqlite3
conn = sqlite3.connect('data/nepse.db')

tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
print("Tables:", [t[0] for t in tables])

try:
    rows = conn.execute("SELECT * FROM clean_price_history WHERE symbol='NABIL' LIMIT 5").fetchall()
    print("clean_price_history rows for NABIL:", len(rows))
except Exception as e:
    print("clean_price_history error:", e)

try:
    rows = conn.execute("SELECT * FROM price_history WHERE symbol='NABIL' LIMIT 5").fetchall()
    print("price_history rows for NABIL:", len(rows))
except Exception as e:
    print("price_history error:", e)

