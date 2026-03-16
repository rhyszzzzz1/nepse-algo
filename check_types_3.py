import sqlite3

conn = sqlite3.connect("data/nepse_upload.db")

for t in ['signals', 'trade_history', 'backtest_results']:
    cols = conn.execute(f"PRAGMA table_info({t})").fetchall()
    print(f"\nTable: {t}")
    for c in cols:
        col_name = c[1]
        try:
            types = conn.execute(f"SELECT typeof({col_name}), count(1) FROM {t} GROUP BY typeof({col_name})").fetchall()
            print(f"  {col_name}: {types}")
        except Exception as e:
            print(f"  Error on {col_name}: {e}")

conn.close()
