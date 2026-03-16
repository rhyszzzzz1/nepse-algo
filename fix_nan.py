import sqlite3

conn = sqlite3.connect("data/nepse_upload.db")
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()

for (table_name,) in tables:
    if table_name == 'sqlite_sequence': continue
    cols = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    
    for c in cols:
        col_name = c[1]
        col_type = c[2].upper()
        
        if 'REAL' in col_type or 'INT' in col_type:
            # Check for NaN (x != x) or Infinity
            # Actually, in SQLite, NaN is not equal to itself.
            try:
                nan_count = conn.execute(f"SELECT count(*) FROM {table_name} WHERE typeof({col_name})='real' AND {col_name} IS NOT NULL AND {col_name} != {col_name}").fetchone()[0]
                if nan_count > 0:
                    print(f"Table {table_name}, Column {col_name} has {nan_count} literal NaN values! FIXING...")
                    conn.execute(f"UPDATE {table_name} SET {col_name} = 0.0 WHERE typeof({col_name})='real' AND {col_name} != {col_name}")
                    conn.commit()
            except Exception as e:
                pass

        # Also check if instead of SQL NULL, the literal string "null"، "NaN", "None" was saved
        try:
            str_count = conn.execute(f"SELECT count(*) FROM {table_name} WHERE typeof({col_name})='text' AND {col_name} IN ('NaN', 'nan', 'None', 'null', 'NULL', '')").fetchone()[0]
            if str_count > 0:
                print(f"Table {table_name}, Column {col_name} has {str_count} string '{col_name}' literal bugs! FIXING...")
                conn.execute(f"UPDATE {table_name} SET {col_name} = 0 WHERE typeof({col_name})='text' AND {col_name} IN ('NaN', 'nan', 'None', 'null', 'NULL', '')")
                conn.commit()
        except:
            pass

print("Done scanning/fixing anomalies.")
conn.close()
