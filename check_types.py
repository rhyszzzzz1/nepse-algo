import sqlite3

conn = sqlite3.connect("data/nepse_upload.db")
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()

print("Scanning tables for type mismatches (NaNs, strings in numeric columns)...")

for (table_name,) in tables:
    if table_name == 'sqlite_sequence': continue
    cols = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    
    print(f"\nTable: {table_name}")
    for c in cols:
        col_name = c[1]
        col_type = c[2].upper()
        
        # Check actual types stored in SQLite
        query = f"SELECT typeof({col_name}), count(1) FROM {table_name} GROUP BY typeof({col_name})"
        types = conn.execute(query).fetchall()
        
        print(f"  {col_name} ({col_type}): {types}")
        
        # In pandas 'NaN' is often stored as the float 'NaN'. Let's see if we have inf/nan
        if 'REAL' in col_type or 'INT' in col_type:
            nan_count = conn.execute(f"SELECT count(1) FROM {table_name} WHERE typeof({col_name}) = 'real' AND ({col_name} > 1e300 OR {col_name} < -1e300 OR {col_name} != {col_name})").fetchone()[0]
            if nan_count > 0:
                print(f"    !!! WARNING: Found {nan_count} NaN/Inf values in {col_name} !!!")

conn.close()
