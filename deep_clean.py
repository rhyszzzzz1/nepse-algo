import sqlite3
import math

DB_PATH = "data/nepse_upload.db"

def clean_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    tables = cursor.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    
    for (table,) in tables:
        if table == 'sqlite_sequence': continue
        
        print(f"Cleaning table: {table}")
        
        # Get column details
        cols = cursor.execute(f"PRAGMA table_info({table})").fetchall()
        numeric_cols = [c[1] for c in cols if 'INT' in c[2].upper() or 'REAL' in c[2].upper()]
        
        if not numeric_cols:
            continue
            
        primary_keys = [c[1] for c in cols if c[5] > 0]
        pk = primary_keys[0] if primary_keys else "rowid"
        
        # Fetch rows
        cursor.execute(f"SELECT {pk}, {', '.join(numeric_cols)} FROM {table}")
        rows = cursor.fetchall()
        
        updates = []
        for r in rows:
            pk_val = r[0]
            vals = r[1:]
            
            needs_update = False
            new_vals = []
            
            for v in vals:
                if isinstance(v, float):
                    if math.isnan(v) or math.isinf(v):
                        needs_update = True
                        new_vals.append(0.0)
                        continue
                new_vals.append(v)
                
            if needs_update:
                updates.append((new_vals, pk_val))
                
        if updates:
            print(f"  -> Fixing {len(updates)} rows with NaN/Inf values...")
            for new_vals, pk_val in updates:
                set_clause = ", ".join([f"{col} = ?" for col in numeric_cols])
                cursor.execute(f"UPDATE {table} SET {set_clause} WHERE {pk} = ?", (*new_vals, pk_val))
            conn.commit()
            print("  -> Fixed.")

    # Fix 'null' literal strings globally
    for (table,) in tables:
        if table == 'sqlite_sequence': continue
        cols = cursor.execute(f"PRAGMA table_info({table})").fetchall()
        text_cols = [c[1] for c in cols if 'TEXT' in c[2].upper()]
        
        for col in text_cols:
            cursor.execute(f"UPDATE {table} SET {col} = NULL WHERE {col} IN ('NaN', 'nan', 'None', 'null', 'NULL')")
            if cursor.rowcount > 0:
                print(f"  -> Fixed {cursor.rowcount} literal string nulls in {table}.{col}")
        conn.commit()

    conn.execute("VACUUM")
    conn.close()
    print("Database cleaned completely.")

if __name__ == "__main__":
    clean_database()
