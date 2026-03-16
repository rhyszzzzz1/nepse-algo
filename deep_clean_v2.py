import sqlite3

DB_PATH = "data/nepse_upload.db"

def clean_database():
    conn = sqlite3.connect(DB_PATH)
    
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    
    for (table,) in tables:
        if table == 'sqlite_sequence': continue
        print(f"Cleaning table: {table}")
        
        cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
        
        for c in cols:
            col_name = c[1]
            col_type = c[2].upper()
            
            # Fix actual NaNs in numeric columns by abusing the fact that NaN compared to any number is False
            if 'REAL' in col_type or 'INT' in col_type:
                # Replace literal NaNs
                conn.execute(f"""
                    UPDATE {table} 
                    SET {col_name} = 0 
                    WHERE typeof({col_name}) = 'real' 
                      AND NOT({col_name} < 0 OR {col_name} >= 0)
                """)
                
                # Replace NULLs in numeric fields with 0
                conn.execute(f"""
                    UPDATE {table} 
                    SET {col_name} = 0 
                    WHERE {col_name} IS NULL
                """)
            
            # Replace string exact literal "NaN", "None", "null" with actual SQLite NULL or 0
            if 'TEXT' in col_type:
                conn.execute(f"""
                    UPDATE {table} 
                    SET {col_name} = NULL 
                    WHERE {col_name} IN ('NaN', 'nan', 'None', 'null', 'NULL')
                """)
                
        conn.commit()

    conn.execute("VACUUM")
    conn.close()
    print("Database cleaned completely without memory overload.")

if __name__ == "__main__":
    clean_database()
