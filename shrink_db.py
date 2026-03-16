import sqlite3
import datetime
import os
import shutil

DB_PATH = "data/nepse.db"
# Changed filename to bypass the WinError 32 file lock
SLIM_DB_PATH = "data/nepse_upload.db"

def create_slim_db():
    print(f"Original DB Size: {os.path.getsize(DB_PATH) / (1024*1024):.2f} MB")
    
    # Let's keep data from 2023 onwards to give you more rows!
    cutoff_date = "2023-01-01"
    print(f"Cutoff Date: {cutoff_date}")
    
    conn = sqlite3.connect(DB_PATH)
    
    total_rows = conn.execute("SELECT count(*) FROM floor_sheet").fetchone()[0]
    keep_rows = conn.execute("SELECT count(*) FROM floor_sheet WHERE date >= ?", (cutoff_date,)).fetchone()[0]
    
    print(f"Total floor_sheet rows: {total_rows}")
    print(f"Rows to keep (since {cutoff_date}): {keep_rows}")
    print(f"Rows to delete: {total_rows - keep_rows}")
    
    conn.close()
    
    # Copy DB
    print(f"\nCreating a copy at {SLIM_DB_PATH}...")
    if os.path.exists(SLIM_DB_PATH):
        try:
            os.remove(SLIM_DB_PATH)
        except OSError:
            pass
            
    shutil.copy2(DB_PATH, SLIM_DB_PATH)
    
    # Connect to slim db and delete old rows
    print("Deleting old rows in slim DB...")
    slim_conn = sqlite3.connect(SLIM_DB_PATH)
    
    # Optimizing the delete transaction
    slim_conn.execute("BEGIN TRANSACTION")
    slim_conn.execute("DELETE FROM floor_sheet WHERE date < ?", (cutoff_date,))
    slim_conn.execute("COMMIT")
    
    # Vacuum to reclaim space
    print("Vacuuming database to reclaim disk space (this might take a few minutes for 3GB)...")
    slim_conn.execute("VACUUM")
    slim_conn.close()
    
    print(f"\nSuccess! Upload DB Size: {os.path.getsize(SLIM_DB_PATH) / (1024*1024):.2f} MB")
    print("Next step: Run 'turso db create nepse-db --from-file data/nepse_upload.db'")

if __name__ == "__main__":
    create_slim_db()
