# src/data/db_factory.py
import os
import sqlite3
import logging

logger = logging.getLogger(__name__)

def get_db_connection(db_path=None, timeout=60.0):
    """
    Returns a database connection. 
    If TURSO_DATABASE_URL and TURSO_AUTH_TOKEN are set, connects to Turso.
    Otherwise, falls back to local SQLite at db_path.
    """
    url = os.environ.get("TURSO_DATABASE_URL")
    token = os.environ.get("TURSO_AUTH_TOKEN")
    
    # Priority 1: Turso (Cloud)
    if url and token:
        try:
            import libsql
            conn = libsql.connect(url, auth_token=token)
            conn.row_factory = sqlite3.Row
            return conn
        except ImportError:
            logger.warning("libsql driver not installed. Falling back to local sqlite.")
        except Exception as e:
            logger.error(f"Failed to connect to Turso at {url}: {e}")
            # Fallback will continue below
            
    # Priority 2: Local SQLite
    if db_path is None:
        # Re-derive default path if not provided
        ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        db_path = os.path.join(ROOT, "data", "nepse.db")
        if "RAILWAY_VOLUME_MOUNT_PATH" in os.environ:
             db_path = os.path.join(os.environ["RAILWAY_VOLUME_MOUNT_PATH"], "nepse.db")

    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=timeout)
    
    # Enable WAL for local sqlite performance
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    except:
        pass
        
    conn.row_factory = sqlite3.Row
    return conn
