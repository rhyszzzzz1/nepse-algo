# src/db.py
# Simple SQLite connection — no PostgreSQL
import sqlite3
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if "RAILWAY_VOLUME_MOUNT_PATH" in os.environ:
    DB_PATH = os.path.join(os.environ["RAILWAY_VOLUME_MOUNT_PATH"], "nepse.db")
else:
    DB_PATH = os.path.join(ROOT, "data", "nepse.db")

try:
    from src.data.db_factory import get_db_connection
except ImportError:
    try:
        from data.db_factory import get_db_connection
    except ImportError:
        from db_factory import get_db_connection

def get_db():
    return get_db_connection(DB_PATH)
