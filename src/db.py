import os
import sqlite3

try:
    import sys as _sys
    _sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from config import DB_PATH
except ImportError:
    # fallback
    DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "nepse.db")

DATABASE_URL = os.environ.get("DATABASE_URL")

class PgCursor:
    def __init__(self, cur):
        self._cur = cur
        self.description = cur.description
    def fetchall(self):
        return self._cur.fetchall()
    def fetchone(self):
        return self._cur.fetchone()
    def fetchmany(self, size):
        return self._cur.fetchmany(size)
    @property
    def lastrowid(self):
        return getattr(self, "_lastrowid", None)
    def __iter__(self):
        return iter(self._cur)

class PgConnection:
    def __init__(self, conn):
        self._conn = conn
        self.row_factory = None

    def cursor(self):
        if self.row_factory:
            import psycopg2.extras
            return PgCursor(self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor))
        return PgCursor(self._conn.cursor())

    def execute(self, sql, params=()):
        if self.row_factory:
            import psycopg2.extras
            cur = self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        else:
            cur = self._conn.cursor()

        # Translate SQLite syntax to PostgreSQL
        pg_sql = sql.replace("?", "%s")
        pg_sql = pg_sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
        
        if "sqlite_master" in pg_sql:
            pg_sql = pg_sql.replace("sqlite_master WHERE type='table'", "pg_tables WHERE schemaname='public'")
            pg_sql = pg_sql.replace("name FROM", "tablename FROM")
        
        is_insert = pg_sql.strip().upper().startswith("INSERT")
        if is_insert and "RETURNING" not in pg_sql.upper():
            pg_sql = pg_sql.rstrip("; \n\t") + " RETURNING id"

        cur.execute(pg_sql, params)
        
        wrapper = PgCursor(cur)
        if is_insert and cur.description:
            wrapper.description = cur.description
            row = cur.fetchone()
            if row:
                wrapper._lastrowid = row[0]
                
        return wrapper

    def executemany(self, sql, seq_of_parameters):
        if self.row_factory:
            import psycopg2.extras
            cur = self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        else:
            cur = self._conn.cursor()

        pg_sql = sql.replace("?", "%s")
        # Optimization: executemany is rare here, a loop is fine for compatibility
        for params in seq_of_parameters:
            cur.execute(pg_sql, params)
        return PgCursor(cur)

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()

def get_db():
    if DATABASE_URL:
        import psycopg2
        return PgConnection(psycopg2.connect(DATABASE_URL))
    else:
        # SQLite local fallback
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        return sqlite3.connect(DB_PATH)
