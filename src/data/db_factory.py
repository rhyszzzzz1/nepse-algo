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
            
            # libsql does not support row_factory directly, so we emulate it
            class RowFactoryCursor:
                def __init__(self, raw_cursor, row_factory):
                    self.raw_cursor = raw_cursor
                    self.row_factory = row_factory

                def execute(self, sql, parameters=()):
                    self.raw_cursor.execute(sql, parameters)
                    return self

                def executemany(self, sql, seq_of_parameters):
                    self.raw_cursor.executemany(sql, seq_of_parameters)
                    return self

                def executescript(self, sql_script):
                    self.raw_cursor.executescript(sql_script)
                    return self

                def fetchone(self):
                    row = self.raw_cursor.fetchone()
                    if row is None:
                        return None
                    return self.row_factory(self.raw_cursor, row)

                def fetchall(self):
                    rows = self.raw_cursor.fetchall()
                    return [self.row_factory(self.raw_cursor, r) for r in rows]

                def fetchmany(self, size=None):
                    rows = self.raw_cursor.fetchmany(size if size is not None else self.arraysize)
                    return [self.row_factory(self.raw_cursor, r) for r in rows]

                @property
                def description(self):
                    return self.raw_cursor.description

                @property
                def lastrowid(self):
                    return self.raw_cursor.lastrowid

                @property
                def rowcount(self):
                    return self.raw_cursor.rowcount

                @property
                def arraysize(self):
                    return self.raw_cursor.arraysize

                @arraysize.setter
                def arraysize(self, value):
                    self.raw_cursor.arraysize = value

                def close(self):
                    self.raw_cursor.close()

                def __iter__(self):
                    while True:
                        row = self.fetchone()
                        if row is None:
                            break
                        yield row

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc_val, exc_tb):
                    self.close()

            class RowFactoryConnection:
                def __init__(self, raw_conn, row_factory=None):
                    self.raw_conn = raw_conn
                    self.row_factory = row_factory

                def cursor(self):
                    cur = self.raw_conn.cursor()
                    if self.row_factory:
                        return RowFactoryCursor(cur, self.row_factory)
                    return cur

                def execute(self, sql, parameters=()):
                    cur = self.cursor()
                    return cur.execute(sql, parameters)

                def executemany(self, sql, seq_of_parameters):
                    cur = self.cursor()
                    return cur.executemany(sql, seq_of_parameters)

                def executescript(self, sql_script):
                    cur = self.cursor()
                    return cur.executescript(sql_script)

                def commit(self):
                    self.raw_conn.commit()

                def rollback(self):
                    self.raw_conn.rollback()

                def close(self):
                    self.raw_conn.close()

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc_val, exc_tb):
                    if exc_type is None:
                        self.commit()
                    else:
                        self.rollback()
                    self.close()

            class DictRow:
                def __init__(self, cursor, row):
                    self._row = row
                    self._description = cursor.description
                    if self._description:
                        self._keys = [col[0].lower() for col in self._description]
                        self._orig_keys = [col[0] for col in self._description]
                    else:
                        self._keys = []
                        self._orig_keys = []

                def keys(self):
                    return self._orig_keys

                def __getitem__(self, item):
                    if isinstance(item, int):
                        return self._row[item]
                    elif isinstance(item, str):
                        try:
                            idx = self._keys.index(item.lower())
                            return self._row[idx]
                        except ValueError:
                            raise KeyError(item)
                    else:
                        raise TypeError("Index must be int or string")
                        
                def __iter__(self):
                    return iter(self._row)
                    
                def __len__(self):
                    return len(self._row)

            raw_conn = libsql.connect(url, auth_token=token)
            conn = RowFactoryConnection(raw_conn, row_factory=DictRow)
            conn.row_factory = DictRow
            return conn
            
        except ImportError:
            try:
                import libsql_client

                class DictRow:
                    def __init__(self, cursor, row):
                        self._row = row
                        self._description = cursor.description
                        if self._description:
                            self._keys = [col[0].lower() for col in self._description]
                            self._orig_keys = [col[0] for col in self._description]
                        else:
                            self._keys = []
                            self._orig_keys = []

                    def keys(self):
                        return self._orig_keys

                    def __getitem__(self, item):
                        if isinstance(item, int):
                            return self._row[item]
                        if isinstance(item, str):
                            try:
                                idx = self._keys.index(item.lower())
                                return self._row[idx]
                            except ValueError:
                                raise KeyError(item)
                        raise TypeError("Index must be int or string")

                    def __iter__(self):
                        return iter(self._row)

                    def __len__(self):
                        return len(self._row)

                class ResultCursor:
                    def __init__(self, result_set, row_factory=None):
                        self._rows = list(result_set.rows or [])
                        self._index = 0
                        self._row_factory = row_factory
                        self.description = [(c,) for c in (result_set.columns or [])]

                    def _convert(self, row):
                        if self._row_factory:
                            return self._row_factory(self, row)
                        return row

                    def fetchone(self):
                        if self._index >= len(self._rows):
                            return None
                        row = self._rows[self._index]
                        self._index += 1
                        return self._convert(row)

                    def fetchall(self):
                        if self._index >= len(self._rows):
                            return []
                        out = [self._convert(r) for r in self._rows[self._index:]]
                        self._index = len(self._rows)
                        return out

                class ClientConnection:
                    def __init__(self, client, row_factory=None):
                        self.client = client
                        self.row_factory = row_factory

                    def execute(self, sql, parameters=()):
                        stmt = (sql, list(parameters)) if parameters else sql
                        rs = self.client.execute(stmt)
                        return ResultCursor(rs, self.row_factory)

                    def executemany(self, sql, seq_of_parameters):
                        # Keep semantics simple for API reads/writes.
                        last = None
                        for params in seq_of_parameters:
                            last = self.execute(sql, params)
                        return last

                    def commit(self):
                        return None

                    def rollback(self):
                        return None

                    def close(self):
                        self.client.close()

                    def __enter__(self):
                        return self

                    def __exit__(self, exc_type, exc_val, exc_tb):
                        self.close()

                client_url = url.replace("libsql://", "https://")
                client = libsql_client.create_client_sync(client_url, auth_token=token)
                return ClientConnection(client, row_factory=DictRow)

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
