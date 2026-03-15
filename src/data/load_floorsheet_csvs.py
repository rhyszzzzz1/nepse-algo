# src/data/load_floorsheet_csvs.py
# Loads all nepse-data CSV files into floor_sheet table
# then computes broker accumulation metrics into broker_summary

import sqlite3
import os
import re
import csv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── CONFIG ────────────────────────────────────────────────────────────────────
ROOT       = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH    = os.path.join(ROOT, "data", "nepse.db")
CSV_DIR    = os.path.join(ROOT, "nepse-data", "data")

def get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=60.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


# ── ENSURE TABLES ─────────────────────────────────────────────────────────────
def ensure_tables():
    conn = get_db()

    # Migrate broker_summary if it exists with old schema (no 'broker' column)
    existing_cols = [r[1] for r in conn.execute("PRAGMA table_info(broker_summary)").fetchall()]
    if existing_cols and 'broker' not in existing_cols:
        print("Migrating broker_summary to new per-broker schema (old table had 0 rows)...")
        conn.execute("DROP TABLE IF EXISTS broker_summary")
        conn.commit()

    conn.execute("""
        CREATE TABLE IF NOT EXISTS floor_sheet (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            date          TEXT NOT NULL,
            symbol        TEXT NOT NULL,
            buyer_broker  INTEGER,
            seller_broker INTEGER,
            quantity      REAL,
            rate          REAL,
            amount        REAL,
            fetched_at    TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fs_date_sym ON floor_sheet(date, symbol)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fs_buyer    ON floor_sheet(date, buyer_broker)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fs_seller   ON floor_sheet(date, seller_broker)")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS broker_summary (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            date                 TEXT NOT NULL,
            symbol               TEXT NOT NULL,
            broker               INTEGER NOT NULL,
            buy_qty              REAL DEFAULT 0,
            sell_qty             REAL DEFAULT 0,
            buy_amount           REAL DEFAULT 0,
            sell_amount          REAL DEFAULT 0,
            net_qty              REAL DEFAULT 0,
            net_amount           REAL DEFAULT 0,
            trades_as_buyer      INTEGER DEFAULT 0,
            trades_as_seller     INTEGER DEFAULT 0,
            fetched_at           TEXT,
            UNIQUE(date, symbol, broker)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bs_sym_broker ON broker_summary(symbol, broker)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bs_date       ON broker_summary(date)")
    conn.commit()
    conn.close()
    print("Tables ready.")


# ── PARSE FILENAME → DATE ─────────────────────────────────────────────────────
def filename_to_date(fname):
    """Convert MM_DD_YYYY.csv -> YYYY-MM-DD"""
    name = os.path.splitext(fname)[0]   # e.g. "12_16_2020"
    m = re.match(r'^(\d{2})_(\d{2})_(\d{4})$', name)
    if not m:
        return None
    month, day, year = m.groups()
    return f"{year}-{month}-{day}"


# ── LOAD ONE CSV ──────────────────────────────────────────────────────────────
def load_csv(fpath, date_str):
    """
    Parse one floor sheet CSV and return list of row tuples.
    CSV columns: Transact. No. | Symbol | Buyer | Seller | Quantity | Rate | Amount
    """
    rows = []
    fetched_at = datetime.now().isoformat()
    with open(fpath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                symbol = str(row.get('Symbol', '')).strip().upper()
                if not symbol:
                    continue
                rows.append((
                    date_str,
                    symbol,
                    int(float(row.get('Buyer', 0) or 0)),
                    int(float(row.get('Seller', 0) or 0)),
                    float(str(row.get('Quantity', 0) or 0).replace(',', '')),
                    float(str(row.get('Rate', 0) or 0).replace(',', '')),
                    float(str(row.get('Amount', 0) or 0).replace(',', '')),
                    fetched_at,
                ))
            except Exception:
                continue
    return rows


# ── INSERT BATCH ──────────────────────────────────────────────────────────────
def insert_batch(rows):
    if not rows:
        return 0
    conn = get_db()
    conn.executemany("""
        INSERT OR IGNORE INTO floor_sheet
        (date, symbol, buyer_broker, seller_broker, quantity, rate, amount, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    saved = conn.execute("SELECT changes()").fetchone()[0]
    conn.close()
    return len(rows)


# ── STEP 1: LOAD ALL CSVs ─────────────────────────────────────────────────────
def load_all_csvs(skip_existing=True):
    files = sorted(f for f in os.listdir(CSV_DIR) if f.endswith('.csv'))
    print(f"Found {len(files)} CSV files in {CSV_DIR}")

    if skip_existing:
        conn = get_db()
        existing_dates = {r[0] for r in conn.execute("SELECT DISTINCT date FROM floor_sheet").fetchall()}
        conn.close()
        files = [f for f in files if filename_to_date(f) not in existing_dates]
        print(f"Skipping already-loaded dates. {len(files)} files to load.")

    if not files:
        print("Nothing to load.")
        return 0

    total = 0
    for i, fname in enumerate(files):
        date_str = filename_to_date(fname)
        if not date_str:
            print(f"  [SKIP] Cannot parse date from: {fname}")
            continue
        fpath = os.path.join(CSV_DIR, fname)
        rows = load_csv(fpath, date_str)
        inserted = insert_batch(rows)
        total += inserted
        if (i + 1) % 10 == 0 or i == 0:
            print(f"  [{i+1}/{len(files)}] {fname} -> {date_str}: {inserted} rows inserted")

    print(f"\nDone! Total rows inserted: {total:,}")
    return total


# ── STEP 2: COMPUTE BROKER ACCUMULATION ───────────────────────────────────────
def compute_broker_summary(date=None):
    """
    Aggregate floor_sheet into broker_summary:
    For each (date, symbol, broker) compute:
      buy_qty, sell_qty, net_qty, buy_amount, sell_amount, net_amount, trade counts
    """
    conn = get_db()

    if date:
        dates = [date]
    else:
        # Get all dates not yet in broker_summary
        all_dates = [r[0] for r in conn.execute(
            "SELECT DISTINCT date FROM floor_sheet ORDER BY date"
        ).fetchall()]
        done_dates = {r[0] for r in conn.execute(
            "SELECT DISTINCT date FROM broker_summary"
        ).fetchall()}
        dates = [d for d in all_dates if d not in done_dates]

    conn.close()

    print(f"Computing broker summary for {len(dates)} dates...")
    total_rows = 0
    fetched_at = datetime.now().isoformat()

    for i, d in enumerate(dates):
        conn = get_db()

        # Buy side: broker appears as buyer
        conn.execute("""
            INSERT OR REPLACE INTO broker_summary
                (date, symbol, broker, buy_qty, buy_amount, trades_as_buyer,
                 sell_qty, sell_amount, trades_as_seller, net_qty, net_amount, fetched_at)
            SELECT
                date, symbol, buyer_broker AS broker,
                SUM(quantity)   AS buy_qty,
                SUM(amount)     AS buy_amount,
                COUNT(*)        AS trades_as_buyer,
                0.0, 0.0, 0,
                SUM(quantity),  -- net_qty starts as buy
                SUM(amount),
                ?
            FROM floor_sheet
            WHERE date = ?
            GROUP BY date, symbol, buyer_broker
            ON CONFLICT(date, symbol, broker) DO UPDATE SET
                buy_qty         = buy_qty         + excluded.buy_qty,
                buy_amount      = buy_amount      + excluded.buy_amount,
                trades_as_buyer = trades_as_buyer + excluded.trades_as_buyer,
                net_qty         = buy_qty         - sell_qty,
                net_amount      = buy_amount      - sell_amount
        """, (fetched_at, d))

        # Sell side: broker appears as seller
        conn.execute("""
            INSERT OR REPLACE INTO broker_summary
                (date, symbol, broker, sell_qty, sell_amount, trades_as_seller,
                 buy_qty, buy_amount, trades_as_buyer, net_qty, net_amount, fetched_at)
            SELECT
                date, symbol, seller_broker AS broker,
                SUM(quantity)   AS sell_qty,
                SUM(amount)     AS sell_amount,
                COUNT(*)        AS trades_as_seller,
                0.0, 0.0, 0,
                -SUM(quantity), -- net starts negative
                -SUM(amount),
                ?
            FROM floor_sheet
            WHERE date = ?
            GROUP BY date, symbol, seller_broker
            ON CONFLICT(date, symbol, broker) DO UPDATE SET
                sell_qty          = sell_qty          + excluded.sell_qty,
                sell_amount       = sell_amount       + excluded.sell_amount,
                trades_as_seller  = trades_as_seller  + excluded.trades_as_seller,
                net_qty           = buy_qty            - (sell_qty + excluded.sell_qty),
                net_amount        = buy_amount         - (sell_amount + excluded.sell_amount)
        """, (fetched_at, d))

        conn.commit()
        count = conn.execute(
            "SELECT COUNT(*) FROM broker_summary WHERE date=?", (d,)
        ).fetchone()[0]
        conn.close()
        total_rows += count

        if (i + 1) % 10 == 0 or i == 0:
            print(f"  [{i+1}/{len(dates)}] {d}: {count} broker-symbol pairs")

    print(f"\nDone! broker_summary rows: {total_rows:,}")
    return total_rows


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("Step 1: Load floor sheet CSVs into DB")
    print("=" * 60)
    ensure_tables()
    load_all_csvs(skip_existing=True)

    conn = get_db()
    count = conn.execute("SELECT COUNT(*) FROM floor_sheet").fetchone()[0]
    dates = conn.execute("SELECT COUNT(DISTINCT date) FROM floor_sheet").fetchone()[0]
    conn.close()
    print(f"\nfloor_sheet: {count:,} rows across {dates} trading days")

    print("\n" + "=" * 60)
    print("Step 2: Compute broker accumulation summary")
    print("=" * 60)
    compute_broker_summary()

    conn = get_db()
    bs = conn.execute("SELECT COUNT(*) FROM broker_summary").fetchone()[0]
    bs_dates = conn.execute("SELECT COUNT(DISTINCT date) FROM broker_summary").fetchone()[0]
    bs_syms  = conn.execute("SELECT COUNT(DISTINCT symbol) FROM broker_summary").fetchone()[0]
    conn.close()
    print(f"\nbroker_summary: {bs:,} rows | {bs_dates} dates | {bs_syms} symbols")
    print("\nDone!")
