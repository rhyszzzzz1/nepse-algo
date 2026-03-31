# src/data/floorsheet_pipeline.py
#
# Unified floor sheet pipeline:
#   1.  Daily updater  — uses NEPSE API to get today's full market floor sheet
#   2.  Historical fill — scrapes merolagani.com/Floorsheet.aspx for past dates
#   3.  broker_summary  — recomputes accumulation metrics for any new dates
#
# Usage:
#   py -3.11 src/data/floorsheet_pipeline.py              # today only
#   py -3.11 src/data/floorsheet_pipeline.py --days 30   # backfill last 30 days
#   py -3.11 src/data/floorsheet_pipeline.py --all        # fill entire gap

import os, re, sys, time, random, sqlite3, traceback, csv
from datetime import datetime, timedelta
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup

try:
    from active_symbols import get_active_symbol_set
except ImportError:
    try:
        from data.active_symbols import get_active_symbol_set
    except ImportError:
        from src.data.active_symbols import get_active_symbol_set

# ── DB ────────────────────────────────────────────────────────────────────────
ROOT    = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(ROOT, "data", "nepse.db")
if "RAILWAY_VOLUME_MOUNT_PATH" in os.environ:
    DB_PATH = os.path.join(os.environ["RAILWAY_VOLUME_MOUNT_PATH"], "nepse.db")

try:
    from db_factory import get_db_connection
except ImportError:
    try:
        from data.db_factory import get_db_connection
    except ImportError:
        from src.data.db_factory import get_db_connection

def get_db():
    return get_db_connection(DB_PATH)


# ── RAW RETENTION POLICY ─────────────────────────────────────────────────────
# How many days of raw floor_sheet rows to keep after aggregation.
# 0 = delete raw rows immediately after broker_summary is computed (recommended
#     for the rolling-year broker-summary workflow to keep the DB small).
# N = keep the last N days of raw rows for trade-level drill-down.
KEEP_RAW_DAYS = 0

# How many calendar days of broker_summary to retain (rolling window).
# Rows older than this are pruned when --prune-raw is used.
BROKER_SUMMARY_KEEP_DAYS = 400   # ~13 months gives headroom beyond 1 year

CHUKUL_FLOORSHEET_BY_DATE_URL = "https://chukul.com/api/data/v2/floorsheet/bydate/"
SHAREHUB_FLOORSHEET_BY_DATE_URL = "https://sharehubnepal.com/live/api/v2/floorsheet"
SHAREHUB_TODAYS_PRICE_URL = "https://sharehubnepal.com/live/api/v2/nepselive/todays-price"


# ── ENSURE SCHEMA ─────────────────────────────────────────────────────────────
def ensure_schema():
    conn = get_db()
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
            fetched_at    TEXT,
            transaction_no TEXT,
            source_file    TEXT,
            page_no        INTEGER,
            row_no_in_page INTEGER
        )
    """)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(floor_sheet)").fetchall()}
    if "transaction_no" not in cols:
        conn.execute("ALTER TABLE floor_sheet ADD COLUMN transaction_no TEXT")
    if "source_file" not in cols:
        conn.execute("ALTER TABLE floor_sheet ADD COLUMN source_file TEXT")
    if "page_no" not in cols:
        conn.execute("ALTER TABLE floor_sheet ADD COLUMN page_no INTEGER")
    if "row_no_in_page" not in cols:
        conn.execute("ALTER TABLE floor_sheet ADD COLUMN row_no_in_page INTEGER")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fs_date_sym ON floor_sheet(date, symbol)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fs_date     ON floor_sheet(date)")
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_fs_unique_txn
        ON floor_sheet(date, symbol, transaction_no)
        WHERE transaction_no IS NOT NULL AND transaction_no != ''
    """)

    # Migrate broker_summary if it has the old schema
    cols = [r[1] for r in conn.execute("PRAGMA table_info(broker_summary)").fetchall()]
    if cols and "broker" not in cols:
        conn.execute("DROP TABLE IF EXISTS broker_summary")
        conn.commit()

    conn.execute("""
        CREATE TABLE IF NOT EXISTS broker_summary (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            date             TEXT NOT NULL,
            symbol           TEXT NOT NULL,
            broker           INTEGER NOT NULL,
            buy_qty          REAL DEFAULT 0,
            sell_qty         REAL DEFAULT 0,
            buy_amount       REAL DEFAULT 0,
            sell_amount      REAL DEFAULT 0,
            net_qty          REAL DEFAULT 0,
            net_amount       REAL DEFAULT 0,
            trades_as_buyer  INTEGER DEFAULT 0,
            trades_as_seller INTEGER DEFAULT 0,
            fetched_at       TEXT,
            UNIQUE(date, symbol, broker)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bs_sym_broker ON broker_summary(symbol, broker)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bs_date_sym   ON broker_summary(date, symbol)")

    # Dates confirmed as non-trading/no-data during market-wide backfills.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS backfill_skip_dates (
            date            TEXT PRIMARY KEY,
            reason          TEXT,
            source          TEXT,
            attempts        INTEGER DEFAULT 0,
            first_marked_at TEXT,
            last_marked_at  TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_skip_date ON backfill_skip_dates(date)")

    # ── Daily Price OHLCV ──
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_price (
            date        TEXT NOT NULL,
            symbol      TEXT NOT NULL,
            open        REAL,
            high        REAL,
            low         REAL,
            close       REAL,
            volume      REAL,
            amount      REAL,
            trades      INTEGER,
            vwap        REAL,
            fetched_at  TEXT,
            PRIMARY KEY (date, symbol)
        )
    """)
    conn.commit()
    conn.close()


# ── HELPERS ───────────────────────────────────────────────────────────────────
def _float(s):
    try: return float(str(s).replace(",", "").strip() or 0)
    except: return 0.0

def _int(s):
    try: return int(float(str(s).replace(",", "").strip() or 0))
    except: return 0

def _http_get_json(url, params=None, timeout=30):
    last_error = None
    for attempt in range(5):
        try:
            resp = requests.get(
                url,
                params=params,
                timeout=timeout,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            )
            if resp.status_code == 429:
                wait = min(2 ** attempt, 20)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_error = exc
            if attempt == 4:
                break
            time.sleep(min(2 ** attempt, 20))
    raise last_error

def _is_nepse_holiday(dt):
    """NEPSE trades Sun–Thu. Fri (4) and Sat (5) are holidays."""
    return dt.weekday() in (4, 5)

def normalize_trade_date(value):
    """Normalize various scraped date formats to YYYY-MM-DD."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%m-%d-%Y", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return text.replace("/", "-")

def _dates_already_in_db():
    """Return dates that have already been processed.
    Checks both floor_sheet (raw) and broker_summary (aggregated+pruned) so
    dates don't get re-fetched after raw rows have been pruned."""
    conn = get_db()
    raw_dates = {r[0] for r in conn.execute("SELECT DISTINCT date FROM floor_sheet").fetchall()}
    # broker_summary is keyed by (date, symbol, broker) — any row for a date
    # means that date was already fully processed and aggregated
    try:
        agg_dates = {r[0] for r in conn.execute("SELECT DISTINCT date FROM broker_summary").fetchall()}
    except Exception:
        agg_dates = set()
    conn.close()
    return raw_dates | agg_dates

def _get_backfill_skip_dates():
    """Dates marked as known non-trading/no-data from prior runs."""
    conn = get_db()
    try:
        rows = conn.execute("SELECT date FROM backfill_skip_dates").fetchall()
        return {r[0] for r in rows}
    except Exception:
        return set()
    finally:
        conn.close()

def _mark_backfill_skip_date(date_str_ymd, reason, source="merolagani"):
    """Persist a date that repeatedly returns no floor-sheet data."""
    ts = datetime.now().isoformat()
    conn = get_db()
    try:
        conn.execute(
            """
            INSERT INTO backfill_skip_dates(date, reason, source, attempts, first_marked_at, last_marked_at)
            VALUES (?, ?, ?, 1, ?, ?)
            ON CONFLICT(date) DO UPDATE SET
                reason = excluded.reason,
                source = excluded.source,
                attempts = backfill_skip_dates.attempts + 1,
                last_marked_at = excluded.last_marked_at
            """,
            (date_str_ymd, reason, source, ts, ts),
        )
        conn.commit()
    finally:
        conn.close()

def _save_rows(rows):
    """Bulk-insert floor_sheet rows. Returns count inserted."""
    if not rows:
        return 0

    active_symbols = get_active_symbol_set()
    if active_symbols:
        rows = [row for row in rows if str(row[1]).upper() in active_symbols]
        if not rows:
            return 0

    normalized_rows = []
    seen = set()
    for row in rows:
        row = tuple(row)
        if len(row) == 8:
            row = row + (None, None, None, None)
        elif len(row) < 12:
            row = row + (None,) * (12 - len(row))
        else:
            row = row[:12]

        dedupe_key = (
            row[0],
            row[1],
            row[8] if row[8] not in (None, "") else None,
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            row[9],
            row[10],
            row[11],
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        normalized_rows.append(row)

    if not normalized_rows:
        return 0

    conn = get_db()
    conn.executemany("""
        INSERT OR IGNORE INTO floor_sheet
        (date, symbol, buyer_broker, seller_broker, quantity, rate, amount, fetched_at,
         transaction_no, source_file, page_no, row_no_in_page)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, normalized_rows)
    conn.commit()
    n = len(normalized_rows)
    conn.close()
    return n


def recompute_broker_summary_for_symbol_dates(symbol, dates):
    """Rebuild broker_summary for a single symbol across specific dates."""
    if not symbol or not dates:
        return 0

    normalized_dates = sorted({normalize_trade_date(date) for date in dates if normalize_trade_date(date)})
    if not normalized_dates:
        return 0

    conn = get_db()
    fetched_at = datetime.now().isoformat()
    total_rows = 0
    try:
        for trade_date in normalized_dates:
            conn.execute(
                "DELETE FROM broker_summary WHERE symbol=? AND date=?",
                (symbol, trade_date),
            )

            conn.execute("""
                INSERT INTO broker_summary
                    (date, symbol, broker, buy_qty, buy_amount, trades_as_buyer,
                     sell_qty, sell_amount, trades_as_seller, net_qty, net_amount, fetched_at)
                SELECT
                    date, symbol, buyer_broker,
                    SUM(quantity), SUM(amount), COUNT(*),
                    0.0, 0.0, 0,
                    SUM(quantity), SUM(amount),
                    ?
                FROM floor_sheet
                WHERE symbol=? AND date=?
                GROUP BY date, symbol, buyer_broker
                ON CONFLICT(date, symbol, broker) DO UPDATE SET
                    buy_qty         = excluded.buy_qty,
                    buy_amount      = excluded.buy_amount,
                    trades_as_buyer = excluded.trades_as_buyer,
                    net_qty         = excluded.buy_qty - broker_summary.sell_qty,
                    net_amount      = excluded.buy_amount - broker_summary.sell_amount
            """, (fetched_at, symbol, trade_date))

            conn.execute("""
                INSERT INTO broker_summary
                    (date, symbol, broker, sell_qty, sell_amount, trades_as_seller,
                     buy_qty, buy_amount, trades_as_buyer, net_qty, net_amount, fetched_at)
                SELECT
                    date, symbol, seller_broker,
                    SUM(quantity), SUM(amount), COUNT(*),
                    0.0, 0.0, 0,
                    -SUM(quantity), -SUM(amount),
                    ?
                FROM floor_sheet
                WHERE symbol=? AND date=?
                GROUP BY date, symbol, seller_broker
                ON CONFLICT(date, symbol, broker) DO UPDATE SET
                    sell_qty          = excluded.sell_qty,
                    sell_amount       = excluded.sell_amount,
                    trades_as_seller  = excluded.trades_as_seller,
                    net_qty           = broker_summary.buy_qty - excluded.sell_qty,
                    net_amount        = broker_summary.buy_amount - excluded.sell_amount
            """, (fetched_at, symbol, trade_date))

            total_rows += conn.execute(
                "SELECT COUNT(*) FROM broker_summary WHERE symbol=? AND date=?",
                (symbol, trade_date),
            ).fetchone()[0]

        conn.commit()
        return total_rows
    finally:
        conn.close()


# ── PRUNING HELPERS ───────────────────────────────────────────────────────────
def prune_raw_floor_sheet(keep_days=None):
    """
    Delete raw floor_sheet rows to limit disk usage.
    keep_days=0  → delete all raw rows (smallest DB, broker_summary already computed)
    keep_days=N  → keep only the most recent N days, delete anything older
    """
    if keep_days is None:
        keep_days = KEEP_RAW_DAYS
    conn = get_db()
    if keep_days <= 0:
        n = conn.execute("SELECT COUNT(*) FROM floor_sheet").fetchone()[0]
        conn.execute("DELETE FROM floor_sheet")
    else:
        cutoff = (datetime.now() - timedelta(days=keep_days)).strftime("%Y-%m-%d")
        n = conn.execute("SELECT COUNT(*) FROM floor_sheet WHERE date < ?", (cutoff,)).fetchone()[0]
        conn.execute("DELETE FROM floor_sheet WHERE date < ?", (cutoff,))
    conn.commit()
    conn.close()
    print(f"[Prune] Removed {n:,} raw floor_sheet rows (keep_days={keep_days})")
    return n


def prune_old_broker_summary(keep_days=None):
    """
    Delete broker_summary rows older than keep_days to enforce the rolling window.
    """
    if keep_days is None:
        keep_days = BROKER_SUMMARY_KEEP_DAYS
    cutoff = (datetime.now() - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    conn = get_db()
    n = conn.execute("SELECT COUNT(*) FROM broker_summary WHERE date < ?", (cutoff,)).fetchone()[0]
    if n:
        conn.execute("DELETE FROM broker_summary WHERE date < ?", (cutoff,))
        conn.commit()
        print(f"[Prune] Removed {n:,} broker_summary rows older than {cutoff}")
    conn.close()
    return n


# ════════════════════════════════════════════════════════════════════════════
# METHOD 1: NEPSE Unofficial API  (today / recent, fast)
# ════════════════════════════════════════════════════════════════════════════
def fetch_by_date_via_sharehub(date_str_ymd, return_status=False, page_size=500):
    """Fetch full market floor sheet for one date from ShareHub."""
    fetched_at = datetime.now().isoformat()
    sharehub_date = date_str_ymd.replace("-0", "-").lstrip("0")
    page_size = min(int(page_size or 100), 100)
    if sharehub_date.startswith("0"):
        sharehub_date = date_str_ymd

    def _ret(saved, status, reason=""):
        if return_status:
            return saved, status, reason
        return saved

    try:
        print(f"[ShareHub] Fetching floor sheet for {date_str_ymd}...")
        first_payload = _http_get_json(
            SHAREHUB_FLOORSHEET_BY_DATE_URL,
            params={"Size": page_size, "currentPage": 1, "date": sharehub_date},
        )
        data = first_payload.get("data") or {}
        first_content = data.get("content") or []
        if not first_content:
            return _ret(0, "no_data", "empty_sharehub_payload")

        total_pages = int(data.get("totalPages") or 1)
        rows = []

        def _normalize_rows(content, page_no):
            normalized = []
            for row_index, row in enumerate(content, start=1):
                symbol = str(row.get("symbol") or "").upper()
                if not symbol:
                    continue
                normalized.append((
                    date_str_ymd,
                    symbol,
                    _int(row.get("buyerMemberId")),
                    _int(row.get("sellerMemberId")),
                    _float(row.get("contractQuantity")),
                    _float(row.get("contractRate")),
                    _float(row.get("contractAmount")),
                    fetched_at,
                    str(row.get("contractId") or "").strip() or None,
                    "sharehub",
                    page_no,
                    row_index,
                ))
            return normalized

        rows.extend(_normalize_rows(first_content, 1))

        def _fetch_page(page_no):
            payload = _http_get_json(
                SHAREHUB_FLOORSHEET_BY_DATE_URL,
                params={"Size": page_size, "currentPage": page_no, "date": sharehub_date},
                timeout=40,
            )
            return page_no, ((payload.get("data") or {}).get("content") or [])

        if total_pages > 1:
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = {executor.submit(_fetch_page, page_no): page_no for page_no in range(2, total_pages + 1)}
                for future in as_completed(futures):
                    page_no, content = future.result()
                    if content:
                        rows.extend(_normalize_rows(content, page_no))

        if not rows:
            return _ret(0, "no_data", "empty_sharehub_rows")

        txn_values = [row[8] for row in rows if row[8]]
        if txn_values:
            unique_ratio = len(set(txn_values)) / max(len(txn_values), 1)
            if total_pages > 1 and unique_ratio < 0.9:
                msg = f"sharehub_duplicate_pagination ratio={unique_ratio:.3f}"
                print(f"[ShareHub] {date_str_ymd}: rejected raw floorsheet ({msg})")
                return _ret(0, "error", msg)

        saved = _save_rows(rows)
        print(f"[ShareHub] {date_str_ymd}: {len(rows):,} trades | {saved:,} saved")
        return _ret(saved, "saved", "sharehub_success")
    except Exception as e:
        print(f"[ShareHub] {date_str_ymd} ERROR: {e}")
        return _ret(0, "error", str(e))


def fetch_by_date_via_chukul(date_str_ymd, return_status=False, page_size=500):
    """Fetch full market floor sheet for one date from Chukul."""
    fetched_at = datetime.now().isoformat()

    def _ret(saved, status, reason=""):
        if return_status:
            return saved, status, reason
        return saved

    try:
        print(f"[Chukul] Fetching floor sheet for {date_str_ymd}...")
        first_payload = _http_get_json(
            CHUKUL_FLOORSHEET_BY_DATE_URL,
            params={"date": date_str_ymd, "page": 1, "size": page_size},
        )
        first_content = first_payload.get("data") or []
        if not first_content:
            return _ret(0, "no_data", "empty_chukul_payload")

        last_page = int(first_payload.get("last_page") or 1)
        rows = []

        def _normalize_rows(content, page_no):
            normalized = []
            for row_index, row in enumerate(content, start=1):
                symbol = str(row.get("symbol") or "").upper()
                if not symbol:
                    continue
                normalized.append((
                    date_str_ymd,
                    symbol,
                    _int(row.get("buyer")),
                    _int(row.get("seller")),
                    _float(row.get("quantity")),
                    _float(row.get("rate")),
                    _float(row.get("amount")),
                    fetched_at,
                    str(row.get("transaction") or "").strip() or None,
                    "chukul",
                    page_no,
                    row_index,
                ))
            return normalized

        rows.extend(_normalize_rows(first_content, 1))

        for page_no in range(2, last_page + 1):
            payload = _http_get_json(
                CHUKUL_FLOORSHEET_BY_DATE_URL,
                params={"date": date_str_ymd, "page": page_no, "size": page_size},
            )
            content = payload.get("data") or []
            if content:
                rows.extend(_normalize_rows(content, page_no))
            if page_no % 50 == 0 or page_no == last_page:
                print(f"[Chukul] {date_str_ymd}: fetched page {page_no}/{last_page}")

        if not rows:
            return _ret(0, "no_data", "empty_chukul_rows")

        saved = _save_rows(rows)
        print(f"[Chukul] {date_str_ymd}: {len(rows):,} trades | {saved:,} saved")
        return _ret(saved, "saved", "chukul_success")
    except Exception as e:
        print(f"[Chukul] {date_str_ymd} ERROR: {e}")
        return _ret(0, "error", str(e))


def fetch_today_via_api():
    """Fetch full market floor sheet for today using NepseUnofficialApi."""
    today = datetime.now().strftime("%Y-%m-%d")
    saved, _, _ = fetch_by_date_via_chukul(today, return_status=True)
    if saved:
        return saved
    saved, _, _ = fetch_by_date_via_sharehub(today, return_status=True)
    if saved:
        return saved
    try:
        from nepse import Nepse
        nepse = Nepse()
        nepse.setTLSVerification(False)
        today = datetime.now().strftime("%Y-%m-%d")
        fetched_at = datetime.now().isoformat()

        print(f"[API] Fetching today's floor sheet ({today})...")
        data = nepse.getFloorSheet()

        import pandas as pd
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            df = pd.DataFrame()
            for v in data.values():
                if isinstance(v, list):
                    df = pd.DataFrame(v)
                    break
        else:
            print("[API] Unexpected response type:", type(data))
            return 0

        if df.empty:
            print("[API] No data returned (market may be closed)")
            return 0

        print(f"[API]   {len(df)} trades received. Columns: {list(df.columns)}")

        # Try to resolve column names flexibly
        sym_col    = next((c for c in df.columns if 'symbol' in c.lower() or 'stock' in c.lower()), None)
        buyer_col  = next((c for c in df.columns if 'buyer' in c.lower() or 'buymem' in c.lower()), None)
        seller_col = next((c for c in df.columns if 'seller' in c.lower() or 'sellmem' in c.lower()), None)
        qty_col    = next((c for c in df.columns if 'quantity' in c.lower() or 'contractq' in c.lower()), None)
        rate_col   = next((c for c in df.columns if 'rate' in c.lower() or 'contractr' in c.lower()), None)
        amt_col    = next((c for c in df.columns if 'amount' in c.lower() or 'contracta' in c.lower()), None)

        rows = []
        for _, row in df.iterrows():
            sym = str(row[sym_col]).upper() if sym_col else ""
            if not sym or sym == "NAN":
                continue
            rows.append((
                today,
                sym,
                _int(row[buyer_col])  if buyer_col  else 0,
                _int(row[seller_col]) if seller_col else 0,
                _float(row[qty_col])  if qty_col    else 0.0,
                _float(row[rate_col]) if rate_col   else 0.0,
                _float(row[amt_col])  if amt_col    else 0.0,
                fetched_at,
            ))

        saved = _save_rows(rows)
        print(f"[API]   Saved {saved} rows for {today}")
        return saved

    except ImportError:
        print("[API] nepse library not installed. Skipping API fetch.")
        return 0
    except Exception as e:
        print(f"[API] Error: {e}")
        traceback.print_exc()
        return 0


# ════════════════════════════════════════════════════════════════════════════
# METHOD 2: Merolagani.com/Floorsheet.aspx  (historical, one date at a time)
# ════════════════════════════════════════════════════════════════════════════
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://merolagani.com/",
})

ML_URL = "https://merolagani.com/Floorsheet.aspx"

def _get_ml_state(soup=None, html=None):
    """Extract all hidden form fields from a parsed page."""
    if soup is None:
        soup = BeautifulSoup(html, "html.parser")
    state = {}
    for inp in soup.find_all("input"):
        n = inp.get("name", "")
        if n:
            state[n] = inp.get("value", "")
    return state

def _ml_parse_table(soup_or_html):
    """Parse the floor sheet table. Returns (rows_list, total_pages)."""
    if isinstance(soup_or_html, str):
        soup = BeautifulSoup(soup_or_html, "html.parser")
    else:
        soup = soup_or_html

    table = None
    for t in soup.find_all("table"):
        headers = t.get_text().lower()
        if "buyer" in headers and "seller" in headers:
            table = t
            break

    rows = []
    if table:
        for tr in table.find_all("tr")[1:]:
            tds = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(tds) < 7:
                continue
            try:
                # Merolagani can return either:
                # 7 cols: # | Symbol | Buyer | Seller | Qty | Rate | Amount
                # 8 cols: # | Transact. No. | Symbol | Buyer | Seller | Qty | Rate | Amount
                offset = 1 if len(tds) >= 8 else 0
                symbol_idx = 1 + offset
                buyer_idx = 2 + offset
                seller_idx = 3 + offset
                qty_idx = 4 + offset
                rate_idx = 5 + offset
                amount_idx = 6 + offset

                quantity = _float(tds[qty_idx])
                rate = _float(tds[rate_idx])
                amount = _float(tds[amount_idx]) if len(tds) > amount_idx else round(quantity * rate, 2)

                rows.append({
                    "symbol":        tds[symbol_idx].upper(),
                    "buyer_broker":  _int(tds[buyer_idx]),
                    "seller_broker": _int(tds[seller_idx]),
                    "quantity":      quantity,
                    "rate":          rate,
                    "amount":        amount,
                })
            except Exception:
                continue

    total_pages = 1
    m = re.search(r"Total pages:\s*(\d+)", soup.get_text(), re.IGNORECASE)
    if m:
        total_pages = int(m.group(1))

    return rows, total_pages

def _ml_parse_delta(raw):
    """Extract HTML from ASP.NET UpdatePanel delta response (for pagination only)."""
    pattern = re.compile(r'(\d+)\|updatePanel\|([^|]+)\|')
    best = ""
    pos = 0
    while True:
        m = pattern.search(raw, pos)
        if not m:
            break
        length = int(m.group(1))
        start  = m.end()
        chunk  = raw[start: start + length]
        if len(chunk) > len(best):
            best = chunk
        pos = start + length
    return best or None

def _ml_fetch_with_playwright(date_str_ymd, ml_date):
    """Browser-based fallback for dates blocked by anti-bot/server-error pages."""
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        print("  [Playwright] not installed, cannot use browser fallback")
        return [], 0

    all_rows = []
    total_pages = 0
    def _safe_page_content(page_obj):
        for _ in range(12):
            try:
                return page_obj.content()
            except Exception:
                page_obj.wait_for_timeout(250)
        return ""

    def _safe_table_html(page_obj):
        for _ in range(12):
            try:
                table = page_obj.locator("table.table").first
                if table.count() > 0:
                    return table.evaluate("el => el.outerHTML")
            except Exception:
                pass
            page_obj.wait_for_timeout(250)
        return ""

    def _goto_page(page_obj, page_no):
        # Try the website helper first; if missing, use pager link click fallback.
        try:
            used_helper = page_obj.evaluate(
                """(p) => {
                    if (typeof changePageIndex === 'function') {
                        changePageIndex(String(p),
                          'ctl00_ContentPlaceHolder1_PagerControl1_hdnCurrentPage',
                          'ctl00_ContentPlaceHolder1_PagerControl1_btnPaging');
                        return true;
                    }
                    return false;
                }""",
                page_no,
            )
        except Exception:
            used_helper = False

        if not used_helper:
            try:
                link = page_obj.locator(f"a:has-text('{page_no}')").first
                if link.count() == 0:
                    return False
                link.click(timeout=15000)
            except Exception:
                return False

        for _ in range(20):
            try:
                page_obj.wait_for_selector("table.table", timeout=1000)
                return True
            except Exception:
                page_obj.wait_for_timeout(250)
        return False

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(ML_URL, wait_until="load", timeout=60000)
            page.fill("#ctl00_ContentPlaceHolder1_txtFloorsheetDateFilter", ml_date)
            page.click("#ctl00_ContentPlaceHolder1_lbtnSearchFloorsheet")
            page.wait_for_load_state("load", timeout=60000)
            page.wait_for_selector("table.table", timeout=60000)

            html1 = _safe_page_content(page)
            soup1 = BeautifulSoup(html1, "html.parser")
            txt1 = soup1.get_text(" ", strip=True)
            if "Could not find floorsheet" in txt1 or "No record" in txt1:
                browser.close()
                return [], 0

            # Parse from full page HTML so pager text ("Total pages") is visible.
            page_rows, total_pages = _ml_parse_table(soup1)
            if not page_rows:
                browser.close()
                return [], 0

            all_rows.extend(page_rows)

            # Full-page pagination (helper function when available, pager link otherwise)
            for page_no in range(2, total_pages + 1):
                if not _goto_page(page, page_no):
                    print(f"  [{date_str_ymd}] Playwright fallback could not move to page {page_no}; keeping fetched rows")
                    break

                page.wait_for_timeout(700)

                htmln = _safe_page_content(page)
                rowsn, _ = _ml_parse_table(htmln)
                if not rowsn:
                    break
                all_rows.extend(rowsn)

            browser.close()
    except Exception as e:
        print(f"  [{date_str_ymd}] Playwright fallback error: {e}")
        return [], 0

    return all_rows, total_pages

def fetch_historical_via_merolagani(date_str_ymd, return_status=False):
    """
    Fetch full market floor sheet for one date from merolagani.com/Floorsheet.aspx
    date_str_ymd: YYYY-MM-DD
    Returns: number of rows saved
    """
    dt = datetime.strptime(date_str_ymd, "%Y-%m-%d")
    ml_date    = dt.strftime("%m/%d/%Y")  # MM/DD/YYYY
    fetched_at = datetime.now().isoformat()

    def _ret(saved, status, reason=""):
        if return_status:
            return saved, status, reason
        return saved

    try:
        # ── Step 1: GET the page to collect form state ────────────────────────
        time.sleep(random.uniform(1.0, 2.0))
        r0 = SESSION.get(ML_URL, timeout=30)
        r0.raise_for_status()
        soup0 = BeautifulSoup(r0.text, "html.parser")
        state = _get_ml_state(soup0)

        # ── Step 2: Regular full-page POST to search for date ─────────────────
        payload = dict(state)
        payload["__EVENTTARGET"]   = "ctl00$ContentPlaceHolder1$lbtnSearchFloorsheet"
        payload["__EVENTARGUMENT"] = ""
        payload.setdefault("ctl00$ASCompany$hdnAutoSuggest", "0")
        payload.setdefault("ctl00$ASCompany$txtAutoSuggest", "")
        payload.setdefault("ctl00$txtNews", "")
        payload.setdefault("ctl00$AutoSuggest1$hdnAutoSuggest", "0")
        payload.setdefault("ctl00$AutoSuggest1$txtAutoSuggest", "")
        payload["ctl00$ContentPlaceHolder1$txtFloorsheetDateFilter"]     = ml_date
        payload["ctl00$ContentPlaceHolder1$txtBuyerBrokerCodeFilter"]    = ""
        payload["ctl00$ContentPlaceHolder1$txtSellerBrokerCodeFilter"]   = ""
        payload["ctl00$ContentPlaceHolder1$ASCompanyFilter$hdnAutoSuggest"] = "0"
        payload["ctl00$ContentPlaceHolder1$ASCompanyFilter$txtAutoSuggest"]  = ""
        payload["ctl00$ContentPlaceHolder1$PagerControl1$hdnPCID"]        = "PC1"
        payload["ctl00$ContentPlaceHolder1$PagerControl1$hdnCurrentPage"] = "0"
        payload["ctl00$ContentPlaceHolder1$PagerControl2$hdnPCID"]        = "PC2"
        payload["ctl00$ContentPlaceHolder1$PagerControl2$hdnCurrentPage"] = "0"

        r1 = None
        for attempt in range(1, 4):
            time.sleep(random.uniform(1.0, 2.0))
            r1 = SESSION.post(ML_URL, data=payload, headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": ML_URL,
            }, timeout=30)
            r1.raise_for_status()

            # Merolagani sometimes serves a transient generic error page.
            if "Something went wrong on the server" in r1.text:
                if attempt < 3:
                    print(f"  [{date_str_ymd}] Server error page returned, retrying ({attempt}/3)...")
                    # Refresh form state before retrying the search POST
                    r0 = SESSION.get(ML_URL, timeout=30)
                    r0.raise_for_status()
                    soup0 = BeautifulSoup(r0.text, "html.parser")
                    state = _get_ml_state(soup0)
                    payload = dict(state)
                    payload["__EVENTTARGET"] = "ctl00$ContentPlaceHolder1$lbtnSearchFloorsheet"
                    payload["__EVENTARGUMENT"] = ""
                    payload.setdefault("ctl00$ASCompany$hdnAutoSuggest", "0")
                    payload.setdefault("ctl00$ASCompany$txtAutoSuggest", "")
                    payload.setdefault("ctl00$txtNews", "")
                    payload.setdefault("ctl00$AutoSuggest1$hdnAutoSuggest", "0")
                    payload.setdefault("ctl00$AutoSuggest1$txtAutoSuggest", "")
                    payload["ctl00$ContentPlaceHolder1$txtFloorsheetDateFilter"] = ml_date
                    payload["ctl00$ContentPlaceHolder1$txtBuyerBrokerCodeFilter"] = ""
                    payload["ctl00$ContentPlaceHolder1$txtSellerBrokerCodeFilter"] = ""
                    payload["ctl00$ContentPlaceHolder1$ASCompanyFilter$hdnAutoSuggest"] = "0"
                    payload["ctl00$ContentPlaceHolder1$ASCompanyFilter$txtAutoSuggest"] = ""
                    payload["ctl00$ContentPlaceHolder1$PagerControl1$hdnPCID"] = "PC1"
                    payload["ctl00$ContentPlaceHolder1$PagerControl1$hdnCurrentPage"] = "0"
                    payload["ctl00$ContentPlaceHolder1$PagerControl2$hdnPCID"] = "PC2"
                    payload["ctl00$ContentPlaceHolder1$PagerControl2$hdnCurrentPage"] = "0"
                    continue
                print(f"  [{date_str_ymd}] Server error page returned after retries; trying Playwright fallback...")

                all_rows, total_pages = _ml_fetch_with_playwright(date_str_ymd, ml_date)
                if not all_rows:
                    print(f"  [{date_str_ymd}] Playwright fallback found no data")
                    return _ret(0, "no_data", "playwright_fallback_no_data")

                db_rows = [
                    (date_str_ymd, row["symbol"], row["buyer_broker"], row["seller_broker"],
                     row["quantity"], row["rate"], row["amount"], fetched_at)
                    for row in all_rows if row.get("symbol")
                ]
                saved = _save_rows(db_rows)
                print(f"  [{date_str_ymd}] [Playwright] {total_pages}p | {len(all_rows)} trades | {saved} saved")
                return _ret(saved, "saved", "playwright_success")
            break

        soup1 = BeautifulSoup(r1.text, "html.parser")

        # Check for "no data" message
        page_text = soup1.get_text()
        if "Could not find floorsheet" in page_text or "No record" in page_text:
            print(f"  [{date_str_ymd}] No data (holiday or data not available)")
            return _ret(0, "no_data", "no_record_message")

        page_rows, total_pages = _ml_parse_table(soup1)

        if not page_rows:
            print(f"  [{date_str_ymd}] No table rows found in response")
            return _ret(0, "no_data", "empty_table")

        all_rows = list(page_rows)
        first_row = page_rows[0].copy()

        # Update state from the result page for pagination
        state = _get_ml_state(soup1)

        # ── Step 3: Paginate using AJAX for pages 2+ ──────────────────────────
        for page in range(1, total_pages):
            payload2 = dict(state)
            payload2["ctl00$ScriptManager1"] = (
                "ctl00$ContentPlaceHolder1$updFloorsheet"
                "|ctl00$ContentPlaceHolder1$PagerControl1$btnPaging"
            )
            payload2["__EVENTTARGET"]   = ""
            payload2["__EVENTARGUMENT"] = ""
            payload2["ctl00$ContentPlaceHolder1$txtFloorsheetDateFilter"]     = ml_date
            payload2["ctl00$ContentPlaceHolder1$txtBuyerBrokerCodeFilter"]    = ""
            payload2["ctl00$ContentPlaceHolder1$txtSellerBrokerCodeFilter"]   = ""
            payload2["ctl00$ContentPlaceHolder1$ASCompanyFilter$hdnAutoSuggest"] = "0"
            payload2["ctl00$ContentPlaceHolder1$ASCompanyFilter$txtAutoSuggest"]  = ""
            payload2["ctl00$ContentPlaceHolder1$PagerControl1$hdnPCID"]        = "PC1"
            payload2["ctl00$ContentPlaceHolder1$PagerControl1$hdnCurrentPage"] = str(page)
            payload2["ctl00$ContentPlaceHolder1$PagerControl1$btnPaging"]      = ""
            payload2["ctl00$ContentPlaceHolder1$PagerControl2$hdnPCID"]        = "PC2"
            payload2["ctl00$ContentPlaceHolder1$PagerControl2$hdnCurrentPage"] = "0"
            payload2["__ASYNCPOST"] = "true"

            time.sleep(random.uniform(0.5, 1.0))
            r2 = SESSION.post(ML_URL, data=payload2, headers={
                "X-MicrosoftAjax":  "Delta=true",
                "X-Requested-With": "XMLHttpRequest",
                "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
                "Referer":          ML_URL,
            }, timeout=30)
            r2.raise_for_status()

            html2 = _ml_parse_delta(r2.text)
            if not html2:
                # Fall back to full-page POST for this page
                p3 = dict(state)
                p3["__EVENTTARGET"]   = "ctl00$ContentPlaceHolder1$PagerControl1$btnPaging"
                p3["__EVENTARGUMENT"] = str(page)
                p3["ctl00$ContentPlaceHolder1$txtFloorsheetDateFilter"] = ml_date
                p3["ctl00$ContentPlaceHolder1$PagerControl1$hdnCurrentPage"] = str(page)
                time.sleep(random.uniform(0.8, 1.5))
                r3 = SESSION.post(ML_URL, data=p3, headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Referer": ML_URL,
                }, timeout=30)
                soup3 = BeautifulSoup(r3.text, "html.parser")
                rows3, _ = _ml_parse_table(soup3)
                state = _get_ml_state(soup3)
            else:
                rows3, _ = _ml_parse_table(html2)
                vs = re.search(r'\|hiddenField\|__VIEWSTATE\|([^|]+)', r2.text)
                ev = re.search(r'\|hiddenField\|__EVENTVALIDATION\|([^|]+)', r2.text)
                if vs: state["__VIEWSTATE"] = vs.group(1)
                if ev: state["__EVENTVALIDATION"] = ev.group(1)

            if not rows3:
                break
            # Duplicate check
            if rows3[0]["quantity"] == first_row["quantity"] and rows3[0]["rate"] == first_row["rate"]:
                break
            all_rows.extend(rows3)

        # ── Save ──────────────────────────────────────────────────────────────
        db_rows = [
            (date_str_ymd, row["symbol"], row["buyer_broker"], row["seller_broker"],
             row["quantity"], row["rate"], row["amount"], fetched_at)
            for row in all_rows if row.get("symbol")
        ]
        saved = _save_rows(db_rows)
        print(f"  [{date_str_ymd}] {total_pages}p | {len(all_rows)} trades | {saved} saved")
        return _ret(saved, "saved", "merolagani_success")

    except Exception as e:
        print(f"  [{date_str_ymd}] ERROR: {e}")
        traceback.print_exc()
        return _ret(0, "error", str(e))


def fetch_today_price_from_sharehub():
    """Fetch latest daily price snapshot from ShareHub live API."""
    payload = _http_get_json(
        SHAREHUB_TODAYS_PRICE_URL,
        params={
            "queryKey[0]": "todayPrice",
            "queryKey[1][order]": "",
            "queryKey[1][sortBy]": "",
            "queryKey[1][searchText]": "",
            "queryKey[1][page]": 1,
            "queryKey[1][limit]": 500,
            "queryKey[1][sortDirection]": "",
        },
        timeout=30,
    )
    rows = payload.get("data") or []
    if not rows:
        return None, []

    trade_date = str(rows[0].get("businessDate") or "").strip()
    normalized = []
    for row in rows:
        symbol = str(row.get("symbol") or "").upper()
        if not symbol:
            continue
        normalized.append(
            (
                trade_date,
                symbol,
                _float(row.get("openPrice")),
                _float(row.get("highPrice")),
                _float(row.get("lowPrice")),
                _float(row.get("ltp")),
                _float(row.get("totalTradedQuantity")),
                _float(row.get("totalTradedValue")),
                _int(row.get("totalTrades")),
                _float(row.get("averageTradedPrice")),
                datetime.now().isoformat(),
            )
        )
    return trade_date, normalized


# ════════════════════════════════════════════════════════════════════════════
# OHLC V COMPUTATION
# ════════════════════════════════════════════════════════════════════════════
def compute_daily_price(dates=None):
    """
    Compute or fetch daily OHLCV rows for new dates.
    Latest date is sourced from ShareHub live daily-price API when available.
    Older dates fall back to floor_sheet aggregation.
    """
    conn = get_db()
    if dates is None:
        all_dates  = {r[0] for r in conn.execute("SELECT DISTINCT date FROM floor_sheet").fetchall()}
        done_dates = {r[0] for r in conn.execute("SELECT DISTINCT date FROM daily_price").fetchall()}
        dates = sorted(all_dates - done_dates)
    conn.close()

    if not dates:
        print("[Price] No new dates to process.")
        return 0

    print(f"[Price] Computing OHLCV for {len(dates)} dates...")
    fetched_at = datetime.now().isoformat()
    total = 0

    sharehub_date = None
    sharehub_rows = []
    try:
        sharehub_date, sharehub_rows = fetch_today_price_from_sharehub()
    except Exception as e:
        print(f"[Price] ShareHub daily-price fetch failed, using floor_sheet fallback: {e}")

    for d in dates:
        conn = get_db()
        try:
            # Delete any partial rows for this date first
            conn.execute("DELETE FROM daily_price WHERE date=?", (d,))

            if sharehub_date and d == sharehub_date and sharehub_rows:
                conn.executemany("""
                    INSERT INTO daily_price (
                        date, symbol, open, high, low, close, volume, amount, trades, vwap, fetched_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, sharehub_rows)
            else:
                # Fallback for older dates from raw floor_sheet aggregation.
                conn.execute("""
                    INSERT INTO daily_price (
                        date, symbol, open, high, low, close, volume, amount, trades, vwap, fetched_at
                    )
                    SELECT
                        date,
                        symbol,
                        (SELECT rate FROM floor_sheet f2 WHERE f2.date=f1.date AND f2.symbol=f1.symbol ORDER BY id ASC LIMIT 1) as open,
                        MAX(rate) as high,
                        MIN(rate) as low,
                        (SELECT rate FROM floor_sheet f3 WHERE f3.date=f1.date AND f3.symbol=f1.symbol ORDER BY id DESC LIMIT 1) as close,
                        SUM(quantity) as volume,
                        SUM(amount) as amount,
                        COUNT(*) as trades,
                        ROUND(SUM(amount) / NULLIF(SUM(quantity), 0), 2) as vwap,
                        ?
                    FROM floor_sheet f1
                    WHERE date=?
                    GROUP BY date, symbol
                """, (fetched_at, d))

            conn.commit()
            n = conn.execute("SELECT COUNT(*) FROM daily_price WHERE date=?", (d,)).fetchone()[0]
            total += n
        except Exception as e:
            print(f"  [{d}] daily_price error: {e}")
        finally:
            conn.close()

    print(f"[Price] Done. {total:,} rows in daily_price for {len(dates)} new dates")
    return total


# ════════════════════════════════════════════════════════════════════════════
# BROKER SUMMARY COMPUTATION
# ════════════════════════════════════════════════════════════════════════════
def compute_broker_summary(dates=None):
    """
    For each (date, symbol, broker) aggregate buy/sell qty/amount and trade counts.
    Only processes dates not already in broker_summary.
    """
    conn = get_db()
    if dates is None:
        all_dates  = {r[0] for r in conn.execute("SELECT DISTINCT date FROM floor_sheet").fetchall()}
        done_dates = {r[0] for r in conn.execute("SELECT DISTINCT date FROM broker_summary").fetchall()}
        dates = sorted(all_dates - done_dates)
    conn.close()

    if not dates:
        print("[Broker] No new dates to process.")
        return 0

    print(f"[Broker] Computing summary for {len(dates)} dates...")
    fetched_at = datetime.now().isoformat()
    total = 0

    for i, d in enumerate(dates):
        conn = get_db()
        try:
            # Delete any partial rows for this date first
            conn.execute("DELETE FROM broker_summary WHERE date=?", (d,))

            # Buyer side
            conn.execute("""
                INSERT INTO broker_summary
                    (date, symbol, broker, buy_qty, buy_amount, trades_as_buyer,
                     sell_qty, sell_amount, trades_as_seller, net_qty, net_amount, fetched_at)
                SELECT
                    date, symbol, buyer_broker,
                    SUM(quantity), SUM(amount), COUNT(*),
                    0.0, 0.0, 0,
                    SUM(quantity), SUM(amount),
                    ?
                FROM floor_sheet
                WHERE date=?
                GROUP BY date, symbol, buyer_broker
                ON CONFLICT(date, symbol, broker) DO UPDATE SET
                    buy_qty         = excluded.buy_qty,
                    buy_amount      = excluded.buy_amount,
                    trades_as_buyer = excluded.trades_as_buyer,
                    net_qty         = excluded.buy_qty - broker_summary.sell_qty,
                    net_amount      = excluded.buy_amount - broker_summary.sell_amount
            """, (fetched_at, d))

            # Seller side
            conn.execute("""
                INSERT INTO broker_summary
                    (date, symbol, broker, sell_qty, sell_amount, trades_as_seller,
                     buy_qty, buy_amount, trades_as_buyer, net_qty, net_amount, fetched_at)
                SELECT
                    date, symbol, seller_broker,
                    SUM(quantity), SUM(amount), COUNT(*),
                    0.0, 0.0, 0,
                    -SUM(quantity), -SUM(amount),
                    ?
                FROM floor_sheet
                WHERE date=?
                GROUP BY date, symbol, seller_broker
                ON CONFLICT(date, symbol, broker) DO UPDATE SET
                    sell_qty          = excluded.sell_qty,
                    sell_amount       = excluded.sell_amount,
                    trades_as_seller  = excluded.trades_as_seller,
                    net_qty           = broker_summary.buy_qty - excluded.sell_qty,
                    net_amount        = broker_summary.buy_amount - excluded.sell_amount
            """, (fetched_at, d))

            conn.commit()
            n = conn.execute("SELECT COUNT(*) FROM broker_summary WHERE date=?", (d,)).fetchone()[0]
            total += n
            if (i + 1) % 10 == 0 or i < 3:
                print(f"  [{i+1}/{len(dates)}] {d}: {n} broker-symbol pairs")
        except Exception as e:
            print(f"  [{d}] ERROR: {e}")
        finally:
            conn.close()

    print(f"[Broker] Done. {total:,} rows in broker_summary for {len(dates)} new dates")
    return total


# ════════════════════════════════════════════════════════════════════════════
# DAILY UPDATE  (call this from scheduler / API endpoint)
# ════════════════════════════════════════════════════════════════════════════
def run_daily_update(prune_raw=True):
    """
    Called every trading day after market close (4 PM NST).
    1. Tries Chukul by-date floorsheet first.
    2. Falls back to ShareHub, then Merolagani if needed.
    3. Recomputes broker_summary + daily_price.
    4. Optionally prunes raw floor_sheet rows (prune_raw=True by default).
    """
    today = datetime.now().strftime("%Y-%m-%d")
    if _is_nepse_holiday(datetime.now()):
        print(f"[Daily] {today} is a holiday, skipping.")
        return

    existing = _dates_already_in_db()
    if today in existing:
        print(f"[Daily] {today} already in DB, recomputing broker_summary only.")
    else:
        saved, status, _ = fetch_by_date_via_chukul(today, return_status=True)
        if not saved:
            print("[Daily] Chukul daily floorsheet unavailable; trying ShareHub...")
            saved, status, _ = fetch_by_date_via_sharehub(today, return_status=True)
        if not saved:
            if status == "error":
                print("[Daily] ShareHub/Chukul fetch errored; falling back to Merolagani scrape...")
            else:
                print("[Daily] ShareHub/Chukul returned no data; falling back to Merolagani scrape...")
            saved = fetch_historical_via_merolagani(today)
        if not saved:
            print(f"[Daily] No floor sheet data available for {today}")
            return

    compute_broker_summary()
    compute_daily_price()
    if prune_raw:
        prune_raw_floor_sheet(keep_days=KEEP_RAW_DAYS)
        prune_old_broker_summary(keep_days=BROKER_SUMMARY_KEEP_DAYS)
    print(f"[Daily] Update complete for {today}")


# ════════════════════════════════════════════════════════════════════════════
# HISTORICAL BACKFILL  (fill the Oct 2021 → present gap)
# ════════════════════════════════════════════════════════════════════════════
def run_historical_backfill(days_back=None, start_date=None, prune_raw=False):
    """
    Fills dates from start_date (or days_back ago) through today using
    the market-wide Merolagani floorsheet scraper.  One HTTP session per
    date — much faster than the per-symbol company-detail approach.

    prune_raw=True: delete raw floor_sheet rows after each batch aggregation
                    to keep the DB small (broker_summary is kept).
    """
    ensure_schema()
    existing = _dates_already_in_db()
    skipped_dates = _get_backfill_skip_dates()

    today = datetime.now().date()
    if start_date:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
    elif days_back:
        start = today - timedelta(days=days_back)
    else:
        # Default: fill from Oct 11 2021 (day after last CSV) to yesterday
        start = datetime.strptime("2021-10-11", "%Y-%m-%d").date()

    dates_to_fetch = []
    d = start
    while d <= today:
        d_str = d.strftime("%Y-%m-%d")
        if (
            not _is_nepse_holiday(datetime.combine(d, datetime.min.time()))
            and d_str not in existing
            and d_str not in skipped_dates
        ):
            dates_to_fetch.append(d_str)
        d += timedelta(days=1)

    print(f"[Backfill] {len(dates_to_fetch)} dates to fetch ({start} → {today})")
    if skipped_dates:
        print(f"[Backfill] Skipping {len(skipped_dates)} previously marked no-data dates")
    if prune_raw:
        print(f"[Backfill] prune_raw=True — raw floor_sheet rows will be deleted after each batch")

    total = 0
    batch = 0
    batch_dates = []
    for i, date_str in enumerate(dates_to_fetch):
        saved, status, reason = fetch_by_date_via_chukul(date_str, return_status=True)
        if not saved:
            saved, status, reason = fetch_by_date_via_sharehub(date_str, return_status=True)
        if not saved:
            saved, status, reason = fetch_historical_via_merolagani(date_str, return_status=True)
        if status == "no_data":
            _mark_backfill_skip_date(date_str, reason)
            print(f"  [{date_str}] Marked as no-data for future runs ({reason})")
        total += saved
        batch += 1
        batch_dates.append(date_str)

        # Compute summaries in batches to keep memory low
        if batch >= 20:
            compute_broker_summary()
            compute_daily_price()
            if prune_raw:
                prune_raw_floor_sheet(keep_days=KEEP_RAW_DAYS)
            batch = 0
            batch_dates = []

        # Random pause every 50 requests to avoid rate limiting
        if (i + 1) % 50 == 0:
            pause = random.uniform(10, 20)
            print(f"[Backfill] Pausing {pause:.0f}s to avoid rate limit...")
            time.sleep(pause)

    # Final aggregation + optional prune
    compute_broker_summary()
    compute_daily_price()
    if prune_raw:
        prune_raw_floor_sheet(keep_days=KEEP_RAW_DAYS)
        prune_old_broker_summary(keep_days=BROKER_SUMMARY_KEEP_DAYS)
    print(f"\n[Backfill] Complete! Total floor sheet rows saved: {total:,}")



# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Floor sheet pipeline — market-wide broker-summary approach")
    parser.add_argument("--daily",        action="store_true", help="Fetch today (API → Merolagani fallback) + update broker_summary")
    parser.add_argument("--rolling-year", action="store_true", help="Backfill last 365 days (market-wide, recommended for broker-summary)")
    parser.add_argument("--days",         type=int,            help="Backfill last N days via Merolagani market-wide fetch")
    parser.add_argument("--from",         dest="start",        help="Backfill from YYYY-MM-DD via Merolagani")
    parser.add_argument("--all",          action="store_true", help="Full backfill Oct 2021 → today")
    parser.add_argument("--summary",      action="store_true", help="Recompute broker_summary + daily_price only (no scrape)")
    parser.add_argument("--prune-raw",    action="store_true", help="Delete raw floor_sheet rows after aggregation to save space")
    parser.add_argument("--prune-only",   action="store_true", help="Only prune old raw + broker_summary rows, no scraping")
    args = parser.parse_args()

    ensure_schema()

    if args.prune_only:
        prune_raw_floor_sheet(keep_days=KEEP_RAW_DAYS)
        prune_old_broker_summary(keep_days=BROKER_SUMMARY_KEEP_DAYS)
    elif args.daily:
        run_daily_update(prune_raw=args.prune_raw)
    elif args.rolling_year:
        run_historical_backfill(days_back=365, prune_raw=args.prune_raw)
    elif args.summary:
        compute_broker_summary()
        compute_daily_price()
        if args.prune_raw:
            prune_raw_floor_sheet(keep_days=KEEP_RAW_DAYS)
            prune_old_broker_summary(keep_days=BROKER_SUMMARY_KEEP_DAYS)
    elif args.days:
        run_historical_backfill(days_back=args.days, prune_raw=args.prune_raw)
    elif args.start:
        run_historical_backfill(start_date=args.start, prune_raw=args.prune_raw)
    elif args.all:
        run_historical_backfill(prune_raw=args.prune_raw)
    else:
        # Default: daily update
        run_daily_update(prune_raw=args.prune_raw)

    # Final stats
    conn = get_db()
    fs  = conn.execute("SELECT COUNT(*) FROM floor_sheet").fetchone()[0]
    fs_d = conn.execute("SELECT COUNT(DISTINCT date) FROM floor_sheet").fetchone()[0]
    bs  = conn.execute("SELECT COUNT(*) FROM broker_summary").fetchone()[0]
    bs_d = conn.execute("SELECT COUNT(DISTINCT date) FROM broker_summary").fetchone()[0]
    dp  = conn.execute("SELECT COUNT(*) FROM daily_price").fetchone()[0]
    dp_d = conn.execute("SELECT COUNT(DISTINCT date) FROM daily_price").fetchone()[0]
    conn.close()
    print(f"\nfloor_sheet   : {fs:>12,} rows | {fs_d} dates")
    print(f"broker_summary: {bs:>12,} rows | {bs_d} dates")
    print(f"daily_price   : {dp:>12,} rows | {dp_d} dates")
