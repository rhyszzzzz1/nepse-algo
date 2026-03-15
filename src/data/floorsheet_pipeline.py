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
import requests
from bs4 import BeautifulSoup

# ── DB ────────────────────────────────────────────────────────────────────────
ROOT    = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(ROOT, "data", "nepse.db")
if "RAILWAY_VOLUME_MOUNT_PATH" in os.environ:
    DB_PATH = os.path.join(os.environ["RAILWAY_VOLUME_MOUNT_PATH"], "nepse.db")

def get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=60.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


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
            fetched_at    TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fs_date_sym ON floor_sheet(date, symbol)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fs_date     ON floor_sheet(date)")

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

def _is_nepse_holiday(dt):
    """NEPSE trades Sun–Thu. Fri (4) and Sat (5) are holidays."""
    return dt.weekday() in (4, 5)

def _dates_already_in_db():
    conn = get_db()
    dates = {r[0] for r in conn.execute("SELECT DISTINCT date FROM floor_sheet").fetchall()}
    conn.close()
    return dates

def _save_rows(rows):
    """Bulk-insert floor_sheet rows. Returns count inserted."""
    if not rows:
        return 0
    conn = get_db()
    conn.executemany("""
        INSERT OR IGNORE INTO floor_sheet
        (date, symbol, buyer_broker, seller_broker, quantity, rate, amount, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    n = len(rows)
    conn.close()
    return n


# ════════════════════════════════════════════════════════════════════════════
# METHOD 1: NEPSE Unofficial API  (today / recent, fast)
# ════════════════════════════════════════════════════════════════════════════
def fetch_today_via_api():
    """Fetch full market floor sheet for today using NepseUnofficialApi."""
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
            if len(tds) < 6:
                continue
            try:
                # Columns: # | Symbol | Buyer | Seller | Qty | Rate | Amount
                rows.append({
                    "symbol":        tds[1].upper(),
                    "buyer_broker":  _int(tds[2]),
                    "seller_broker": _int(tds[3]),
                    "quantity":      _float(tds[4]),
                    "rate":          _float(tds[5]),
                    "amount":        _float(tds[6]) if len(tds) > 6 else round(_float(tds[4]) * _float(tds[5]), 2),
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

def fetch_historical_via_merolagani(date_str_ymd):
    """
    Fetch full market floor sheet for one date from merolagani.com/Floorsheet.aspx
    date_str_ymd: YYYY-MM-DD
    Returns: number of rows saved
    """
    dt = datetime.strptime(date_str_ymd, "%Y-%m-%d")
    ml_date    = dt.strftime("%m/%d/%Y")  # MM/DD/YYYY
    fetched_at = datetime.now().isoformat()

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
        payload["ctl00$ContentPlaceHolder1$txtFloorsheetDateFilter"]     = ml_date
        payload["ctl00$ContentPlaceHolder1$txtBuyerBrokerCodeFilter"]    = ""
        payload["ctl00$ContentPlaceHolder1$txtSellerBrokerCodeFilter"]   = ""
        payload["ctl00$ContentPlaceHolder1$ASCompanyFilter$hdnAutoSuggest"] = "0"
        payload["ctl00$ContentPlaceHolder1$ASCompanyFilter$txtAutoSuggest"]  = ""
        payload["ctl00$ContentPlaceHolder1$PagerControl1$hdnPCID"]        = "PC1"
        payload["ctl00$ContentPlaceHolder1$PagerControl1$hdnCurrentPage"] = "0"
        payload["ctl00$ContentPlaceHolder1$PagerControl2$hdnPCID"]        = "PC2"
        payload["ctl00$ContentPlaceHolder1$PagerControl2$hdnCurrentPage"] = "0"

        time.sleep(random.uniform(1.0, 2.0))
        r1 = SESSION.post(ML_URL, data=payload, headers={
            "Content-Type":            "application/x-www-form-urlencoded",
            "Referer":                 ML_URL,
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest":          "document",
            "Sec-Fetch-Mode":          "navigate",
            "Sec-Fetch-Site":          "same-origin",
        }, timeout=30)
        r1.raise_for_status()

        soup1 = BeautifulSoup(r1.text, "html.parser")

        # Check for "no data" message
        page_text = soup1.get_text()
        if "Could not find floorsheet" in page_text or "No record" in page_text:
            print(f"  [{date_str_ymd}] No data (holiday or data not available)")
            return 0

        page_rows, total_pages = _ml_parse_table(soup1)

        if not page_rows:
            print(f"  [{date_str_ymd}] No table rows found in response")
            return 0

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
        return saved

    except Exception as e:
        print(f"  [{date_str_ymd}] ERROR: {e}")
        traceback.print_exc()
        return 0


# ════════════════════════════════════════════════════════════════════════════
# OHLC V COMPUTATION
# ════════════════════════════════════════════════════════════════════════════
def compute_daily_price(dates=None):
    """
    Compute Open, High, Low, Close, Volume, VWAP from floor_sheet for new dates.
    Only processes dates not already in daily_price.
    Note: Open/Close are naive (first/last trade in the db insertion order).
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

    for d in dates:
        conn = get_db()
        try:
            # Delete any partial rows for this date first
            conn.execute("DELETE FROM daily_price WHERE date=?", (d,))

            # Use sqlite window functions/group_concat to get open/close safely
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
def run_daily_update():
    """
    Called every trading day after market close (4 PM NST).
    Fetches today's floor sheet via NEPSE API → saves → computes broker_summary.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    if _is_nepse_holiday(datetime.now()):
        print(f"[Daily] {today} is a holiday, skipping.")
        return

    existing = _dates_already_in_db()
    if today in existing:
        print(f"[Daily] {today} already in DB, recomputing broker_summary only.")
    else:
        saved = fetch_today_via_api()
        if not saved:
            print(f"[Daily] No floor sheet data for {today}")
            return

    compute_broker_summary()
    compute_daily_price()
    print(f"[Daily] Update complete for {today}")


# ════════════════════════════════════════════════════════════════════════════
# HISTORICAL BACKFILL  (fill the Oct 2021 → present gap)
# ════════════════════════════════════════════════════════════════════════════
def run_historical_backfill(days_back=None, start_date=None):
    """
    Fills the gap from Oct 2021 → today using Merolagani scraping.
    days_back: number of days to go back from today
    start_date: YYYY-MM-DD string to start from (overrides days_back)
    """
    ensure_schema()
    existing = _dates_already_in_db()

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
        if not _is_nepse_holiday(datetime.combine(d, datetime.min.time())) and d_str not in existing:
            dates_to_fetch.append(d_str)
        d += timedelta(days=1)

    print(f"[Backfill] {len(dates_to_fetch)} dates to fetch ({start} → {today})")

    total = 0
    batch = 0
    for i, date_str in enumerate(dates_to_fetch):
        saved = fetch_historical_via_merolagani(date_str)
        total += saved
        batch += 1

        # Compute summaries in batches to keep memory low
        if batch >= 20:
            compute_broker_summary()
            compute_daily_price()
            batch = 0

        # Random pause every 50 requests to avoid rate limiting
        if (i + 1) % 50 == 0:
            pause = random.uniform(10, 20)
            print(f"[Backfill] Pausing {pause:.0f}s to avoid rate limit...")
            time.sleep(pause)

    # Final updates
    compute_broker_summary()
    compute_daily_price()
    print(f"\n[Backfill] Complete! Total floor sheet rows saved: {total:,}")



# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Floor sheet pipeline")
    parser.add_argument("--daily",   action="store_true", help="Fetch today via NEPSE API")
    parser.add_argument("--days",    type=int,            help="Backfill last N days via Merolagani")
    parser.add_argument("--from",    dest="start",        help="Backfill from YYYY-MM-DD via Merolagani")
    parser.add_argument("--all",     action="store_true", help="Full backfill Oct 2021 → today")
    parser.add_argument("--summary", action="store_true", help="Recompute broker_summary only")
    args = parser.parse_args()

    ensure_schema()

    if args.daily:
        run_daily_update()
    elif args.summary:
        compute_broker_summary()
        compute_daily_price()
    elif args.days:

        run_historical_backfill(days_back=args.days)
    elif args.start:
        run_historical_backfill(start_date=args.start)
    elif args.all:
        run_historical_backfill()
    else:
        # Default: daily update
        run_daily_update()

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
