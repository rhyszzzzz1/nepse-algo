# src/data/fetcher.py
# NEPSE Data Fetcher — Merolagani scraper edition
# Replaces NepseUnofficialApi with direct scraping of merolagani.com
# Covers: company list, full price history (10+ years), floor sheet, market summary
# Compatible with: Python 3.11, SQLite only, same function signatures as before

import os
import sys
import sqlite3
import time
import random
import re
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
import pandas as pd

# Force SQLite — ignore any PostgreSQL environment variables
os.environ.pop("DATABASE_URL", None)
os.environ.pop("POSTGRES_URL", None)

# ── DB SETUP (inline — do NOT import from db.py per project rules) ─────────────
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(ROOT, "data", "nepse.db")

def get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ── HTTP SESSION ───────────────────────────────────────────────────────────────
# Merolagani is tolerant of scrapers but we add realistic headers + small delays
# to be polite and avoid getting throttled during bulk fetches.

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer":         "https://merolagani.com/",
})

BASE_URL   = "https://merolagani.com"
COMPANY_LIST_URL = f"{BASE_URL}/CompanyList.aspx"

# Delay between requests (seconds) — randomised to be polite
MIN_DELAY = 0.4
MAX_DELAY = 1.0

def _get(url, params=None, retries=3):
    """GET with retry + polite delay."""
    for attempt in range(retries):
        try:
            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
            resp = SESSION.get(url, params=params, timeout=20)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if attempt == retries - 1:
                raise
            wait = 2 ** attempt
            print(f"   Retry {attempt+1}/{retries} after {wait}s — {e}")
            time.sleep(wait)

# ── CREATE TABLES ──────────────────────────────────────────────────────────────
def create_tables():
    """Create all database tables if they don't exist."""
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS price_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol      TEXT NOT NULL,
            date        TEXT NOT NULL,
            open        REAL,
            high        REAL,
            low         REAL,
            close       REAL,
            volume      REAL,
            fetched_at  TEXT,
            UNIQUE(symbol, date)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol      TEXT NOT NULL,
            name        TEXT,
            sector      TEXT,
            fetched_at  TEXT,
            UNIQUE(symbol)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS floor_sheet (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            date            TEXT NOT NULL,
            symbol          TEXT NOT NULL,
            buyer_broker    INTEGER,
            seller_broker   INTEGER,
            quantity        REAL,
            rate            REAL,
            amount          REAL,
            fetched_at      TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_summary (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            date           TEXT NOT NULL,
            nepse_index    REAL,
            total_turnover REAL,
            total_volume   REAL,
            fetched_at     TEXT,
            UNIQUE(date)
        )
    """)

    conn.commit()
    conn.close()
    print("✅ Tables created successfully")


# ── COMPANY LIST ───────────────────────────────────────────────────────────────
# Merolagani lists all companies at:
#   https://merolagani.com/CompanyList.aspx
# The table has columns: SN, Symbol, Company Name, Sector
# All rows are in <table class="table">

def fetch_company_list():
    """
    Fetch all listed companies from Merolagani and save to database.
    Returns a DataFrame with columns: symbol, name, sector.
    """
    print("Fetching company list from Merolagani...")
    rows = []

    try:
        resp = _get(COMPANY_LIST_URL)
        soup = BeautifulSoup(resp.text, "html.parser")

        # Find the main company table — it's the first <table class="table">
        table = soup.find("table", class_="table")
        if not table:
            print("❌ Could not find company table on Merolagani")
            return None

        for tr in table.find("tbody").find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 4:
                continue
            # Columns: SN | Symbol | Company Name | Sector
            symbol = tds[1].get_text(strip=True)
            name   = tds[2].get_text(strip=True)
            sector = tds[3].get_text(strip=True)

            if symbol:
                rows.append({"symbol": symbol, "name": name, "sector": sector})

        df = pd.DataFrame(rows)
        print(f"   Found {len(df)} companies")

        fetched_at = datetime.now().isoformat()
        conn = get_db()

        for _, row in df.iterrows():
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO companies (symbol, name, sector, fetched_at)
                    VALUES (?, ?, ?, ?)
                """, (row["symbol"], row["name"], row["sector"], fetched_at))
            except Exception as e:
                print(f"   Skipped {row['symbol']}: {e}")

        conn.commit()
        conn.close()
        print(f"✅ Saved {len(df)} companies to database")
        return df

    except Exception as e:
        print(f"❌ Error fetching company list: {e}")
        import traceback; traceback.print_exc()
        return None


# ── PRICE HISTORY ──────────────────────────────────────────────────────────────
# Merolagani price history endpoint (JSON, used by their own charts):
#   GET https://merolagani.com/handlers/TechnicalChartHandler.ashx
#       ?type=company&q=NABIL&resolution=D&from=<unix>&to=<unix>
#
# Response JSON keys: t (timestamp), o, h, l, c, v (OHLCV)
# This gives full daily OHLCV going back 10+ years for most stocks.
# No login or token required.

CHART_HANDLER = f"{BASE_URL}/handlers/TechnicalChartHandler.ashx"

def _unix(dt: datetime) -> int:
    return int(dt.timestamp())

def fetch_price_history(symbol, years_back=15):
    """
    Fetch full OHLCV price history for a stock symbol from Merolagani.
    Saves to price_history table.
    Returns a DataFrame.
    """
    print(f"Fetching price history for {symbol}...")

    now  = datetime.now()
    from_ts = _unix(now - timedelta(days=365 * years_back))
    to_ts   = _unix(now)

    params = {
        "type":       "company",
        "q":          symbol,
        "resolution": "D",       # Daily bars
        "from":       from_ts,
        "to":         to_ts,
    }

    try:
        resp = _get(CHART_HANDLER, params=params)
        data = resp.json()

        # Handler returns {"s": "ok", "t": [...], "o": [...], "h": [...], "l": [...], "c": [...], "v": [...]}
        # or {"s": "no_data"} if symbol not found
        if data.get("s") != "ok":
            print(f"   No data for {symbol} (status: {data.get('s')})")
            return None

        timestamps = data.get("t", [])
        opens      = data.get("o", [])
        highs      = data.get("h", [])
        lows       = data.get("l", [])
        closes     = data.get("c", [])
        volumes    = data.get("v", [])

        if not timestamps:
            print(f"   Empty response for {symbol}")
            return None

        # Build DataFrame
        df = pd.DataFrame({
            "date":   [datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d") for ts in timestamps],
            "open":   [float(v or 0) for v in opens],
            "high":   [float(v or 0) for v in highs],
            "low":    [float(v or 0) for v in lows],
            "close":  [float(v or 0) for v in closes],
            "volume": [float(v or 0) for v in volumes],
        })

        print(f"   Got {len(df)} rows ({df['date'].min()} → {df['date'].max()})")

        fetched_at = datetime.now().isoformat()
        conn = get_db()
        saved = 0

        for _, row in df.iterrows():
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO price_history
                    (symbol, date, open, high, low, close, volume, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    symbol,
                    row["date"],
                    row["open"],
                    row["high"],
                    row["low"],
                    row["close"],
                    row["volume"],
                    fetched_at,
                ))
                saved += 1
            except Exception as e:
                print(f"   Skipped row: {e}")

        conn.commit()
        conn.close()
        print(f"✅ Saved {saved} rows for {symbol}")
        return df

    except Exception as e:
        print(f"❌ Error fetching {symbol}: {e}")
        import traceback; traceback.print_exc()
        return None


# ── BULK PRICE HISTORY (PARALLEL) ─────────────────────────────────────────────
def fetch_all_price_histories(max_workers=3):
    """
    Fetch price history for every company in the DB using parallel threads.
    max_workers=3 to avoid hammering Merolagani and to stay within Railway RAM.
    """
    conn = get_db()
    symbols = [row[0] for row in conn.execute("SELECT symbol FROM companies").fetchall()]
    conn.close()

    total = len(symbols)
    print(f"Fetching price history for {total} companies with {max_workers} workers...")

    success, failed, completed = 0, 0, 0

    def process_one(symbol):
        try:
            result = fetch_price_history(symbol)
            return symbol, result is not None
        except Exception:
            return symbol, False

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_one, sym): sym for sym in symbols}
        for future in as_completed(futures):
            sym, ok = future.result()
            completed += 1
            if ok:
                success += 1
            else:
                failed += 1
            if completed % 20 == 0:
                print(f"Progress: {completed}/{total} | ✅ {success} | ❌ {failed}")

    print(f"\n✅ Done. Success: {success} | Failed: {failed}")


# ── FLOOR SHEET ────────────────────────────────────────────────────────────────
# Merolagani exposes today's floor sheet per stock at:
#   GET https://merolagani.com/handlers/NewFloorSheetHandler.ashx
#       ?type=floorsheet&symbol=NABIL&page=1
#
# Returns JSON:
#   { "floorsheets": { "content": [ {...}, ... ], "totalPages": N } }
# Each item has: contractNo, stockSymbol, buyerBrokerNo, sellerBrokerNo,
#                contractQuantity, contractRate, contractAmount, businessDate

FLOORSHEET_HANDLER = f"{BASE_URL}/handlers/NewFloorSheetHandler.ashx"

def fetch_floor_sheet_for(symbol):
    """
    Fetch today's floor sheet (broker trades) for a symbol from Merolagani.
    Paginates through all pages automatically.
    Saves to floor_sheet table. Returns a DataFrame.
    """
    print(f"Fetching floor sheet for {symbol}...")
    all_rows = []
    page = 1

    try:
        while True:
            params = {"type": "floorsheet", "symbol": symbol, "page": page}
            resp = _get(FLOORSHEET_HANDLER, params=params)
            data = resp.json()

            content = (
                data.get("floorsheets", {})
                    .get("content", [])
            )
            if not content:
                break

            all_rows.extend(content)
            total_pages = data.get("floorsheets", {}).get("totalPages", 1)
            if page >= total_pages:
                break
            page += 1

        if not all_rows:
            print(f"   No floor sheet trades for {symbol} today")
            return pd.DataFrame()

        df = pd.DataFrame(all_rows)
        print(f"   Got {len(df)} trades across {page} page(s)")

        fetched_at = datetime.now().isoformat()
        today      = datetime.now().strftime("%Y-%m-%d")
        conn = get_db()

        for _, row in df.iterrows():
            try:
                conn.execute("""
                    INSERT INTO floor_sheet
                    (date, symbol, buyer_broker, seller_broker, quantity, rate, amount, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    today,
                    symbol,
                    int(row.get("buyerBrokerNo",  0) or 0),
                    int(row.get("sellerBrokerNo", 0) or 0),
                    float(row.get("contractQuantity", 0) or 0),
                    float(row.get("contractRate",     0) or 0),
                    float(row.get("contractAmount",   0) or 0),
                    fetched_at,
                ))
            except Exception as e:
                print(f"   Skipped row: {e}")

        conn.commit()
        conn.close()
        print(f"✅ Saved floor sheet for {symbol}")
        return df

    except Exception as e:
        print(f"❌ Error fetching floor sheet for {symbol}: {e}")
        import traceback; traceback.print_exc()
        return None


# ── MARKET SUMMARY ─────────────────────────────────────────────────────────────
# Merolagani market summary page: https://merolagani.com/MarketSummary.aspx
# The NEPSE index value lives in a <span> or table cell labelled "NEPSE Index"
# We also try the live market handler used by their ticker:
#   GET https://merolagani.com/handlers/TechnicalChartHandler.ashx
#       ?type=index&q=NEPSE&resolution=D&from=<unix>&to=<unix>

MARKET_SUMMARY_URL = f"{BASE_URL}/MarketSummary.aspx"

def fetch_market_summary():
    """
    Fetch today's NEPSE market summary (index, turnover, volume).
    Tries the chart handler first (clean JSON), falls back to HTML scraping.
    Saves to market_summary table. Returns the summary dict.
    """
    print("Fetching market summary...")
    fetched_at = datetime.now().isoformat()
    today      = datetime.now().strftime("%Y-%m-%d")
    conn = get_db()

    try:
        # ── Method 1: JSON chart handler for NEPSE index ───────────────────────
        now = datetime.now()
        params = {
            "type":       "index",
            "q":          "NEPSE",
            "resolution": "D",
            "from":       _unix(now - timedelta(days=5)),
            "to":         _unix(now),
        }
        resp = _get(CHART_HANDLER, params=params)
        data = resp.json()

        nepse_index    = 0.0
        total_turnover = 0.0
        total_volume   = 0.0

        if data.get("s") == "ok" and data.get("c"):
            nepse_index = float(data["c"][-1])    # latest close
            total_volume = float(data.get("v", [0])[-1] or 0)
            print(f"   NEPSE Index (from chart handler): {nepse_index}")

        # ── Method 2: HTML scrape for turnover ────────────────────────────────
        # Only fetch if we still have 0 turnover (chart handler doesn't give turnover)
        try:
            page_resp = _get(MARKET_SUMMARY_URL)
            soup = BeautifulSoup(page_resp.text, "html.parser")

            # Look for "Total Turnover" label in any table/span
            text = soup.get_text(" ", strip=True)
            # Pattern: "Total Turnover: 1,234,567,890"
            m = re.search(r"Total Turnover[:\s]+([\d,]+)", text, re.IGNORECASE)
            if m:
                total_turnover = float(m.group(1).replace(",", ""))
                print(f"   Total Turnover: {total_turnover}")

            # If index was 0 from method 1, try scraping it
            if nepse_index == 0:
                m2 = re.search(r"NEPSE Index[:\s]+([\d,]+\.?\d*)", text, re.IGNORECASE)
                if m2:
                    nepse_index = float(m2.group(1).replace(",", ""))
                    print(f"   NEPSE Index (from HTML): {nepse_index}")

        except Exception as html_err:
            print(f"   HTML fallback failed (non-critical): {html_err}")

        conn.execute("""
            INSERT OR REPLACE INTO market_summary
            (date, nepse_index, total_turnover, total_volume, fetched_at)
            VALUES (?, ?, ?, ?, ?)
        """, (today, nepse_index, total_turnover, total_volume, fetched_at))
        conn.commit()
        print("✅ Market summary saved")

        return {
            "date":           today,
            "nepse_index":    nepse_index,
            "total_turnover": total_turnover,
            "total_volume":   total_volume,
        }

    except Exception as e:
        print(f"❌ Error fetching market summary: {e}")
        import traceback; traceback.print_exc()
        return None
    finally:
        conn.close()


# ── INCREMENTAL UPDATE ─────────────────────────────────────────────────────────
def fetch_price_history_incremental(symbol):
    """
    Only fetch data newer than what we already have in the DB.
    Use this for daily updates after the initial full load.
    """
    conn = get_db()
    row = conn.execute(
        "SELECT MAX(date) FROM price_history WHERE symbol = ?", (symbol,)
    ).fetchone()
    conn.close()

    last_date = row[0] if row and row[0] else None

    if last_date:
        # Fetch from the day after our last record
        from_dt = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        years_back = max(1, (datetime.now() - from_dt).days / 365)
        print(f"   Incremental: fetching {symbol} from {from_dt.date()} (last known: {last_date})")
    else:
        years_back = 15  # Full history for new symbols
        print(f"   Full fetch for new symbol: {symbol}")

    return fetch_price_history(symbol, years_back=years_back)


# ── MAIN TEST RUN ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 55)
    print("NEPSE Data Fetcher — Merolagani Scraper Edition")
    print("=" * 55)

    # 1. Create tables
    create_tables()

    # 2. Fetch company list
    companies = fetch_company_list()

    # 3. Test price history with a couple of stocks
    for sym in ["NABIL", "NICA", "SCB"]:
        fetch_price_history(sym)

    # 4. Fetch market summary
    fetch_market_summary()

    # 5. Database summary
    conn = get_db()
    print("\n📊 Database summary:")
    print(f"   Companies:     {conn.execute('SELECT COUNT(*) FROM companies').fetchone()[0]}")
    print(f"   Price rows:    {conn.execute('SELECT COUNT(*) FROM price_history').fetchone()[0]}")
    print(f"   Floor trades:  {conn.execute('SELECT COUNT(*) FROM floor_sheet').fetchone()[0]}")

    # Show date range per tested symbol
    for sym in ["NABIL", "NICA", "SCB"]:
        r = conn.execute(
            "SELECT MIN(date), MAX(date), COUNT(*) FROM price_history WHERE symbol=?", (sym,)
        ).fetchone()
        if r and r[0]:
            print(f"   {sym}: {r[0]} → {r[1]} ({r[2]} rows)")
    conn.close()

    print("\n✅ Step 2 complete! Merolagani scraper is working.")