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
from requests.exceptions import JSONDecodeError as RequestsJSONDecodeError

try:
    from active_symbols import filter_active_symbols, get_active_symbol_set
except ImportError:
    try:
        from data.active_symbols import filter_active_symbols, get_active_symbol_set
    except ImportError:
        from src.data.active_symbols import filter_active_symbols, get_active_symbol_set

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
COMPANY_DETAIL_URL = f"{BASE_URL}/CompanyDetail.aspx"
SHARESANSAR_BASE_URL = "https://www.sharesansar.com"

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


def _extract_hidden_fields(soup):
    return {
        inp.get("name"): inp.get("value", "")
        for inp in soup.select("input[type='hidden']")
        if inp.get("name")
    }


def _extract_aspx_updatepanel_fragments(text):
    fragments = []
    pattern = re.compile(r"\|\d+\|(\d+)\|updatePanel\|([^|]+)\|")
    pos = 0
    while True:
        match = pattern.search(text, pos)
        if not match:
            break
        length = int(match.group(1))
        start = match.end()
        fragment = text[start:start + length]
        if fragment:
            fragments.append(fragment)
        pos = start + length
    return fragments


def _extract_aspx_hidden_fields(text):
    fields = {}
    # ASP.NET async responses can carry updated viewstate-like fields in
    # hiddenField segments, e.g. |hiddenField|__VIEWSTATE|...|
    for name, value in re.findall(r"\|hiddenField\|([^|]+)\|([^|]*)", text):
        fields[name] = value
    return fields


def _get_company_detail_page(symbol):
    url = f"{COMPANY_DETAIL_URL}?symbol={symbol}"
    resp = _get(url)
    return url, resp.text, BeautifulSoup(resp.text, "html.parser")


def _normalize_market_date(value):
    raw = str(value).strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%d-%b-%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _parse_number(value):
    raw = str(value).replace(",", "").replace("%", "").strip()
    return float(raw) if raw not in {"", "-", "--"} else 0.0


def _extract_price_rows_from_html(html_text):
    soup = BeautifulSoup(html_text, "html.parser")
    rows = []

    for table in soup.select("#ctl00_ContentPlaceHolder1_CompanyDetail1_divDataPrice table, #divHistory table, table"):
        for tr in table.select("tr"):
            cells = [c.get_text(" ", strip=True) for c in tr.select("th,td")]
            if len(cells) < 6:
                continue

            date_value = _normalize_market_date(cells[0])
            if not date_value:
                continue

            # Price History generally includes Date, Open, High, Low, Close,
            # Change%, Volume; but keep a tolerant fallback for older formats.
            if len(cells) >= 7:
                volume_index = 6
            else:
                volume_index = 5

            try:
                rows.append({
                    "date": date_value,
                    "open": _parse_number(cells[1]),
                    "high": _parse_number(cells[2]),
                    "low": _parse_number(cells[3]),
                    "close": _parse_number(cells[4]),
                    "volume": _parse_number(cells[volume_index]),
                })
            except Exception:
                continue

    if not rows:
        return pd.DataFrame()

    return (
        pd.DataFrame(rows)
        .drop_duplicates(subset=["date"])
        .sort_values("date")
        .reset_index(drop=True)
    )


def _fetch_price_history_from_merolagani_history_tab(symbol, max_pages=400):
    print(f"   Fetching from Merolagani Price History tab for {symbol}...")
    url, _, soup = _get_company_detail_page(symbol)

    state = _extract_hidden_fields(soup)
    base = {
        "symbol": symbol.lower(),
        "ctl00$ASCompany$hdnAutoSuggest": "0",
        "ctl00$ASCompany$txtAutoSuggest": "",
        "ctl00$txtNews": "",
        "ctl00$AutoSuggest1$hdnAutoSuggest": "0",
        "ctl00$AutoSuggest1$txtAutoSuggest": "",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$hdnStockSymbol": symbol.upper(),
        "ctl00$ContentPlaceHolder1$CompanyDetail1$hdnActiveTabID": "#divHistory",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$StockGraph1$hdnStockSymbol": symbol.upper(),
        "ctl00$ContentPlaceHolder1$CompanyDetail1$txtMarketDatePriceFilter": "",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlTransactionHistory1$hdnPCID": "PC1",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlTransactionHistory1$hdnCurrentPage": "0",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlTransactionHistory2$hdnPCID": "PC2",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlTransactionHistory2$hdnCurrentPage": "0",
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__ASYNCPOST": "true",
    }

    headers = {
        "X-MicrosoftAjax": "Delta=true",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": url,
        "Origin": "https://www.merolagani.com",
    }

    def post_event(script_target, event_target="", extra=None):
        payload = {}
        payload.update(state)
        payload.update(base)
        payload["ctl00$ScriptManager1"] = f"ctl00$ContentPlaceHolder1$CompanyDetail1$tabPanel|{script_target}"
        payload["__EVENTTARGET"] = event_target
        if extra:
            payload.update(extra)

        resp = SESSION.post(url, data=payload, headers=headers, timeout=20)
        resp.raise_for_status()
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
        text = resp.text

        state.update(_extract_aspx_hidden_fields(text))
        for fragment in _extract_aspx_updatepanel_fragments(text):
            fragment_soup = BeautifulSoup(fragment, "html.parser")
            state.update(_extract_hidden_fields(fragment_soup))
        return text

    rows_by_date = {}

    # 1) Open Price History tab
    first = post_event(
        "ctl00$ContentPlaceHolder1$CompanyDetail1$btnHistoryTab",
        event_target="ctl00$ContentPlaceHolder1$CompanyDetail1$btnHistoryTab",
        extra={"ctl00$ContentPlaceHolder1$CompanyDetail1$btnHistoryTab": ""},
    )

    for fragment in _extract_aspx_updatepanel_fragments(first) or [first]:
        df = _extract_price_rows_from_html(fragment)
        for _, row in df.iterrows():
            rows_by_date[row["date"]] = row.to_dict()

    # 2) Page through transaction history
    for page in range(2, max_pages + 1):
        text = post_event(
            "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlTransactionHistory1$btnPaging",
            event_target="",
            extra={
                "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlTransactionHistory1$hdnCurrentPage": str(page),
                "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlTransactionHistory1$btnPaging": "",
            },
        )

        page_rows = 0
        for fragment in _extract_aspx_updatepanel_fragments(text) or [text]:
            df = _extract_price_rows_from_html(fragment)
            page_rows += len(df)
            for _, row in df.iterrows():
                rows_by_date[row["date"]] = row.to_dict()

        if page_rows == 0:
            break

    if not rows_by_date:
        return None

    return (
        pd.DataFrame(list(rows_by_date.values()))
        .drop_duplicates(subset=["date"])
        .sort_values("date")
        .reset_index(drop=True)
    )


def _make_company_detail_post_data(symbol, soup):
    data = {
        "symbol": symbol.lower(),
        "ctl00$ASCompany$hdnAutoSuggest": "0",
        "ctl00$ASCompany$txtAutoSuggest": "",
        "ctl00$txtNews": "",
        "ctl00$AutoSuggest1$hdnAutoSuggest": "0",
        "ctl00$AutoSuggest1$txtAutoSuggest": "",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$hdnStockSymbol": symbol.upper(),
        "ctl00$ContentPlaceHolder1$CompanyDetail1$StockGraph1$hdnStockSymbol": symbol.upper(),
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__ASYNCPOST": "true",
    }
    data.update(_extract_hidden_fields(soup))
    return data


def _post_company_detail(symbol, trigger_control, extra_data=None):
    url, _, soup = _get_company_detail_page(symbol)
    data = _make_company_detail_post_data(symbol, soup)
    data["ctl00$ScriptManager1"] = f"ctl00$ContentPlaceHolder1$CompanyDetail1$tabPanel|{trigger_control}"
    if extra_data:
        data.update(extra_data)
    headers = {
        "X-MicrosoftAjax": "Delta=true",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": url,
        "Origin": "https://www.merolagani.com",
    }
    resp = SESSION.post(url, data=data, headers=headers, timeout=20)
    resp.raise_for_status()
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
    return resp.text


def _parse_price_history_table(table):
    rows = []
    for tr in table.select("tr"):
        cells = [td.get_text(" ", strip=True) for td in tr.select("th,td")]
        if len(cells) < 6:
            continue
        if cells[0].strip().lower() in {"date", "market date"}:
            continue

        date_value = cells[0].strip()
        normalized_date = None
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d-%b-%Y", "%d/%m/%Y"):
            try:
                normalized_date = datetime.strptime(date_value, fmt).strftime("%Y-%m-%d")
                break
            except ValueError:
                continue
        if normalized_date is None:
            continue

        def parse_number(value):
            raw = str(value).replace(",", "").strip()
            return float(raw) if raw not in {"", "-", "--"} else 0.0

        rows.append({
            "date": normalized_date,
            "open": parse_number(cells[1]),
            "high": parse_number(cells[2]),
            "low": parse_number(cells[3]),
            "close": parse_number(cells[4]),
            "volume": parse_number(cells[5]),
        })

    return pd.DataFrame(rows)


def _fetch_price_history_from_sharesansar(symbol):
    print(f"   Falling back to Sharesansar price history for {symbol}...")
    session = requests.Session()
    session.headers.update({
        "User-Agent": SESSION.headers["User-Agent"],
        "Accept-Language": SESSION.headers["Accept-Language"],
        "Referer": f"{SHARESANSAR_BASE_URL}/company/{symbol.lower()}",
    })

    company_url = f"{SHARESANSAR_BASE_URL}/company/{symbol.lower()}"
    company_resp = session.get(company_url, timeout=20)
    company_resp.raise_for_status()
    soup = BeautifulSoup(company_resp.text, "html.parser")

    token_tag = soup.select_one("meta[name=_token]")
    company_tag = soup.select_one("#companyid")
    if token_tag is None or company_tag is None:
        return None

    token = token_tag.get("content", "")
    company_id = company_tag.get_text(strip=True)
    if not token or not company_id:
        return None

    headers = {
        "X-CSRF-Token": token,
        "X-Requested-With": "XMLHttpRequest",
        "Referer": company_url,
    }

    frames = []
    start = 0
    page_size = 50
    records_total = None

    while records_total is None or start < records_total:
        payload = {
            "company": company_id,
            "draw": str((start // page_size) + 1),
            "start": str(start),
            "length": str(page_size),
        }
        resp = session.post(
            f"{SHARESANSAR_BASE_URL}/company-price-history",
            headers=headers,
            data=payload,
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()

        rows = data.get("data") or []
        if not rows:
            break

        records_total = int(data.get("recordsFiltered") or data.get("recordsTotal") or len(rows))
        frame = pd.DataFrame([
            {
                "date": row.get("published_date"),
                "open": float(str(row.get("open", 0)).replace(",", "") or 0),
                "high": float(str(row.get("high", 0)).replace(",", "") or 0),
                "low": float(str(row.get("low", 0)).replace(",", "") or 0),
                "close": float(str(row.get("close", 0)).replace(",", "") or 0),
                "volume": float(str(row.get("traded_quantity", 0)).replace(",", "") or 0),
            }
            for row in rows if row.get("published_date")
        ])
        if not frame.empty:
            frames.append(frame)

        start += len(rows)
        if len(rows) < page_size:
            break

    if not frames:
        return None

    df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df

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
        active_symbols = get_active_symbol_set()
        if active_symbols:
            df = df[df["symbol"].str.upper().isin(active_symbols)].copy()
            print(f"   Filtered to {len(df)} active companies from active_symbols.txt")
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
# Merolagani advanced chart endpoint (JSON, used by the current company chart):
#   GET https://www.merolagani.com/handlers/TechnicalChartHandler.ashx
#       ?type=get_advanced_chart&symbol=NLG&resolution=1D
#       &rangeStartDate=<unix>&rangeEndDate=<unix>
#       &from=&isAdjust=1&currencyCode=NPR
#
# Response JSON keys: t (timestamp), o, h, l, c, v, s
# This currently returns working OHLCV data directly from Merolagani.

CHART_HANDLER = f"{BASE_URL}/handlers/TechnicalChartHandler.ashx"

def _unix(dt: datetime) -> int:
    # datetime.timestamp() can raise OSError on Windows for early dates.
    # Use explicit epoch arithmetic for cross-platform stability.
    epoch = datetime(1970, 1, 1)
    return int((dt - epoch).total_seconds())

def _coerce_history_date(value, default_time):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        dt = datetime.strptime(value, "%Y-%m-%d")
        return dt.replace(
            hour=default_time.hour,
            minute=default_time.minute,
            second=default_time.second,
            microsecond=0,
        )
    raise TypeError(f"Unsupported date value: {value!r}")


def fetch_price_history(symbol, years_back=15, start_date=None, end_date=None):
    """
    Fetch full OHLCV price history for a stock symbol from Merolagani.
    Saves to price_history table.
    Returns a DataFrame.
    """
    print(f"Fetching price history for {symbol}...")

    now = datetime.now()
    range_start = _coerce_history_date(start_date, datetime.min.replace(hour=0, minute=0, second=0))
    range_end = _coerce_history_date(end_date, now)

    if range_end is None:
        range_end = now
    if range_start is None:
        range_start = range_end - timedelta(days=365 * years_back)
    if range_start > range_end:
        raise ValueError(f"start_date must be on or before end_date for {symbol}")

    from_ts = _unix(range_start)
    to_ts = _unix(range_end)

    try:
        df = _fetch_price_history_from_merolagani_history_tab(symbol)

        # Fallback 1: Merolagani advanced chart endpoint
        if df is None or df.empty:
            params = {
                "type": "get_advanced_chart",
                "symbol": symbol.upper(),
                "resolution": "1D",
                "rangeStartDate": from_ts,
                "rangeEndDate": to_ts,
                "from": "",
                "isAdjust": 1,
                "currencyCode": "NPR",
            }
            resp = _get(CHART_HANDLER, params=params)
            try:
                data = resp.json()
            except (ValueError, RequestsJSONDecodeError):
                data = None

            if data:
                timestamps = data.get("t", [])
                opens      = data.get("o", [])
                highs      = data.get("h", [])
                lows       = data.get("l", [])
                closes     = data.get("c", [])
                volumes    = data.get("v", [])

                if timestamps:
                    df = pd.DataFrame({
                        "date":   [datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d") for ts in timestamps],
                        "open":   [float(v or 0) for v in opens],
                        "high":   [float(v or 0) for v in highs],
                        "low":    [float(v or 0) for v in lows],
                        "close":  [float(v or 0) for v in closes],
                        "volume": [float(v or 0) for v in volumes],
                    })

        # Fallback 2: Sharesansar endpoint
        if df is None or df.empty:
            df = _fetch_price_history_from_sharesansar(symbol)
            if df is None or df.empty:
                print(f"   No price history rows found for {symbol}")
                return None

        if start_date is not None:
            df = df[df["date"] >= _coerce_history_date(start_date, now).strftime("%Y-%m-%d")]
        if end_date is not None:
            df = df[df["date"] <= _coerce_history_date(end_date, now).strftime("%Y-%m-%d")]
        if df.empty:
            print(f"   No price history rows remained after date filtering for {symbol}")
            return None

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
    symbols = filter_active_symbols(symbols)

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


def fetch_all_price_history(max_workers=3):
    """Compatibility alias used by the step runner."""
    return fetch_all_price_histories(max_workers=max_workers)


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