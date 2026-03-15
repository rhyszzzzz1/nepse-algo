# src/data/merolagani_floorsheet.py
# Fetches HISTORICAL floor sheet data from merolagani.com
# Pagination fix: confirmed from DevTools — page nav uses btnPaging submit button,
# NOT lbtnSearchFloorsheet. ScriptManager and EVENTTARGET differ between search and page.

import os
import re
import sqlite3
import time
import random
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup
import pandas as pd

os.environ.pop("DATABASE_URL", None)
os.environ.pop("POSTGRES_URL", None)

# ── DB ─────────────────────────────────────────────────────────────────────────
ROOT    = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(ROOT, "data", "nepse.db")

def get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ── SESSION ────────────────────────────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://merolagani.com/",
})

BASE_URL = "https://merolagani.com"

def _float(s):
    try:
        return float(str(s).replace(",", "").strip() or 0)
    except:
        return 0.0

def _int(s):
    try:
        return int(str(s).replace(",", "").strip() or 0)
    except:
        return 0


# ── GET page state ─────────────────────────────────────────────────────────────
def _get_viewstate(symbol):
    """GET CompanyDetail page, return dict of all hidden form fields."""
    url = f"{BASE_URL}/CompanyDetail.aspx"
    try:
        time.sleep(random.uniform(0.8, 1.5))
        r = SESSION.get(url, params={"symbol": symbol}, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        state = {}
        for inp in soup.find_all("input", {"type": "hidden"}):
            name  = inp.get("name", "")
            value = inp.get("value", "")
            if name:
                state[name] = value
        if "__VIEWSTATE" not in state:
            print(f"   ❌ No __VIEWSTATE for {symbol}")
            return None
        return state
    except Exception as e:
        print(f"   ❌ GET failed for {symbol}: {e}")
        return None


# ── BUILD BASE PAYLOAD ─────────────────────────────────────────────────────────
def _base_payload(symbol, date_str, state):
    """
    Common form fields shared by both search POST and page-nav POST.
    date_str format: MM/DD/YYYY
    """
    return {
        "ctl00$ASCompany$hdnAutoSuggest":       "0",
        "ctl00$ASCompany$txtAutoSuggest":       "",
        "ctl00$txtNews":                        "",
        "ctl00$AutoSuggest1$hdnAutoSuggest":    "0",
        "ctl00$AutoSuggest1$txtAutoSuggest":    "",

        "ctl00$ContentPlaceHolder1$CompanyDetail1$hdnStockSymbol":             symbol.upper(),
        "ctl00$ContentPlaceHolder1$CompanyDetail1$hdnActiveTabID":             "#divFloorsheet",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$StockGraph1$hdnStockSymbol": symbol.upper(),
        "ctl00$ContentPlaceHolder1$CompanyDetail1$ddlAncFiscalYearFilter":     "",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$txtNews":                    "",

        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlAnnouncement1$hdnPCID":        "PC1",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlAnnouncement1$hdnCurrentPage": "0",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlAnnouncement2$hdnPCID":        "PC2",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlAnnouncement2$hdnCurrentPage": "0",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlNews1$hdnPCID":                "PC1",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlNews1$hdnCurrentPage":         "0",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlNews2$hdnPCID":                "PC2",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlNews2$hdnCurrentPage":         "0",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$txtMarketDatePriceFilter":                 date_str,
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlTransactionHistory1$hdnPCID":        "PC1",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlTransactionHistory1$hdnCurrentPage": "0",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlTransactionHistory2$hdnPCID":        "PC2",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlTransactionHistory2$hdnCurrentPage": "0",

        "ctl00$ContentPlaceHolder1$CompanyDetail1$txtBuyerFilter":  "",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$txtSellerFilter": "",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$txtFloorsheetDateFilter": date_str,

        # PC2 always stays 0; only PC1 changes for page nav
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlFloorsheet2$hdnPCID":        "PC2",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlFloorsheet2$hdnCurrentPage": "0",

        "ctl00$ContentPlaceHolder1$CompanyDetail1$ddlFiscalYear":    "",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$ddlSectorFilter":  "0",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlAgm1$hdnPCID":  "PC1",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlAgm1$hdnCurrentPage": "0",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlAgm2$hdnPCID":  "PC2",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlAgm2$hdnCurrentPage": "0",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$ddlFiscalYear1":   "",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$ddlSectorFilter1": "0",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControl1$hdnPCID":  "PC1",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControl1$hdnCurrentPage": "0",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControl2$hdnPCID":  "PC2",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControl2$hdnCurrentPage": "0",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$ddlFiscalYear2":   "",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$ddlSectorFilter2": "0",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControl3$hdnPCID":  "PC3",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControl3$hdnCurrentPage": "0",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControl4$hdnPCID":  "PC4",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControl4$hdnCurrentPage": "0",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControl5$hdnPCID":  "PC5",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControl5$hdnCurrentPage": "0",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControl6$hdnPCID":  "PC6",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControl6$hdnCurrentPage": "0",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControl7$hdnPCID":  "PC7",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControl7$hdnCurrentPage": "0",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControl8$hdnPCID":  "PC8",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControl8$hdnCurrentPage": "0",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControl9$hdnPCID":  "PC9",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControl9$hdnCurrentPage": "0",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControl10$hdnPCID": "PC10",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControl10$hdnCurrentPage": "0",

        "__VIEWSTATE":          state.get("__VIEWSTATE", ""),
        "__VIEWSTATEGENERATOR": state.get("__VIEWSTATEGENERATOR", ""),
        "__EVENTVALIDATION":    state.get("__EVENTVALIDATION", ""),
        "__ASYNCPOST":          "true",
    }


# ── SEARCH POST (page 0, triggers the date search) ────────────────────────────
def _post_search(symbol, date_str, state):
    """POST the initial date search — returns page 0 HTML and total_pages."""
    payload = _base_payload(symbol, date_str, state)

    # Search-specific fields (confirmed from DevTools)
    payload.update({
        "ctl00$ScriptManager1": (
            "ctl00$ContentPlaceHolder1$CompanyDetail1$tabPanel"
            "|ctl00$ContentPlaceHolder1$CompanyDetail1$lbtnSearchFloorsheet"
        ),
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlFloorsheet1$hdnPCID":        "PC1",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlFloorsheet1$hdnCurrentPage": "0",
        "__EVENTTARGET":   "ctl00$ContentPlaceHolder1$CompanyDetail1$lbtnSearchFloorsheet",
        "__EVENTARGUMENT": "",
    })

    return _do_post(symbol, date_str, payload)


# ── PAGE NAV POST (pages 1+) ──────────────────────────────────────────────────
def _post_page(symbol, date_str, page_index, state):
    """
    POST a page navigation request.
    Key differences from search (confirmed from DevTools):
      - ScriptManager1 ends with PagerControlFloorsheet1$btnPaging
      - __EVENTTARGET is EMPTY
      - __EVENTARGUMENT is EMPTY
      - PagerControlFloorsheet1$btnPaging = "" (submit button value)
      - PagerControlFloorsheet1$hdnCurrentPage = page_index
    """
    payload = _base_payload(symbol, date_str, state)

    payload.update({
        "ctl00$ScriptManager1": (
            "ctl00$ContentPlaceHolder1$CompanyDetail1$tabPanel"
            "|ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlFloorsheet1$btnPaging"
        ),
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlFloorsheet1$hdnPCID":        "PC1",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlFloorsheet1$hdnCurrentPage": str(page_index),
        "__EVENTTARGET":   "",   # ← empty for submit button, confirmed
        "__EVENTARGUMENT": "",
        # The submit button itself must be included as a form field
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlFloorsheet1$btnPaging": "",
    })

    return _do_post(symbol, date_str, payload)


# ── SHARED POST LOGIC ─────────────────────────────────────────────────────────
def _do_post(symbol, date_str, payload):
    """Execute the POST and parse the HTML response. Returns (df, total_pages)."""
    url = f"{BASE_URL}/CompanyDetail.aspx"
    headers = {
        "X-MicrosoftAjax":  "Delta=true",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer":          f"{BASE_URL}/CompanyDetail.aspx?symbol={symbol}",
    }

    try:
        time.sleep(random.uniform(0.5, 1.0))
        r = SESSION.post(url, params={"symbol": symbol},
                         data=payload, headers=headers, timeout=30)
        r.raise_for_status()

        # Extract HTML from ASP.NET UpdatePanel response
        html = _extract_panel_html(r.text)
        if not html:
            return pd.DataFrame(), 0

        soup = BeautifulSoup(html, "html.parser")

        # Find floorsheet table — look for Buyer/Seller/Qty headers
        table = None
        for t in soup.find_all("table"):
            txt = t.get_text().lower()
            if "buyer" in txt and ("qty" in txt or "quantity" in txt):
                table = t
                break

        if not table:
            return pd.DataFrame(), 0

        # Parse rows — columns: # | Date | Transact.No. | Buyer | Seller | Qty | Rate | Amount
        rows = []
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 7:
                continue
            # Skip header rows
            if tds[0].find("th") or tds[1].get_text(strip=True).lower() in ("date", "#"):
                continue
            try:
                raw_date = tds[1].get_text(strip=True)
                rows.append({
                    "date":           raw_date,
                    "transaction_no": tds[2].get_text(strip=True),
                    "buyer_broker":   _int(tds[3].get_text(strip=True)),
                    "seller_broker":  _int(tds[4].get_text(strip=True)),
                    "quantity":       _float(tds[5].get_text(strip=True)),
                    "rate":           _float(tds[6].get_text(strip=True)),
                    "amount":         _float(tds[7].get_text(strip=True)) if len(tds) > 7 else 0.0,
                })
            except Exception:
                continue

        # Total pages from "Showing X - Y of Z records. [Total pages: N]"
        total_pages = 1
        page_text = soup.get_text()
        m = re.search(r"Total pages:\s*(\d+)", page_text, re.IGNORECASE)
        if m:
            total_pages = int(m.group(1))

        return pd.DataFrame(rows), total_pages

    except Exception as e:
        print(f"   ❌ POST failed: {e}")
        import traceback; traceback.print_exc()
        return pd.DataFrame(), 0


def _extract_panel_html(raw):
    """
    Extract HTML content from ASP.NET UpdatePanel delta response.
    Format: ...N|updatePanel|ID|<HTML of length N>|...
    """
    try:
        # Find all updatePanel segments and return the largest one (the tab panel)
        pattern = re.compile(r'(\d+)\|updatePanel\|([^|]+)\|')
        best_html = ""
        pos = 0
        while True:
            m = pattern.search(raw, pos)
            if not m:
                break
            length   = int(m.group(1))
            html_start = m.end()
            html_chunk = raw[html_start: html_start + length]
            if len(html_chunk) > len(best_html):
                best_html = html_chunk
            pos = html_start + length

        if best_html:
            return best_html

        # Fallback: grab everything between first <table and last </table>
        if "<table" in raw:
            start = raw.find("<table")
            end   = raw.rfind("</table>") + len("</table>")
            return raw[start:end]

    except Exception:
        pass
    return None


# ── FETCH ALL PAGES FOR ONE DATE ───────────────────────────────────────────────
def _fetch_date(symbol, date_str, state):
    """
    Fetch all pages for one date. Returns combined DataFrame.
    date_str: MM/DD/YYYY
    """
    # Page 0: use search POST
    df0, total_pages = _post_search(symbol, date_str, state)

    if df0.empty:
        return pd.DataFrame()

    all_dfs = [df0]

    # Pages 1+ use page-nav POST
    for page in range(1, total_pages):
        df_p, _ = _post_page(symbol, date_str, page, state)
        if df_p.empty:
            print(f"   ⚠️  Empty response on page {page} — stopping pagination")
            break
        # Verify this is actually a new page (not duplicate of page 0)
        if not df_p.empty and not df0.empty:
            first_row_same = (
                df_p.iloc[0]["transaction_no"] == df0.iloc[0]["transaction_no"]
            )
            if first_row_same:
                print(f"   ⚠️  Page {page} is duplicate of page 0 — stopping")
                break
        all_dfs.append(df_p)

    return pd.concat(all_dfs, ignore_index=True)


# ── NORMALISE DATE ─────────────────────────────────────────────────────────────
def _normalise_date(raw):
    """Convert any date format to YYYY-MM-DD."""
    raw = str(raw).strip().replace("/", "-")
    for fmt in ("%Y-%m-%d", "%m-%d-%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return raw  # return as-is if can't parse


# ── SAVE TO DB ─────────────────────────────────────────────────────────────────
def _save_floorsheet(symbol, df):
    if df.empty:
        return 0
    conn = get_db()
    fetched_at = datetime.now().isoformat()
    saved = 0
    for _, row in df.iterrows():
        try:
            conn.execute("""
                INSERT OR IGNORE INTO floor_sheet
                (date, symbol, buyer_broker, seller_broker, quantity, rate, amount, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                _normalise_date(row.get("date", "")),
                symbol,
                int(row.get("buyer_broker",  0) or 0),
                int(row.get("seller_broker", 0) or 0),
                float(row.get("quantity", 0) or 0),
                float(row.get("rate",     0) or 0),
                float(row.get("amount",   0) or 0),
                fetched_at,
            ))
            saved += 1
        except Exception:
            pass
    conn.commit()
    conn.close()
    return saved


# ── PUBLIC: FETCH ONE SYMBOL ───────────────────────────────────────────────────
def fetch_historical_floorsheet(symbol, days_back=365, skip_existing=True):
    """
    Fetch historical floor sheet for symbol going back N days.
    Skips Sun/Mon (NEPSE holidays — trades Sun–Thu; actually skips Fri+Sat).
    Skips dates already in DB if skip_existing=True.
    """
    print(f"\n{'='*55}")
    print(f"Historical floorsheet: {symbol} ({days_back} days back)")
    print(f"{'='*55}")

    # Existing dates in DB
    existing_dates = set()
    if skip_existing:
        conn = get_db()
        rows = conn.execute(
            "SELECT DISTINCT date FROM floor_sheet WHERE symbol=?", (symbol,)
        ).fetchall()
        existing_dates = {r[0] for r in rows}
        conn.close()
        if existing_dates:
            print(f"   Skipping {len(existing_dates)} dates already in DB")

    # Build date list (skip Fri=4 and Sat=5 — NEPSE holidays)
    today = datetime.now()
    dates = []
    for i in range(days_back):
        d = today - timedelta(days=i)
        if d.weekday() in (4, 5):
            continue
        db_date  = d.strftime("%Y-%m-%d")
        ml_date  = d.strftime("%m/%d/%Y")   # MM/DD/YYYY for merolagani
        if db_date not in existing_dates:
            dates.append((ml_date, db_date))

    print(f"   Dates to fetch: {len(dates)}")
    if not dates:
        print("   Nothing new to fetch.")
        return 0

    # Initial viewstate
    state = _get_viewstate(symbol)
    if not state:
        print(f"   ❌ Could not load page for {symbol}")
        return 0

    total_saved = dates_with_data = dates_empty = 0

    for idx, (ml_date, db_date) in enumerate(dates):
        # Refresh viewstate every 15 dates to avoid stale state errors
        if idx > 0 and idx % 15 == 0:
            new_state = _get_viewstate(symbol)
            if new_state:
                state = new_state

        df = _fetch_date(symbol, ml_date, state)

        if df.empty:
            dates_empty += 1
            if idx % 10 == 0:
                print(f"   [{idx+1}/{len(dates)}] {db_date}: no data")
        else:
            saved = _save_floorsheet(symbol, df)
            total_saved += saved
            dates_with_data += 1
            print(f"   [{idx+1}/{len(dates)}] {db_date}: {saved} trades saved ({len(df)} rows fetched)")

    print(f"\n✅ {symbol} done — {dates_with_data} dates, {total_saved} rows saved")
    return total_saved


# ── PUBLIC: BULK FETCH ─────────────────────────────────────────────────────────
def fetch_historical_floorsheet_bulk(symbols=None, days_back=365):
    """Fetch historical floorsheet for multiple symbols sequentially."""
    if symbols is None:
        conn = get_db()
        symbols = [r[0] for r in conn.execute("SELECT symbol FROM companies").fetchall()]
        conn.close()

    print(f"Bulk floorsheet fetch: {len(symbols)} symbols, {days_back} days back")
    grand_total = 0
    for i, sym in enumerate(symbols):
        print(f"\n[{i+1}/{len(symbols)}] {sym}")
        grand_total += fetch_historical_floorsheet(sym, days_back=days_back)
        time.sleep(random.uniform(1.0, 2.0))

    print(f"\n✅ All done. Total rows saved: {grand_total}")
    return grand_total


# ── MAIN TEST ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Merolagani Historical Floorsheet Fetcher — Pagination Fix")
    print("=" * 55)

    # First clear existing NABIL data so we get a clean test
    conn = get_db()
    deleted = conn.execute("DELETE FROM floor_sheet WHERE symbol='NABIL'").rowcount
    conn.commit()
    conn.close()
    print(f"Cleared {deleted} existing NABIL rows for clean test\n")

    result = fetch_historical_floorsheet("NABIL", days_back=10)

    conn = get_db()
    rows = conn.execute("""
        SELECT date, COUNT(*) as trades
        FROM floor_sheet WHERE symbol='NABIL'
        GROUP BY date ORDER BY date DESC
    """).fetchall()
    conn.close()

    print("\n📊 NABIL floorsheet results:")
    for r in rows:
        print(f"   {r[0]}: {r[1]} trades")

    # Sanity check — no round hundreds
    counts = [r[1] for r in rows]
    round_hundreds = [c for c in counts if c % 100 == 0 and c > 0]
    if round_hundreds:
        print(f"\n⚠️  Still seeing round hundreds: {round_hundreds}")
        print("   Pagination may still be broken — check debug output above")
    else:
        print(f"\n✅ No suspicious round hundreds — pagination looks correct!")