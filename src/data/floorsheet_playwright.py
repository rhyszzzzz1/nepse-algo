# src/data/floorsheet_playwright.py
#
# Scrapes merolagani.com/Floorsheet.aspx using Playwright (full browser)
# This bypasses all JavaScript/cookie/VIEWSTATE issues that block raw HTTP POSTs
#
# Usage:
#   py -3.11 src/data/floorsheet_playwright.py --date 2022-01-03
#   py -3.11 src/data/floorsheet_playwright.py --days 30
#   py -3.11 src/data/floorsheet_playwright.py --all     (Oct 2021 → today)

import os, sys, re, sqlite3, time, random
from datetime import datetime, timedelta

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

def _float(s):
    try: return float(str(s).replace(",", "").strip() or 0)
    except: return 0.0

def _int(s):
    try: return int(float(str(s).replace(",", "").strip() or 0))
    except: return 0

def _is_holiday(dt):
    return dt.weekday() in (4, 5)  # Fri=4, Sat=5

def _dates_in_db():
    conn = get_db()
    dates = {r[0] for r in conn.execute("SELECT DISTINCT date FROM floor_sheet").fetchall()}
    conn.close()
    return dates

def _save_rows(rows):
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


# ── PARSE TABLE FROM HTML ─────────────────────────────────────────────────────
def parse_table_html(html):
    """
    Parse floor sheet table from HTML string.
    Table columns: # | Transact.No. | Symbol | Buyer | Seller | Quantity | Rate | Amount
    Returns (list of row dicts, total_pages).
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    # Find the table that has buyer/seller/symbol headers
    table = None
    for t in soup.find_all("table"):
        txt = t.get_text().lower()
        if "buyer" in txt and "seller" in txt and "symbol" in txt:
            table = t
            break

    if not table:
        return [], 1

    # Find which row is the header (contains <th> tags)
    header_idx = 0
    for i, tr in enumerate(table.find_all("tr")):
        if tr.find("th"):
            header_idx = i
            break

    # Map column names to indices from the header row
    all_trs = table.find_all("tr")
    col_map = {}  # name -> index
    if all_trs:
        header_cells = all_trs[header_idx].find_all(["th", "td"])
        for i, cell in enumerate(header_cells):
            txt = cell.get_text(strip=True).lower()
            if "symbol" in txt:   col_map["symbol"] = i
            elif "buyer" in txt:  col_map["buyer"]  = i
            elif "seller" in txt: col_map["seller"] = i
            elif "qty" in txt or "quantity" in txt: col_map["qty"] = i
            elif "rate" in txt:   col_map["rate"]   = i
            elif "amount" in txt: col_map["amount"] = i

    # Fallback column positions if header detection fails
    # Standard layout: # | Transact.No. | Symbol | Buyer | Seller | Qty | Rate | Amount
    if not col_map:
        col_map = {"symbol": 2, "buyer": 3, "seller": 4, "qty": 5, "rate": 6, "amount": 7}

    rows = []
    for tr in all_trs[header_idx + 1:]:  # data rows only
        tds = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(tds) < 6:
            continue
        try:
            sym_i = col_map.get("symbol", 2)
            sym   = tds[sym_i].strip().upper() if sym_i < len(tds) else ""
            if not sym or sym in ("SYMBOL", "#", ""):
                continue
            b_i   = col_map.get("buyer",  3)
            s_i   = col_map.get("seller", 4)
            q_i   = col_map.get("qty",    5)
            r_i   = col_map.get("rate",   6)
            a_i   = col_map.get("amount", 7)
            qty   = _float(tds[q_i]) if q_i < len(tds) else 0.0
            rate  = _float(tds[r_i]) if r_i < len(tds) else 0.0
            rows.append({
                "symbol":        sym,
                "buyer_broker":  _int(tds[b_i])  if b_i  < len(tds) else 0,
                "seller_broker": _int(tds[s_i])  if s_i  < len(tds) else 0,
                "quantity":      qty,
                "rate":          rate,
                "amount":        _float(tds[a_i]) if a_i < len(tds) else round(qty * rate, 2),
            })
        except Exception:
            continue

    # Total pages from pagination text
    total_pages = 1
    m = re.search(r"Total pages:\s*(\d+)", soup.get_text(), re.IGNORECASE)
    if m:
        total_pages = int(m.group(1))

    return rows, total_pages


# ── PLAYWRIGHT SCRAPER ────────────────────────────────────────────────────────
def scrape_date_playwright(page, date_str_ymd):
    """
    Scrape one date using an already-open Playwright page.
    Navigates fresh to Floorsheet.aspx for each date to ensure clean state.
    Returns number of rows saved.
    """
    from playwright.sync_api import TimeoutError as PWTimeout

    dt         = datetime.strptime(date_str_ymd, "%Y-%m-%d")
    ml_date    = dt.strftime("%m/%d/%Y")
    fetched_at = datetime.now().isoformat()

    try:
        # -- Clear and fill the date field ---------------------------------
        date_input = page.locator("#ctl00_ContentPlaceHolder1_txtFloorsheetDateFilter")
        date_input.wait_for(state="visible", timeout=10000)
        # Select-all + delete + slow type + Tab (proven working approach)
        date_input.click()
        page.keyboard.press("Control+a")
        page.keyboard.press("Delete")
        page.wait_for_timeout(200)
        date_input.type(ml_date, delay=80)   # slow type triggers JS events
        page.wait_for_timeout(300)
        page.keyboard.press("Tab")           # close datepicker / confirm value
        page.wait_for_timeout(200)

        # ── Click Search and wait for postback ───────────────────────────────
        search_btn = page.locator("#ctl00_ContentPlaceHolder1_lbtnSearchFloorsheet")
        search_btn.click()

        # ASP.NET full postback — wait for DOM to settle
        page.wait_for_load_state("load",        timeout=25000)
        page.wait_for_load_state("networkidle", timeout=15000)

        body_text = page.inner_text("body")

        # ── Check results ─────────────────────────────────────────────────────
        if "Could not find floorsheet" in body_text:
            print(f"  [{date_str_ymd}] No trading data")
            return 0

        if "Something went wrong" in body_text:
            print(f"  [{date_str_ymd}] Server error")
            return 0

        # ── Parse page 1 ─────────────────────────────────────────────────────
        html = page.inner_html("body")
        all_rows, total_pages = parse_table_html(html)

        if not all_rows:
            # Extra debug: show what date the page is showing
            showing = re.search(r"As of (\S+)", body_text)
            date_shown = showing.group(1) if showing else "unknown"
            print(f"  [{date_str_ymd}] No rows (page shows: {date_shown})")
            return 0

        print(f"  [{date_str_ymd}] {total_pages}p | p1: {len(all_rows)} rows", end="", flush=True)

        # ── Paginate (Next button is more reliable than page numbers) ─────────
        for page_num in range(2, total_pages + 1):
            try:
                table_loc = page.locator("table").filter(has_text=re.compile(r"Buyer", re.IGNORECASE)).first
                old_text = table_loc.inner_text() if table_loc.is_visible() else ""

                # Target the exact page number link, fallback to "Next"
                next_btn = page.get_by_role("link", name=str(page_num), exact=True).first
                if next_btn.count() == 0 or not next_btn.is_visible():
                    next_btn = page.locator("a").filter(has_text=re.compile(r"Next", re.IGNORECASE)).first
                
                next_btn.scroll_into_view_if_needed()
                next_btn.click()
                
                # Fast polling: wait for the table text to change
                changed = False
                for _ in range(50):
                    page.wait_for_timeout(400)
                    new_text = table_loc.inner_text() if table_loc.is_visible() else ""
                    if new_text != old_text and new_text != "":
                        changed = True
                        break
                
                if not changed:
                    print(f" [timeout html unchanged p{page_num}]", end="")
                    break
                    
                html_p = page.inner_html("body")
                rows_p, _ = parse_table_html(html_p)
                if not rows_p:
                    break
                all_rows.extend(rows_p)
                print(f" +{len(rows_p)}", end="", flush=True)
            except Exception as e:
                print(f" [p{page_num} err]", end="")
                break

        print()  # newline

        # ── Save ──────────────────────────────────────────────────────────────
        db_rows = [
            (date_str_ymd, r["symbol"], r["buyer_broker"], r["seller_broker"],
             r["quantity"], r["rate"], r["amount"], fetched_at)
            for r in all_rows if r.get("symbol")
        ]
        saved = _save_rows(db_rows)
        return saved

    except PWTimeout:
        print(f"  [{date_str_ymd}] Timeout")
        return 0
    except Exception as e:
        print(f"  [{date_str_ymd}] ERROR: {e}")
        import traceback; traceback.print_exc()
        return 0


# ── COMPUTE BROKER SUMMARY ────────────────────────────────────────────────────
def compute_broker_summary(dates=None):
    """Aggregate floor_sheet into broker_summary for new dates."""
    conn = get_db()
    if dates is None:
        all_dates  = {r[0] for r in conn.execute("SELECT DISTINCT date FROM floor_sheet").fetchall()}
        done_dates = {r[0] for r in conn.execute("SELECT DISTINCT date FROM broker_summary").fetchall()}
        dates = sorted(all_dates - done_dates)
    conn.close()

    if not dates:
        print("[Broker] Nothing new to process.")
        return 0

    print(f"[Broker] Computing summary for {len(dates)} dates...")
    fetched_at = datetime.now().isoformat()
    total = 0

    for d in dates:
        conn = get_db()
        try:
            conn.execute("DELETE FROM broker_summary WHERE date=?", (d,))
            conn.execute("""
                INSERT INTO broker_summary
                    (date, symbol, broker, buy_qty, buy_amount, trades_as_buyer,
                     sell_qty, sell_amount, trades_as_seller, net_qty, net_amount, fetched_at)
                SELECT date, symbol, buyer_broker,
                       SUM(quantity), SUM(amount), COUNT(*),
                       0.0, 0.0, 0,
                       SUM(quantity), SUM(amount), ?
                FROM floor_sheet WHERE date=?
                GROUP BY date, symbol, buyer_broker
                ON CONFLICT(date, symbol, broker) DO UPDATE SET
                    buy_qty=excluded.buy_qty, buy_amount=excluded.buy_amount,
                    trades_as_buyer=excluded.trades_as_buyer,
                    net_qty=excluded.buy_qty-broker_summary.sell_qty,
                    net_amount=excluded.buy_amount-broker_summary.sell_amount
            """, (fetched_at, d))
            conn.execute("""
                INSERT INTO broker_summary
                    (date, symbol, broker, sell_qty, sell_amount, trades_as_seller,
                     buy_qty, buy_amount, trades_as_buyer, net_qty, net_amount, fetched_at)
                SELECT date, symbol, seller_broker,
                       SUM(quantity), SUM(amount), COUNT(*),
                       0.0, 0.0, 0,
                       -SUM(quantity), -SUM(amount), ?
                FROM floor_sheet WHERE date=?
                GROUP BY date, symbol, seller_broker
                ON CONFLICT(date, symbol, broker) DO UPDATE SET
                    sell_qty=excluded.sell_qty, sell_amount=excluded.sell_amount,
                    trades_as_seller=excluded.trades_as_seller,
                    net_qty=broker_summary.buy_qty-excluded.sell_qty,
                    net_amount=broker_summary.buy_amount-excluded.sell_amount
            """, (fetched_at, d))
            conn.commit()
            n = conn.execute("SELECT COUNT(*) FROM broker_summary WHERE date=?", (d,)).fetchone()[0]
            total += n
        except Exception as e:
            print(f"  [{d}] broker_summary error: {e}")
        finally:
            conn.close()

    print(f"[Broker] Done. {total:,} total rows.")
    return total


# ── MAIN BACKFILL RUNNER ──────────────────────────────────────────────────────
def run_backfill(start_date=None, days_back=None, single_date=None):
    from playwright.sync_api import sync_playwright

    existing = _dates_in_db()
    today    = datetime.now().date()

    if single_date:
        dates = [single_date]
    else:
        if start_date:
            start = datetime.strptime(start_date, "%Y-%m-%d").date()
        elif days_back:
            start = today - timedelta(days=days_back)
        else:
            start = datetime.strptime("2023-08-01", "%Y-%m-%d").date()  # earliest available on Merolagani

        dates = []
        d = start
        while d <= today:
            s = d.strftime("%Y-%m-%d")
            if not _is_holiday(datetime.combine(d, datetime.min.time())) and s not in existing:
                dates.append(s)
            d += timedelta(days=1)

    if not dates:
        print("Nothing to fetch — all dates already in DB.")
        return

    print(f"Fetching {len(dates)} dates via Playwright browser...")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            locale="en-US",
        )
        page = context.new_page()

        # Navigate ONCE -- reuse the same page for all dates
        # (Re-navigating per date breaks the datepicker JS interaction)
        page.goto("https://merolagani.com/Floorsheet.aspx",
                  wait_until="networkidle", timeout=30000)

        total_saved = 0
        for i, date_str in enumerate(dates):
            saved = scrape_date_playwright(page, date_str)
            total_saved += saved

            # Compute broker_summary in batches of 20
            if (i + 1) % 20 == 0:
                compute_broker_summary()

            # Small pause every 10 requests
            if (i + 1) % 10 == 0:
                pause = random.uniform(3, 7)
                print(f"  [pause {pause:.0f}s] {i+1}/{len(dates)} done, {total_saved:,} rows saved so far")
                time.sleep(pause)
            else:
                time.sleep(random.uniform(0.5, 1.5))

        browser.close()

    compute_broker_summary()

    conn = get_db()
    fs  = conn.execute("SELECT COUNT(*) FROM floor_sheet").fetchone()[0]
    fsd = conn.execute("SELECT COUNT(DISTINCT date) FROM floor_sheet").fetchone()[0]
    bs  = conn.execute("SELECT COUNT(*) FROM broker_summary").fetchone()[0]
    conn.close()
    print(f"\nDone! floor_sheet: {fs:,} rows / {fsd} dates | broker_summary: {bs:,} rows")


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Merolagani floorsheet via Playwright")
    parser.add_argument("--date",   help="Single date YYYY-MM-DD")
    parser.add_argument("--days",   type=int, help="Last N days")
    parser.add_argument("--from",   dest="start", help="Start from YYYY-MM-DD")
    parser.add_argument("--all",    action="store_true", help="Full backfill Oct 2021 → today")
    parser.add_argument("--summary",action="store_true", help="Just recompute broker_summary")
    args = parser.parse_args()

    if args.summary:
        compute_broker_summary()
    elif args.date:
        run_backfill(single_date=args.date)
    elif args.days:
        run_backfill(days_back=args.days)
    elif args.start:
        run_backfill(start_date=args.start)
    elif args.all:
        run_backfill()
    else:
        # Default: last 7 days
        run_backfill(days_back=7)
