import argparse
import concurrent.futures
import json
import os
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from src.data.fetcher import create_tables, fetch_market_summary, fetch_price_history_incremental
from src.data.cleaner import clean_all_symbols
from src.signals.generator import create_signals_table as create_signal_table, generate_all_signals
from src.signals.nepse_signals import (
    create_signals_table as create_nepse_signal_table,
    calculate_all_signals,
)

try:
    from src.data.active_symbols import filter_active_symbols
except Exception:
    def filter_active_symbols(symbols, file_path=None):
        return list(symbols)


NEPAL_TZ = ZoneInfo("Asia/Kathmandu")
DB_PATH = ROOT / "data" / "nepse.db"
STATE_PATH = ROOT / "data" / "daily_update_state.json"
HOLIDAY_FILE = ROOT / "data" / "trading_holidays.txt"
LOG_DIR = ROOT / "data" / "daily_update_logs"

SHAREHUB_URL = "https://sharehubnepal.com/live/api/v2/floorsheet"
SHAREHUB_PAGE_SIZE = 100
PRICE_WORKERS = 4
PRICE_DELAY_SECONDS = 0.15


def now_np() -> datetime:
    return datetime.now(NEPAL_TZ)


def today_str() -> str:
    return now_np().strftime("%Y-%m-%d")


def log(message: str) -> None:
    stamp = now_np().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{stamp}] {message}", flush=True)


def load_holidays(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    }


def is_trading_day(day_str: str) -> bool:
    day = datetime.strptime(day_str, "%Y-%m-%d").date()
    if day.weekday() >= 5:
        return False
    return day_str not in load_holidays(HOLIDAY_FILE)


def already_completed(day_str: str) -> bool:
    if not STATE_PATH.exists():
        return False
    try:
        state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return False
    return state.get("date") == day_str and state.get("status") == "completed"


def write_state(payload: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_broker_tables() -> None:
    with get_db() as conn:
        conn.execute(
            """
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
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS broker_summary (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                date                  TEXT NOT NULL,
                symbol                TEXT NOT NULL,
                total_trades          INTEGER,
                total_volume          REAL,
                top3_buyer_volume     REAL,
                top3_seller_volume    REAL,
                buyer_concentration   REAL,
                seller_concentration  REAL,
                net_broker_flow       REAL,
                fetched_at            TEXT,
                UNIQUE(date, symbol)
            )
            """
        )
        conn.commit()


def get_symbols() -> list[str]:
    with get_db() as conn:
        company_symbols = {row[0] for row in conn.execute("SELECT DISTINCT symbol FROM companies") if row[0]}
        price_symbols = {row[0] for row in conn.execute("SELECT DISTINCT symbol FROM price_history") if row[0]}
    symbols = sorted({str(s).strip().upper() for s in company_symbols | price_symbols if str(s).strip()})
    return filter_active_symbols(symbols)


def fetch_sharehub_page(session: requests.Session, business_date: str, page: int, retries: int = 6) -> dict:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            response = session.get(
                SHAREHUB_URL,
                params={"date": business_date, "page": page, "size": SHAREHUB_PAGE_SIZE},
                timeout=45,
            )
            response.raise_for_status()
            payload = response.json()
            if not payload.get("success"):
                raise RuntimeError(f"ShareHub unsuccessful response: {payload}")
            return payload["data"]
        except Exception as exc:
            last_error = exc
            if attempt == retries:
                break
            sleep_s = min(60, (2 ** attempt) + 1.0)
            log(f"ShareHub retry {attempt}/{retries-1} for {business_date} page {page} after {sleep_s:.1f}s: {exc}")
            time.sleep(sleep_s)
    raise RuntimeError(f"ShareHub failed for {business_date} page {page}: {last_error}")


def fetch_today_floorsheet_rows(business_date: str) -> tuple[list[tuple], list[str]]:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://sharehubnepal.com/",
        }
    )

    first = fetch_sharehub_page(session, business_date, 1)
    total_pages = int(first.get("totalPages", 1) or 1)
    rows = list(first.get("content", []))

    for page in range(2, total_pages + 1):
        time.sleep(0.15)
        payload = fetch_sharehub_page(session, business_date, page)
        rows.extend(payload.get("content", []))

    fetched_at = now_np().isoformat()
    db_rows = []
    symbols = set()
    for row in rows:
        row_date = str(row.get("businessDate", "")).split("T", 1)[0]
        if row_date != business_date:
            continue
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        symbols.add(symbol)
        db_rows.append(
            (
                business_date,
                symbol,
                int(row.get("buyerMemberId") or 0),
                int(row.get("sellerMemberId") or 0),
                float(row.get("contractQuantity") or 0),
                float(row.get("contractRate") or 0),
                float(row.get("contractAmount") or 0),
                fetched_at,
            )
        )
    return db_rows, sorted(symbols)


def replace_floorsheet_rows(business_date: str, rows: list[tuple]) -> None:
    with get_db() as conn:
        conn.execute("DELETE FROM floor_sheet WHERE date = ?", (business_date,))
        conn.execute("DELETE FROM broker_summary WHERE date = ?", (business_date,))
        if rows:
            conn.executemany(
                """
                INSERT INTO floor_sheet
                (date, symbol, buyer_broker, seller_broker, quantity, rate, amount, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        conn.commit()


def calculate_broker_summary_for_date(symbol: str, business_date: str) -> dict | None:
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT buyer_broker, seller_broker, quantity, amount
            FROM floor_sheet
            WHERE symbol = ? AND date = ?
            """,
            (symbol, business_date),
        ).fetchall()

        if not rows:
            return None

        total_trades = len(rows)
        total_volume = sum(float(r["quantity"] or 0) for r in rows)
        buyer_totals = {}
        seller_totals = {}

        for row in rows:
            buyer = int(row["buyer_broker"] or 0)
            seller = int(row["seller_broker"] or 0)
            qty = float(row["quantity"] or 0)
            buyer_totals[buyer] = buyer_totals.get(buyer, 0.0) + qty
            seller_totals[seller] = seller_totals.get(seller, 0.0) + qty

        top3_buyer_volume = sum(sorted(buyer_totals.values(), reverse=True)[:3])
        top3_seller_volume = sum(sorted(seller_totals.values(), reverse=True)[:3])
        buyer_concentration = (top3_buyer_volume / total_volume * 100.0) if total_volume else 0.0
        seller_concentration = (top3_seller_volume / total_volume * 100.0) if total_volume else 0.0
        net_broker_flow = top3_buyer_volume - top3_seller_volume

        conn.execute(
            """
            INSERT OR REPLACE INTO broker_summary
            (date, symbol, total_trades, total_volume,
             top3_buyer_volume, top3_seller_volume,
             buyer_concentration, seller_concentration,
             net_broker_flow, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                business_date,
                symbol,
                total_trades,
                total_volume,
                top3_buyer_volume,
                top3_seller_volume,
                round(buyer_concentration, 2),
                round(seller_concentration, 2),
                net_broker_flow,
                now_np().isoformat(),
            ),
        )
        conn.commit()

    return {
        "symbol": symbol,
        "total_trades": total_trades,
        "total_volume": total_volume,
    }


def safe_fetch_sector_indices() -> None:
    try:
        from src.data.broker_fetcher import fetch_sector_indices
    except Exception as exc:
        log(f"Skipping sector index refresh: {exc}")
        return
    fetch_sector_indices()


def refresh_broker_summary(symbols: list[str], business_date: str) -> None:
    if not symbols:
        log("No floorsheet symbols found for broker summary refresh.")
        return
    log(f"Refreshing broker summaries for {len(symbols)} symbols on {business_date}...")
    ok = 0
    for index, symbol in enumerate(symbols, start=1):
        result = calculate_broker_summary_for_date(symbol, business_date)
        if result is not None:
            ok += 1
        if index % 50 == 0 or index == len(symbols):
            log(f"Broker summary progress: {index}/{len(symbols)} | success={ok}")


def refresh_incremental_prices(symbols: list[str], max_workers: int) -> None:
    log(f"Refreshing incremental price history for {len(symbols)} symbols with {max_workers} workers...")

    def process_one(symbol: str) -> tuple[str, bool]:
        try:
            result = fetch_price_history_incremental(symbol)
            time.sleep(PRICE_DELAY_SECONDS)
            return symbol, result is not None
        except Exception:
            return symbol, False

    success = 0
    failed = 0
    completed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_one, symbol): symbol for symbol in symbols}
        for future in concurrent.futures.as_completed(futures):
            symbol, ok = future.result()
            completed += 1
            if ok:
                success += 1
            else:
                failed += 1
            if completed % 25 == 0 or completed == len(symbols):
                log(f"Price progress: {completed}/{len(symbols)} | success={success} | failed={failed}")


def ensure_tables() -> None:
    create_tables()
    ensure_broker_tables()
    create_signal_table()
    create_nepse_signal_table()


def run_daily_update(force: bool = False, max_workers: int = PRICE_WORKERS) -> None:
    day_str = today_str()
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    if not force and not is_trading_day(day_str):
        log(f"{day_str} is not a trading day. Skipping.")
        return

    if not force and already_completed(day_str):
        log(f"{day_str} already completed. Skipping duplicate run.")
        return

    write_state({"date": day_str, "status": "running", "started_at": now_np().isoformat()})
    ensure_tables()

    symbols = get_symbols()
    log(f"Loaded {len(symbols)} symbols for daily update.")

    refresh_incremental_prices(symbols, max_workers=max_workers)

    log("Fetching market summary...")
    fetch_market_summary()

    log("Fetching sector indices...")
    safe_fetch_sector_indices()

    log(f"Fetching ShareHub floorsheet for {day_str}...")
    rows, floor_symbols = fetch_today_floorsheet_rows(day_str)
    replace_floorsheet_rows(day_str, rows)
    log(f"Saved {len(rows)} floorsheet rows across {len(floor_symbols)} symbols for {day_str}.")

    refresh_broker_summary(floor_symbols, day_str)

    log("Cleaning symbols...")
    clean_all_symbols(max_workers=10)

    log("Generating technical signals...")
    generate_all_signals()

    log("Generating NEPSE-specific signals...")
    calculate_all_signals(max_workers=10)

    write_state({"date": day_str, "status": "completed", "completed_at": now_np().isoformat()})
    log("Daily market update completed.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily 4 PM NEPSE market updater.")
    parser.add_argument("--force", action="store_true", help="Run even on non-trading days or if already completed today.")
    parser.add_argument("--max-workers", type=int, default=PRICE_WORKERS, help="Incremental price-history worker count.")
    args = parser.parse_args()
    run_daily_update(force=args.force, max_workers=args.max_workers)


if __name__ == "__main__":
    main()
