import argparse
import hashlib
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from src.data.fetcher import sync_daily_price_into_price_history
from src.data.floorsheet_pipeline import compute_broker_summary
from src.data.derived_analytics import build_latest_snapshots

DB_PATH = ROOT / "data" / "nepse.db"
CACHE_PATH = ROOT / "data" / "cache" / "sharehub_realtime_state.json"
RAW_CACHE_DIR = ROOT / "data" / "cache" / "sharehub_realtime"
TIMEZONE = ZoneInfo("Asia/Kathmandu")

SHAREHUB_TODAYS_PRICE_URL = "https://sharehubnepal.com/live/api/v2/nepselive/todays-price"
SHAREHUB_INDEX_ANALYSIS_URL = "https://sharehubnepal.com/data/api/v1/index/date-wise-analysis"
SHAREHUB_FLOORSHEET_URL = "https://sharehubnepal.com/live/api/v2/floorsheet"

TODAYS_PRICE_PARAMS = {
    "queryKey[0]": "todayPrice",
    "queryKey[1][order]": "",
    "queryKey[1][sortBy]": "",
    "queryKey[1][searchText]": "",
    "queryKey[1][page]": 1,
    "queryKey[1][limit]": 500,
    "queryKey[1][sortDirection]": "",
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Continuously sync ShareHub realtime market + floorsheet data into the local DB."
    )
    parser.add_argument("--db-path", default=str(DB_PATH))
    parser.add_argument("--cache-file", default=str(CACHE_PATH))
    parser.add_argument("--raw-cache-dir", default=str(RAW_CACHE_DIR))
    parser.add_argument("--market-interval", type=int, default=5, help="Polling interval in seconds for market/daily-price updates.")
    parser.add_argument("--floorsheet-interval", type=int, default=60, help="Polling interval in seconds for realtime floorsheet updates.")
    parser.add_argument("--floorsheet-page-size", type=int, default=100)
    parser.add_argument("--start-time", default="11:00")
    parser.add_argument("--end-time", default="15:00")
    parser.add_argument("--once", action="store_true", help="Run a single sync pass and exit.")
    parser.add_argument("--wait-for-window", action="store_true", help="Sleep until the next trading window if currently outside it.")
    return parser.parse_args()


def now_local():
    return datetime.now(TIMEZONE)


def parse_clock(value: str) -> dt_time:
    hour, minute = value.split(":")
    return dt_time(int(hour), int(minute))


def is_trading_window(current: datetime, start_clock: dt_time, end_clock: dt_time) -> bool:
    if current.weekday() > 4:  # Monday-Friday only, as requested
        return False
    current_time = current.time()
    return start_clock <= current_time <= end_clock


def next_window_start(current: datetime, start_clock: dt_time) -> datetime:
    probe = current
    while True:
        if probe.weekday() <= 4:
            candidate = probe.replace(
                hour=start_clock.hour,
                minute=start_clock.minute,
                second=0,
                microsecond=0,
            )
            if candidate > current:
                return candidate
        probe = (probe + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)


def sleep_until_window(start_clock: dt_time):
    current = now_local()
    target = next_window_start(current, start_clock)
    seconds = max(1, int((target - current).total_seconds()))
    print(f"[Realtime] Outside trading window. Sleeping until {target.isoformat()} ({seconds}s)")
    time.sleep(seconds)


def ensure_dirs(cache_file: Path, raw_cache_dir: Path):
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    raw_cache_dir.mkdir(parents=True, exist_ok=True)


def load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(path: Path, state: dict):
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def sha1_payload(value) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def http_get_json(session: requests.Session, url: str, *, params=None, timeout=60, retries=3):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc
            if attempt == retries:
                break
            wait_s = min(8, 2 ** (attempt - 1))
            print(f"[Realtime] retry {attempt}/{retries} for {url} after {wait_s}s: {exc}")
            time.sleep(wait_s)
    raise RuntimeError(f"Failed GET {url}: {last_error}")


def get_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def normalize_trade_date(value: str) -> str:
    return str(value).split("T", 1)[0]


def fetch_index_analysis(session: requests.Session, trade_date: str):
    payload = http_get_json(
        session,
        SHAREHUB_INDEX_ANALYSIS_URL,
        params={"date": trade_date},
        timeout=60,
    )
    data = payload.get("data") or {}
    rows = data.get("content") if isinstance(data, dict) else data
    rows = rows or []
    normalized = []
    market_row = None
    symbol_to_sector = {
        "NEPSE": "NEPSE",
        "FLOAT": "Float Index",
        "SENSITIVE": "Sensitive Index",
        "SENFLOAT": "Sensitive Float Index",
        "BANKING": "Banking SubIndex",
        "HOTELS": "Hotels And Tourism Index",
        "OTHERS": "Others Index",
        "HYDROPOWER": "HydroPower Index",
        "DEVBANK": "Development Bank Index",
        "MANUFACTURE": "Manufacturing And Processing",
        "NONLIFEINSU": "Non Life Insurance",
    }
    fetched_at = now_local().isoformat()
    for row in rows:
        symbol = str(row.get("symbol") or "").upper().strip()
        if not symbol:
            continue
        sector_name = symbol_to_sector.get(symbol, str(row.get("name") or symbol).strip())
        close_value = float(row.get("close") or 0)
        normalized.append(
            {
                "date": trade_date,
                "sector": sector_name,
                "value": close_value,
                "fetched_at": fetched_at,
            }
        )
        if symbol == "NEPSE":
            market_row = {
                "date": trade_date,
                "nepse_index": close_value,
                "total_turnover": float(row.get("turnover") or row.get("amount") or 0),
                "total_volume": float(row.get("volume") or 0),
                "fetched_at": fetched_at,
            }
    return market_row, normalized


def fetch_today_price(session: requests.Session):
    payload = http_get_json(
        session,
        SHAREHUB_TODAYS_PRICE_URL,
        params=TODAYS_PRICE_PARAMS,
        timeout=60,
    )
    rows = payload.get("data") or []
    if not rows:
        return None, []
    trade_date = normalize_trade_date(rows[0].get("businessDate"))
    fetched_at = now_local().isoformat()
    normalized = []
    for row in rows:
        symbol = str(row.get("symbol") or "").upper().strip()
        if not symbol:
            continue
        normalized.append(
            {
                "date": trade_date,
                "symbol": symbol,
                "open": float(row.get("openPrice") or 0),
                "high": float(row.get("highPrice") or 0),
                "low": float(row.get("lowPrice") or 0),
                "close": float(row.get("ltp") or 0),
                "volume": float(row.get("totalTradedQuantity") or 0),
                "amount": float(row.get("totalTradedValue") or 0),
                "trades": int(row.get("totalTrades") or 0),
                "vwap": float(row.get("averageTradedPrice") or 0),
                "fetched_at": fetched_at,
            }
        )
    return trade_date, normalized


def fetch_floorsheet_page(session: requests.Session, trade_date: str, page: int, page_size: int):
    payload = http_get_json(
        session,
        SHAREHUB_FLOORSHEET_URL,
        params={"Size": page_size, "page": page},
        timeout=90,
    )
    data = payload.get("data") or {}
    return data


def floorsheet_signature(first_page: dict) -> str:
    content = first_page.get("content") or []
    signature_payload = {
        "totalTrades": first_page.get("totalTrades"),
        "totalQty": first_page.get("totalQty"),
        "totalAmount": first_page.get("totalAmount"),
        "totalPages": first_page.get("totalPages"),
        "firstContractIds": [row.get("contractId") for row in content[:25]],
    }
    return sha1_payload(signature_payload)


def get_seen_transaction_keys(conn: sqlite3.Connection, trade_date: str):
    rows = conn.execute(
        """
        SELECT symbol, transaction_no, buyer_broker, seller_broker, quantity, rate
        FROM floor_sheet
        WHERE date = ?
        """,
        (trade_date,),
    ).fetchall()
    seen = set()
    for row in rows:
        transaction_no = str(row["transaction_no"] or "")
        symbol = str(row["symbol"] or "").upper().strip()
        if transaction_no:
            seen.add((trade_date, symbol, transaction_no))
        else:
            seen.add(
                (
                    trade_date,
                    symbol,
                    row["buyer_broker"],
                    row["seller_broker"],
                    float(row["quantity"] or 0),
                    float(row["rate"] or 0),
                )
            )
    return seen


def normalize_live_trade(trade_date: str, row: dict, fetched_at: str, page_no: int, row_idx: int):
    symbol = str(row.get("symbol") or "").upper().strip()
    transaction_no = str(row.get("contractId") or "")
    buyer = row.get("buyerMemberId")
    seller = row.get("sellerMemberId")
    buyer = -1 if buyer in (None, "") else int(buyer)
    seller = -1 if seller in (None, "") else int(seller)
    quantity = float(row.get("contractQuantity") or 0)
    rate = float(row.get("contractRate") or 0)
    amount = float(row.get("contractAmount") or 0)
    key = (trade_date, symbol, transaction_no) if transaction_no else (
        trade_date,
        symbol,
        buyer,
        seller,
        quantity,
        rate,
    )
    normalized = {
        "date": trade_date,
        "symbol": symbol,
        "buyer_broker": buyer,
        "seller_broker": seller,
        "quantity": quantity,
        "rate": rate,
        "amount": amount,
        "fetched_at": fetched_at,
        "transaction_no": transaction_no,
        "source_file": "sharehub_realtime",
        "page_no": page_no,
        "row_no_in_page": row_idx,
    }
    return key, normalized


def fetch_incremental_floorsheet(session: requests.Session, conn: sqlite3.Connection, trade_date: str, page_size: int):
    first_page = fetch_floorsheet_page(session, trade_date, 1, page_size)
    total_pages = int(first_page.get("totalPages") or 1)
    fetched_at = now_local().isoformat()
    seen_keys = get_seen_transaction_keys(conn, trade_date)
    new_rows = []
    scanned_pages = 0

    page_no = 1
    while page_no <= total_pages:
        if page_no == 1:
            page_data = first_page
        else:
            page_data = fetch_floorsheet_page(session, trade_date, page_no, page_size)
        scanned_pages += 1
        content = page_data.get("content") or []
        page_new_rows = []
        for row_idx, row in enumerate(content, start=1):
            key, normalized = normalize_live_trade(trade_date, row, fetched_at, page_no, row_idx)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            page_new_rows.append(normalized)
        new_rows.extend(page_new_rows)
        if not page_new_rows:
            break
        page_no += 1

    raw_snapshot = {
        "fetched_at": fetched_at,
        "trade_date": trade_date,
        "summary": {
            "totalTrades": first_page.get("totalTrades"),
            "totalQty": first_page.get("totalQty"),
            "totalAmount": first_page.get("totalAmount"),
            "totalPages": total_pages,
            "pageSize": first_page.get("pageSize") or page_size,
            "scannedPages": scanned_pages,
            "newRows": len(new_rows),
        },
        "rows": new_rows,
    }
    return first_page, new_rows, raw_snapshot


def replace_market_summary(conn: sqlite3.Connection, market_row: dict):
    conn.execute("DELETE FROM market_summary WHERE date = ?", (market_row["date"],))
    conn.execute(
        """
        INSERT INTO market_summary (date, nepse_index, total_turnover, total_volume, fetched_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            market_row["date"],
            market_row["nepse_index"],
            market_row["total_turnover"],
            market_row["total_volume"],
            market_row["fetched_at"],
        ),
    )


def replace_sector_index(conn: sqlite3.Connection, trade_date: str, rows: list[dict]):
    conn.execute("DELETE FROM sector_index WHERE date = ?", (trade_date,))
    conn.executemany(
        """
        INSERT INTO sector_index (date, sector, value, fetched_at)
        VALUES (?, ?, ?, ?)
        """,
        [(row["date"], row["sector"], row["value"], row["fetched_at"]) for row in rows],
    )


def replace_daily_price(conn: sqlite3.Connection, trade_date: str, rows: list[dict]):
    conn.execute("DELETE FROM daily_price WHERE date = ?", (trade_date,))
    conn.executemany(
        """
        INSERT INTO daily_price (date, symbol, open, high, low, close, volume, amount, trades, vwap, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["date"],
                row["symbol"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["volume"],
                row["amount"],
                row["trades"],
                row["vwap"],
                row["fetched_at"],
            )
            for row in rows
        ],
    )


def append_floor_sheet(conn: sqlite3.Connection, rows: list[dict]):
    if not rows:
        return
    conn.executemany(
        """
        INSERT OR IGNORE INTO floor_sheet
        (date, symbol, buyer_broker, seller_broker, quantity, rate, amount, fetched_at, transaction_no, source_file, page_no, row_no_in_page)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["date"],
                row["symbol"],
                row["buyer_broker"],
                row["seller_broker"],
                row["quantity"],
                row["rate"],
                row["amount"],
                row["fetched_at"],
                row["transaction_no"],
                row["source_file"],
                row["page_no"],
                row["row_no_in_page"],
            )
            for row in rows
        ],
    )


def write_raw_snapshot(raw_cache_dir: Path, snapshot: dict):
    path = raw_cache_dir / f"floorsheet_{snapshot['trade_date']}.json"
    path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")


def seconds_since(last_checked_at: str | None):
    if not last_checked_at:
        return None
    try:
        then = datetime.fromisoformat(last_checked_at)
        return (now_local() - then).total_seconds()
    except Exception:
        return None


def run_once(args, state: dict):
    db_path = Path(args.db_path)
    cache_file = Path(args.cache_file)
    raw_cache_dir = Path(args.raw_cache_dir)
    ensure_dirs(cache_file, raw_cache_dir)

    session = requests.Session()

    trade_date, daily_rows = fetch_today_price(session)
    if not trade_date:
        print("[Realtime] No today-price rows returned; skipping pass.")
        return state

    market_row, sector_rows = fetch_index_analysis(session, trade_date)
    if not market_row or not sector_rows:
        print(f"[Realtime] No index analysis rows returned for {trade_date}; skipping pass.")
        return state

    state.setdefault("today", {})
    today_state = state["today"]
    market_due = args.once or seconds_since(today_state.get("last_market_checked_at")) is None or seconds_since(today_state.get("last_market_checked_at")) >= args.market_interval
    floorsheet_due = args.once or seconds_since(today_state.get("last_floorsheet_checked_at")) is None or seconds_since(today_state.get("last_floorsheet_checked_at")) >= args.floorsheet_interval
    changed_sections = []

    derived_result = None
    broker_rows = None
    conn = get_db(db_path)
    try:
        if market_due:
            daily_hash = sha1_payload(daily_rows)
            index_hash = sha1_payload({"market": market_row, "sectors": sector_rows})
            if today_state.get("index_hash") != index_hash:
                replace_market_summary(conn, market_row)
                replace_sector_index(conn, trade_date, sector_rows)
                today_state["index_hash"] = index_hash
                changed_sections.append("market")

            if today_state.get("daily_hash") != daily_hash:
                replace_daily_price(conn, trade_date, daily_rows)
                conn.commit()
                sync_daily_price_into_price_history(trade_date)
                today_state["daily_hash"] = daily_hash
                changed_sections.append("daily_price")
            today_state["last_market_checked_at"] = now_local().isoformat()

        first_page = None
        if floorsheet_due:
            first_page = fetch_floorsheet_page(session, trade_date, 1, args.floorsheet_page_size)
            floor_sig = floorsheet_signature(first_page)
            if today_state.get("floorsheet_signature") != floor_sig:
                _, floor_rows, raw_snapshot = fetch_incremental_floorsheet(
                    session,
                    conn,
                    trade_date,
                    args.floorsheet_page_size,
                )
                append_floor_sheet(conn, floor_rows)
                write_raw_snapshot(raw_cache_dir, raw_snapshot)
                today_state["floorsheet_signature"] = floor_sig
                today_state["floorsheet_total_trades"] = first_page.get("totalTrades")
                today_state["floorsheet_total_pages"] = first_page.get("totalPages")
                today_state["floorsheet_new_rows"] = len(floor_rows)
                if floor_rows:
                    changed_sections.append("floorsheet")
            today_state["last_floorsheet_checked_at"] = now_local().isoformat()

        conn.commit()
    finally:
        conn.close()

    if "floorsheet" in changed_sections:
        try:
            broker_rows = compute_broker_summary([trade_date])
        except Exception as exc:
            today_state["broker_error"] = str(exc)

    if "market" in changed_sections or "daily_price" in changed_sections or "floorsheet" in changed_sections:
        try:
            derived_result = build_latest_snapshots(as_of_date=trade_date)
        except Exception as exc:
            today_state["derived_error"] = str(exc)

    today_state["trade_date"] = trade_date
    today_state["last_checked_at"] = now_local().isoformat()
    today_state["last_changed_sections"] = changed_sections
    if broker_rows is not None:
        today_state["broker_rows"] = broker_rows
    if derived_result is not None:
        today_state["derived_result"] = derived_result
    print(
        f"[Realtime] {trade_date} | changed={changed_sections or ['none']} | "
        f"daily_rows={len(daily_rows)} | sectors={len(sector_rows)} | "
        f"floorsheet_trades={(first_page or {}).get('totalTrades', today_state.get('floorsheet_total_trades'))} | "
        f"broker_rows={broker_rows if broker_rows is not None else today_state.get('broker_rows')}"
    )
    save_state(cache_file, state)
    return state


def main():
    args = parse_args()
    start_clock = parse_clock(args.start_time)
    end_clock = parse_clock(args.end_time)
    cache_file = Path(args.cache_file)
    state = load_state(cache_file)

    while True:
        current = now_local()
        if not is_trading_window(current, start_clock, end_clock):
            if args.once and not args.wait_for_window:
                print("[Realtime] Outside trading window. Exiting because --once was set.")
                return
            if args.wait_for_window:
                sleep_until_window(start_clock)
                state = load_state(cache_file)
                continue
            print("[Realtime] Outside trading window. Sleeping for 60s.")
            time.sleep(60)
            continue

        state = run_once(args, state)
        if args.once:
            return
        time.sleep(max(1, min(args.market_interval, args.floorsheet_interval)))


if __name__ == "__main__":
    main()
