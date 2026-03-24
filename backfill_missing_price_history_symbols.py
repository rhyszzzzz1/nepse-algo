import argparse
import csv
import sqlite3
import time
from pathlib import Path

import pyarrow.parquet as pq

from src.data.fetcher import create_tables, fetch_price_history


ROOT = Path(r"C:\nepse-algo")
DB_PATH = ROOT / "data" / "nepse.db"
FLOORSHEET_DIR = ROOT / "data" / "chukul_floorsheet"
DEFAULT_OUT_CSV = ROOT / "data" / "missing_price_history_symbols.csv"


def get_price_history_symbols(conn: sqlite3.Connection) -> set[str]:
    return {str(row[0]).strip().upper() for row in conn.execute("SELECT DISTINCT symbol FROM price_history") if row[0]}


def scan_floorsheet_symbols(root: Path) -> dict[str, dict[str, str | int]]:
    stats: dict[str, dict[str, str | int]] = {}
    for parquet_path in sorted(root.glob("date=*/data.parquet")):
        date_part = parquet_path.parent.name
        if not date_part.startswith("date="):
            continue
        trade_date = date_part.split("=", 1)[1]
        table = pq.ParquetFile(parquet_path).read(columns=["symbol"])
        if "symbol" not in table.column_names:
            continue
        symbols_in_file = {
            str(raw_symbol).strip().upper()
            for raw_symbol in table.column("symbol").to_pylist()
            if raw_symbol is not None and str(raw_symbol).strip()
        }
        for symbol in symbols_in_file:
            entry = stats.setdefault(
                symbol,
                {
                    "symbol": symbol,
                    "floorsheet_dates": 0,
                    "first_floorsheet_date": trade_date,
                    "last_floorsheet_date": trade_date,
                },
            )
            entry["floorsheet_dates"] = int(entry["floorsheet_dates"]) + 1
            if trade_date < str(entry["first_floorsheet_date"]):
                entry["first_floorsheet_date"] = trade_date
            if trade_date > str(entry["last_floorsheet_date"]):
                entry["last_floorsheet_date"] = trade_date
    return stats


def write_missing_csv(path: Path, rows: list[dict[str, str | int]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "symbol",
                "floorsheet_dates",
                "first_floorsheet_date",
                "last_floorsheet_date",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def db_row_count_for_symbol(conn: sqlite3.Connection, symbol: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM price_history WHERE symbol = ?",
        (symbol,),
    ).fetchone()
    return int(row[0] or 0)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Find floorsheet symbols missing from price_history and backfill OHLCV for them."
    )
    parser.add_argument("--db-path", default=str(DB_PATH))
    parser.add_argument("--floorsheet-dir", default=str(FLOORSHEET_DIR))
    parser.add_argument("--out-csv", default=str(DEFAULT_OUT_CSV))
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--delay", type=float, default=0.75)
    parser.add_argument("--skip-fetch", action="store_true")
    args = parser.parse_args()

    db_path = Path(args.db_path)
    floorsheet_dir = Path(args.floorsheet_dir)
    out_csv = Path(args.out_csv)

    create_tables()

    with sqlite3.connect(db_path) as conn:
        price_symbols = get_price_history_symbols(conn)

    print("Scanning floorsheet parquet symbol universe...")
    floorsheet_stats = scan_floorsheet_symbols(floorsheet_dir)
    floor_symbols = set(floorsheet_stats.keys())
    missing = sorted(floor_symbols - price_symbols)

    rows = [floorsheet_stats[symbol] for symbol in missing]
    write_missing_csv(out_csv, rows)

    print(f"Floorsheet symbols: {len(floor_symbols)}")
    print(f"price_history symbols: {len(price_symbols)}")
    print(f"Missing symbols: {len(missing)}")
    print(f"Saved missing-symbol list to {out_csv}")

    if args.limit is not None:
        missing = missing[: args.limit]
        print(f"Applying limit -> {len(missing)} symbols")

    if args.skip_fetch:
        return

    with sqlite3.connect(db_path) as conn:
        for index, symbol in enumerate(missing, start=1):
            before = db_row_count_for_symbol(conn, symbol)
            print(f"[{index}/{len(missing)}] Fetch {symbol} (before_rows={before})")
            fetch_price_history(symbol)
            after = db_row_count_for_symbol(conn, symbol)
            print(f"  after_rows={after}")
            time.sleep(args.delay)


if __name__ == "__main__":
    main()
