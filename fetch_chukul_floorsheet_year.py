import argparse
import csv
import random
import sqlite3
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests


ROOT = Path(__file__).resolve().parent
DEFAULT_DB_PATH = ROOT / "data" / "nepse.db"
DEFAULT_OUT_DIR = ROOT / "data" / "chukul_floorsheet"
BASE_URL = "https://chukul.com/api/data/v2/floorsheet/bydate/"
PAGE_SIZE = 500
CONNECT_TIMEOUT = 20
READ_TIMEOUT = 120
MAX_RETRIES = 8
PAGE_DELAY_SECONDS = 0.35


def get_db_dates(db_path: Path, start_date: str, end_date: str) -> list[str]:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT date
            FROM price_history
            WHERE date BETWEEN ? AND ?
            ORDER BY date
            """,
            (start_date, end_date),
        ).fetchall()
        return [row[0] for row in rows]
    finally:
        conn.close()


def weekday_dates(start_date: str, end_date: str) -> list[str]:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    out = []
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def shard_dates(dates: list[str], worker_index: int, worker_count: int) -> list[str]:
    offset = worker_index - 1
    return [d for i, d in enumerate(dates) if i % worker_count == offset]


def load_dates_file(path: Path) -> list[str]:
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def fetch_json(session: requests.Session, date_str: str, page: int, retries: int = MAX_RETRIES) -> dict:
    params = {"date": date_str, "page": page, "size": PAGE_SIZE}
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(
                BASE_URL,
                params=params,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_err = exc
            if attempt == retries:
                break
            # Back off progressively with a little jitter to avoid synchronized
            # retry spikes across workers.
            sleep_s = min(45, (2 ** attempt) + random.uniform(0.2, 1.5))
            print(
                f"    retry {attempt}/{retries - 1} for {date_str} page {page} "
                f"after {sleep_s:.1f}s: {exc}"
            )
            time.sleep(sleep_s)
    raise RuntimeError(f"Failed {date_str} page {page}: {last_err}")


def output_path(out_dir: Path, date_str: str) -> Path:
    return out_dir / f"date={date_str}" / "data.parquet"


def fetch_one_date(session: requests.Session, date_str: str) -> tuple[pd.DataFrame, int]:
    first = fetch_json(session, date_str, 1)
    rows = list(first.get("data", []))
    last_page = int(first.get("last_page", 1) or 1)

    for page in range(2, last_page + 1):
        time.sleep(PAGE_DELAY_SECONDS)
        payload = fetch_json(session, date_str, page)
        rows.extend(payload.get("data", []))

    df = pd.DataFrame(rows)
    if df.empty:
        return df, last_page

    df["date"] = date_str
    return df, last_page


def save_date(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)


def append_summary(summary_path: Path, row: dict) -> None:
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not summary_path.exists()
    with summary_path.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "date",
                "status",
                "rows",
                "pages",
                "output_path",
                "error",
            ],
        )
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch Chukul date-wise floorsheet data and save one Parquet file per date."
    )
    parser.add_argument("--start-date", default=(date.today() - timedelta(days=365)).isoformat())
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--worker-index", type=int, required=True)
    parser.add_argument("--worker-count", type=int, required=True)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dates-file", help="Optional file with one YYYY-MM-DD date per line.")
    parser.add_argument(
        "--date-source",
        choices=["db", "weekdays"],
        default="db",
        help="Use price_history trading dates from the SQLite DB, or weekday fallback.",
    )
    args = parser.parse_args()

    if args.worker_index < 1 or args.worker_index > args.worker_count:
        raise SystemExit("--worker-index must be between 1 and --worker-count")

    db_path = Path(args.db_path)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.dates_file:
        dates = load_dates_file(Path(args.dates_file))
    else:
        if args.date_source == "db":
            dates = get_db_dates(db_path, args.start_date, args.end_date)
            if not dates:
                print("No DB dates found in range; falling back to weekdays.")
                dates = weekday_dates(args.start_date, args.end_date)
        else:
            dates = weekday_dates(args.start_date, args.end_date)

    assigned_dates = shard_dates(dates, args.worker_index, args.worker_count)
    summary_path = out_dir / f"worker_{args.worker_index:02d}_summary.csv"

    print(
        f"Worker {args.worker_index}/{args.worker_count}: "
        f"{len(assigned_dates)} dates from {args.start_date} to {args.end_date}"
    )

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://chukul.com/floorsheet",
        }
    )

    for idx, date_str in enumerate(assigned_dates, start=1):
        out_path = output_path(out_dir, date_str)
        if out_path.exists() and not args.overwrite:
            print(f"[{idx}/{len(assigned_dates)}] Skip {date_str} (already exists)")
            append_summary(
                summary_path,
                {
                    "date": date_str,
                    "status": "skipped",
                    "rows": "",
                    "pages": "",
                    "output_path": str(out_path),
                    "error": "",
                },
            )
            continue

        print(f"[{idx}/{len(assigned_dates)}] Fetch {date_str}")
        try:
            df, pages = fetch_one_date(session, date_str)
            save_date(df, out_path)
            append_summary(
                summary_path,
                {
                    "date": date_str,
                    "status": "saved",
                    "rows": 0 if df.empty else len(df),
                    "pages": pages,
                    "output_path": str(out_path),
                    "error": "",
                },
            )
            print(f"  saved rows={0 if df.empty else len(df)} pages={pages}")
        except Exception as exc:
            append_summary(
                summary_path,
                {
                    "date": date_str,
                    "status": "error",
                    "rows": "",
                    "pages": "",
                    "output_path": str(out_path),
                    "error": str(exc),
                },
            )
            print(f"  error: {exc}")


if __name__ == "__main__":
    main()
