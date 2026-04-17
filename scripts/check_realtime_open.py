from __future__ import annotations

import argparse
import sqlite3
import time
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = ROOT / "data" / "nepse.db"
DEFAULT_LOG_PATH = ROOT / "data" / "logs" / "sharehub_realtime_sync.log"
DEFAULT_OUT_PATH = ROOT / "data" / "logs" / "sharehub_realtime_open_check.log"
TIMEZONE = ZoneInfo("Asia/Kathmandu")


def parse_args():
    parser = argparse.ArgumentParser(description="Wait until shortly after market open and verify realtime sync landed.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--realtime-log", default=str(DEFAULT_LOG_PATH))
    parser.add_argument("--out", default=str(DEFAULT_OUT_PATH))
    parser.add_argument("--target-time", default="11:01")
    return parser.parse_args()


def now_local() -> datetime:
    return datetime.now(TIMEZONE)


def parse_clock(value: str) -> dt_time:
    hour, minute = value.split(":")
    return dt_time(int(hour), int(minute))


def next_target(target_clock: dt_time) -> datetime:
    current = now_local()
    candidate = current.replace(
        hour=target_clock.hour,
        minute=target_clock.minute,
        second=0,
        microsecond=0,
    )
    if candidate <= current:
        candidate = candidate + timedelta(days=1)
    return candidate


def wait_until(target: datetime) -> None:
    while True:
        remaining = (target - now_local()).total_seconds()
        if remaining <= 0:
            return
        time.sleep(min(remaining, 30))


def collect_counts(db_path: Path, trade_date: str) -> dict[str, int]:
    conn = sqlite3.connect(db_path)
    try:
        return {
            "market_summary": int(conn.execute("SELECT COUNT(*) FROM market_summary WHERE date = ?", (trade_date,)).fetchone()[0]),
            "sector_index": int(conn.execute("SELECT COUNT(*) FROM sector_index WHERE date = ?", (trade_date,)).fetchone()[0]),
            "daily_price": int(conn.execute("SELECT COUNT(*) FROM daily_price WHERE date = ?", (trade_date,)).fetchone()[0]),
            "price_history": int(conn.execute("SELECT COUNT(*) FROM price_history WHERE date = ?", (trade_date,)).fetchone()[0]),
            "floor_sheet": int(conn.execute("SELECT COUNT(*) FROM floor_sheet WHERE date = ?", (trade_date,)).fetchone()[0]),
        }
    finally:
        conn.close()


def tail_lines(path: Path, count: int = 10) -> list[str]:
    if not path.exists():
        return ["[check] realtime log file not found"]
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    return lines[-count:] if lines else ["[check] realtime log file is empty"]


def main():
    args = parse_args()
    db_path = Path(args.db_path)
    realtime_log = Path(args.realtime_log)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    target = next_target(parse_clock(args.target_time))
    wait_until(target)

    trade_date = now_local().date().isoformat()
    counts = collect_counts(db_path, trade_date)
    lines = tail_lines(realtime_log, 12)

    report_lines = [
        f"[check] completed_at={now_local().isoformat()}",
        f"[check] trade_date={trade_date}",
        *[f"[check] {key}={value}" for key, value in counts.items()],
        "[check] realtime_log_tail:",
        *lines,
    ]
    out_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
