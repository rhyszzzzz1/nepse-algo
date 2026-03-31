import argparse
import os
import re
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(ROOT, "data", "nepse.db")
PIPELINE_PATH = os.path.join(ROOT, "src", "data", "merolagani_pipeline.py")
LOG_PATH = os.path.join(ROOT, "data", "broker_backfill.log")
SAVED_ROWS_RE = re.compile(r"Saved\s+(\d+)\s+floorsheet\s+rows", re.IGNORECASE)


def default_start_date():
    return (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")


def load_symbols():
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute("SELECT symbol FROM instruments ORDER BY symbol").fetchall()
        return [row[0] for row in rows]
    finally:
        conn.close()


def broker_days_for_symbol(symbol, start_date, end_date):
    conn = sqlite3.connect(DB_PATH)
    try:
        row = conn.execute(
            """
            SELECT COUNT(DISTINCT date), MIN(date), MAX(date)
            FROM broker_summary
            WHERE symbol = ? AND date >= ? AND date <= ?
            """,
            (symbol, start_date, end_date),
        ).fetchone()
        return row
    finally:
        conn.close()


def append_log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line, flush=True)
    with open(LOG_PATH, "a", encoding="utf-8") as log_file:
        log_file.write(line + "\n")


def run_symbol(symbol, start_date, end_date, progress_hook=None):
    cmd = [
        sys.executable,
        PIPELINE_PATH,
        "--broker-only",
        "--symbols",
        symbol,
        "--floor-start",
        start_date,
        "--floor-end",
        end_date,
    ]
    started = time.time()
    fetched_rows = 0
    save_events = 0
    tail_lines = []

    proc = subprocess.Popen(
        cmd,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    if proc.stdout:
        for raw_line in proc.stdout:
            line = raw_line.rstrip("\n")
            if not line:
                continue
            tail_lines.append(line)
            if len(tail_lines) > 80:
                tail_lines = tail_lines[-80:]

            match = SAVED_ROWS_RE.search(line)
            if match:
                fetched_rows += int(match.group(1))
                save_events += 1
                if progress_hook:
                    progress_hook(fetched_rows, save_events, time.time() - started)

    return_code = proc.wait()
    duration = max(0.001, time.time() - started)
    stdout_tail = "\n".join(tail_lines[-20:])
    return {
        "returncode": return_code,
        "fetched_rows": fetched_rows,
        "duration_sec": duration,
        "stdout_tail": stdout_tail,
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Backfill one year of broker summary for all symbols")
    parser.add_argument("--start", default=default_start_date(), help="Start date in YYYY-MM-DD")
    parser.add_argument("--end", default=datetime.now().strftime("%Y-%m-%d"), help="End date in YYYY-MM-DD")
    parser.add_argument("--resume-after", default=None, help="Resume after this symbol (exclusive)")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay in seconds between symbols")
    return parser.parse_args()


def main():
    args = parse_args()
    symbols = load_symbols()
    if args.resume_after:
        marker = args.resume_after.strip().upper()
        symbols = [symbol for symbol in symbols if symbol.upper() > marker]

    append_log(f"Starting broker backfill for {len(symbols)} symbols from {args.start} to {args.end}")

    completed = 0
    failed = []
    run_started = time.time()
    cumulative_fetched_rows = 0

    for index, symbol in enumerate(symbols, start=1):
        symbols_total = len(symbols)
        symbols_done_before = index - 1
        symbols_left_before = symbols_total - symbols_done_before
        before_days, before_min, before_max = broker_days_for_symbol(symbol, args.start, args.end)
        append_log(
            f"[{index}/{symbols_total}] {symbol}: before_days={before_days} range={before_min}..{before_max} | symbols_done={symbols_done_before}/{symbols_total} symbols_left={symbols_left_before}"
        )

        def on_symbol_progress(symbol_rows, save_events, symbol_elapsed):
            if save_events == 1 or save_events % 25 == 0:
                total_rows_done = cumulative_fetched_rows + symbol_rows
                symbols_left_after_current = symbols_total - index
                symbols_for_avg = max(1, index)
                est_rows_left = int((total_rows_done / symbols_for_avg) * symbols_left_after_current)
                symbol_rps = symbol_rows / max(0.001, symbol_elapsed)
                append_log(
                    f"[{index}/{symbols_total}] {symbol}: progress rows_done={total_rows_done:,} est_rows_left={est_rows_left:,} symbol_rows={symbol_rows:,} symbol_rps={symbol_rps:.2f} symbols_done={index}/{symbols_total} symbols_left={symbols_left_after_current}"
                )

        result = run_symbol(symbol, args.start, args.end, progress_hook=on_symbol_progress)
        after_days, after_min, after_max = broker_days_for_symbol(symbol, args.start, args.end)
        cumulative_fetched_rows += result["fetched_rows"]
        symbols_left_after = symbols_total - index
        rows_per_second_overall = cumulative_fetched_rows / max(0.001, time.time() - run_started)
        rows_per_second_symbol = result["fetched_rows"] / max(0.001, result["duration_sec"])
        est_rows_left = int((cumulative_fetched_rows / max(1, index)) * symbols_left_after)

        if result["returncode"] == 0:
            completed += 1
            append_log(
                f"[{index}/{symbols_total}] {symbol}: ok after_days={after_days} range={after_min}..{after_max} | symbol_rows_fetched={result['fetched_rows']:,} symbol_rps={rows_per_second_symbol:.2f} total_rows_done={cumulative_fetched_rows:,} est_rows_left={est_rows_left:,} overall_rps={rows_per_second_overall:.2f} symbols_done={index}/{symbols_total} symbols_left={symbols_left_after}"
            )
        else:
            failed.append(symbol)
            append_log(
                f"[{index}/{symbols_total}] {symbol}: failed code={result['returncode']} after_days={after_days} | symbol_rows_fetched={result['fetched_rows']:,} symbol_rps={rows_per_second_symbol:.2f} total_rows_done={cumulative_fetched_rows:,} est_rows_left={est_rows_left:,} overall_rps={rows_per_second_overall:.2f} symbols_done={index}/{symbols_total} symbols_left={symbols_left_after}"
            )

        if result["stdout_tail"]:
            append_log(f"[{index}/{symbols_total}] {symbol}: stdout_tail={result['stdout_tail'][-1000:]}")

        time.sleep(args.delay)

    append_log(f"Finished. completed={completed} failed={len(failed)}")
    if failed:
        append_log("Failed symbols: " + ", ".join(failed))


if __name__ == "__main__":
    main()
