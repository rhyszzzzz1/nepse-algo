import argparse
import csv
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parent
DEFAULT_OUT_DIR = ROOT / "data" / "chukul_floorsheet"
FETCH_SCRIPT = ROOT / "fetch_chukul_floorsheet_year.py"


def scan_failed_dates(out_dir: Path) -> list[str]:
    status_by_date: dict[str, str] = {}
    for summary_path in sorted(out_dir.glob("worker_*_summary.csv")):
        with summary_path.open(newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                date_str = (row.get("date") or "").strip()
                status = (row.get("status") or "").strip()
                if not date_str:
                    continue
                if status == "saved":
                    status_by_date[date_str] = "saved"
                elif status == "error" and status_by_date.get(date_str) != "saved":
                    status_by_date[date_str] = "error"
    failed = []
    for date_str, status in sorted(status_by_date.items()):
        if status != "error":
            continue
        parquet_path = out_dir / f"date={date_str}" / "data.parquet"
        if not parquet_path.exists():
            failed.append(date_str)
    return failed


def write_dates_file(path: Path, dates: list[str]) -> None:
    path.write_text("\n".join(dates), encoding="utf-8")


def run_retry_round(
    out_dir: Path,
    dates_file: Path,
    worker_count: int,
    round_no: int,
) -> int:
    procs = []
    for worker_index in range(1, worker_count + 1):
        cmd = [
            sys.executable,
            str(FETCH_SCRIPT),
            "--dates-file",
            str(dates_file),
            "--worker-index",
            str(worker_index),
            "--worker-count",
            str(worker_count),
            "--out-dir",
            str(out_dir),
            "--overwrite",
        ]
        procs.append(subprocess.Popen(cmd, cwd=str(ROOT)))

    exit_code = 0
    for proc in procs:
        rc = proc.wait()
        if rc != 0:
            exit_code = rc
    return exit_code


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Continuously scan Chukul summaries for failed dates and retry them."
    )
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--worker-count", type=int, default=4)
    parser.add_argument("--poll-seconds", type=int, default=45)
    parser.add_argument("--max-rounds", type=int, default=20)
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    retry_dates_path = out_dir / "_monitor_retry_dates.txt"

    for round_no in range(1, args.max_rounds + 1):
        failed_dates = scan_failed_dates(out_dir)
        if not failed_dates:
            print("No failed dates found. Monitoring complete.")
            return

        print(f"Round {round_no}: retrying {len(failed_dates)} failed dates")
        for d in failed_dates:
            print(f"  {d}")

        write_dates_file(retry_dates_path, failed_dates)
        rc = run_retry_round(out_dir, retry_dates_path, args.worker_count, round_no)
        if rc != 0:
            print(f"Retry round {round_no} completed with non-zero exit code: {rc}")

        time.sleep(args.poll_seconds)

    remaining = scan_failed_dates(out_dir)
    print(f"Stopped after {args.max_rounds} rounds. Remaining failed dates: {len(remaining)}")
    for d in remaining:
        print(f"  {d}")


if __name__ == "__main__":
    main()
