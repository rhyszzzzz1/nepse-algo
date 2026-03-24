import csv
import sqlite3
import subprocess
import time
from pathlib import Path


ROOT = Path(r"C:\nepse-algo")
DB_PATH = ROOT / "data" / "nepse.db"
CHUKUL_DIR = ROOT / "data" / "chukul_floorsheet"
PARQUET_DIR = ROOT / "data" / "parquet_ml"


def trading_date_count() -> int:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("select count(distinct date) from price_history").fetchone()
    return int(row[0] or 0)


def count_saved_dates() -> int:
    if not CHUKUL_DIR.exists():
        return 0
    return sum(1 for p in CHUKUL_DIR.iterdir() if p.is_dir() and p.name.startswith("date="))


def count_failed_dates() -> int:
    failed = set()
    for path in CHUKUL_DIR.glob("worker_*_summary.csv"):
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("status") == "error":
                    date = row.get("date", "")
                    parquet = CHUKUL_DIR / f"date={date}" / "data.parquet"
                    if date and not parquet.exists():
                        failed.add(date)
    return len(failed)


def parquet_tables_done() -> int:
    if not PARQUET_DIR.exists():
        return 0
    return sum(1 for p in PARQUET_DIR.iterdir() if p.is_dir())


def parquet_done() -> bool:
    return (PARQUET_DIR / "_export_summary.json").exists()


def proc_count(pattern: str) -> int:
    cmd = (
        "Get-CimInstance Win32_Process | "
        f"Where-Object {{ $_.CommandLine -match '{pattern}' }} | "
        "Measure-Object | Select-Object -ExpandProperty Count"
    )
    result = subprocess.run(
        ["powershell", "-NoProfile", "-Command", cmd],
        capture_output=True,
        text=True,
        check=False,
    )
    try:
        return int((result.stdout or "").strip() or "0")
    except ValueError:
        return 0


def main() -> None:
    total_dates = trading_date_count()
    while True:
        saved = count_saved_dates()
        failed = count_failed_dates()
        remaining = max(total_dates - saved, 0)
        fetch_proc = proc_count("fetch_chukul_floorsheet_year.py")
        retry_proc = proc_count("monitor_chukul_retries.py")
        parquet_proc = proc_count("export_sqlite_to_parquet.py")
        parquet_tables = parquet_tables_done()
        done_pct = (saved / total_dates * 100) if total_dates else 0.0

        print("\n" + "=" * 72)
        print(time.strftime("%Y-%m-%d %H:%M:%S"))
        print(f"Floorsheet dates: {saved}/{total_dates} ({done_pct:.2f}%)")
        print(f"Remaining dates: {remaining}")
        print(f"Failed dates waiting on retry: {failed}")
        print(f"Fetch worker processes seen: {fetch_proc}")
        print(f"Retry monitor processes seen: {retry_proc}")
        print(f"Parquet exporter processes seen: {parquet_proc}")
        print(f"Parquet table folders present: {parquet_tables}")
        print(f"Parquet export summary present: {parquet_done()}")
        print("=" * 72)
        time.sleep(30)


if __name__ == "__main__":
    main()
