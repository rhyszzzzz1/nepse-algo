"""
Run a full daily data refresh for this NEPSE project.

What this updates:
1) OHLCV + market summary + sector indexes (via src/pipeline/run_pipeline.py step 1)
2) ShareHub homepage feeds: index analysis, announcements, news feed, public offerings
3) Floorsheet + broker_summary + daily_price (via src/data/floorsheet_pipeline.py --daily)
4) Company metadata + news + dividend (via src/data/merolagani_pipeline.py)
5) clean_price_history + signals + nepse_signals (via src/pipeline/run_pipeline.py --skip-fetch)
6) derived analytics snapshots for scanners/profile pages (via build_derived_analytics.py)
7) export latest LightGBM prediction batch into ml_predictions_daily (via export_lightgbm_predictions.py)

Optional:
- Upload all derived tables to Turso at the end.

Usage:
  python daily_update_all_data.py
  python daily_update_all_data.py --skip-news-dividend
  python daily_update_all_data.py --with-turso-upload
"""

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime


ROOT = os.path.dirname(os.path.abspath(__file__))


def run_step(step_name, args):
    cmd = [sys.executable] + args
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    start = time.time()
    print("\n" + "=" * 72)
    print(f"[STEP] {step_name}")
    print(f"[CMD ] {' '.join(cmd)}")
    print("=" * 72)
    try:
        subprocess.run(cmd, cwd=ROOT, check=True, env=env)
    except subprocess.CalledProcessError as exc:
        elapsed = time.time() - start
        print(f"[FAIL] {step_name} failed after {elapsed:.1f}s (exit={exc.returncode})")
        raise
    elapsed = time.time() - start
    print(f"[ OK ] {step_name} completed in {elapsed:.1f}s")


def main():
    parser = argparse.ArgumentParser(description="Daily updater for all project data")
    parser.add_argument(
        "--skip-news-dividend",
        action="store_true",
        help="Skip Merolagani company tabs (about/news/dividend)",
    )
    parser.add_argument(
        "--skip-fetch-step",
        action="store_true",
        help="Skip run_pipeline step 1 (OHLCV/market/sector fetch)",
    )
    parser.add_argument(
        "--skip-signals",
        action="store_true",
        help="Skip clean/signals/nepse_signals refresh",
    )
    parser.add_argument(
        "--no-prune-raw",
        action="store_true",
        help="Keep raw floor_sheet rows instead of pruning after daily floorsheet update",
    )
    parser.add_argument(
        "--with-turso-upload",
        action="store_true",
        help="Run upload_to_turso.py after local update finishes",
    )
    parser.add_argument(
        "--skip-derived",
        action="store_true",
        help="Skip derived analytics snapshot rebuild",
    )
    parser.add_argument(
        "--skip-ml-export",
        action="store_true",
        help="Skip latest LightGBM prediction export",
    )
    args = parser.parse_args()

    started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("=" * 72)
    print("NEPSE DAILY ALL-DATA UPDATE")
    print(f"Start time: {started}")
    print(f"Python: {sys.executable}")
    print("=" * 72)

    if not args.skip_fetch_step:
        run_step(
            "Fetch OHLCV + market summary + sector indexes",
            ["src/pipeline/run_pipeline.py", "--step", "1"],
        )
        run_step(
            "Fetch ShareHub per-date index analysis (homepage market snapshot)",
            [
                "-c",
                "from datetime import datetime; from src.data.fetcher import fetch_sharehub_index_analysis_for_date; print(fetch_sharehub_index_analysis_for_date(datetime.now().strftime('%Y-%m-%d')))",
            ],
        )
        run_step(
            "Fetch Chukul sector-wise market-cap stock buckets",
            [
                "-c",
                "from src.data.fetcher import fetch_chukul_sector_low_cap; print(fetch_chukul_sector_low_cap())",
            ],
        )
        run_step(
            "Fetch ShareHub announcements",
            [
                "-c",
                "from src.data.fetcher import fetch_sharehub_announcements; print(fetch_sharehub_announcements())",
            ],
        )
        run_step(
            "Fetch ShareHub news feed",
            [
                "-c",
                "from src.data.fetcher import fetch_sharehub_news_feed; print(fetch_sharehub_news_feed())",
            ],
        )
        run_step(
            "Fetch ShareHub public offerings",
            [
                "-c",
                "from src.data.fetcher import fetch_sharehub_public_offerings; print(fetch_sharehub_public_offerings())",
            ],
        )

    fs_args = ["src/data/floorsheet_pipeline.py", "--daily"]
    if not args.no_prune_raw:
        fs_args.append("--prune-raw")
    run_step("Daily floorsheet + broker summary + daily price", fs_args)

    run_step(
        "Sync latest daily_price snapshot into price_history",
        [
            "-c",
            "from src.data.fetcher import sync_daily_price_into_price_history; print(sync_daily_price_into_price_history())",
        ],
    )

    if not args.skip_news_dividend:
        run_step(
            "Company metadata + news + dividend refresh",
            [
                "src/data/merolagani_pipeline.py",
                "--skip-ohlcv",
                "--skip-floorsheet",
            ],
        )

    if not args.skip_signals:
        run_step(
            "Clean + technical signals + NEPSE signals",
            ["src/pipeline/run_pipeline.py", "--skip-fetch"],
        )

    if not args.skip_derived:
        run_step(
            "Derived analytics snapshots for scanners and profiles",
            ["build_derived_analytics.py"],
        )

    if not args.skip_ml_export:
        run_step(
            "Export latest LightGBM prediction batch",
            ["export_lightgbm_predictions.py"],
        )

    if args.with_turso_upload:
        run_step("Upload local DB tables to Turso", ["upload_to_turso.py"])

    ended = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("\n" + "=" * 72)
    print("[DONE] Daily all-data update completed successfully")
    print(f"End time: {ended}")
    print("=" * 72)


if __name__ == "__main__":
    main()
