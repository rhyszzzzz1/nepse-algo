from __future__ import annotations

import argparse
import asyncio
import json
import sys

from src.nepse_trading_dashboard.scraper import run_dashboard_scrape


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scrape NEPSETrading broker-analysis dashboard sections.")
    parser.add_argument(
        "--section",
        action="append",
        default=[],
        help="Section name or slug to scrape. Repeat to select multiple sections.",
    )
    return parser


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = build_parser()
    args = parser.parse_args()
    result = asyncio.run(run_dashboard_scrape(args.section))
    print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
