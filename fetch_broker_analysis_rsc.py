from __future__ import annotations

import argparse
import asyncio
import json
import sys

from src.nepse_trading_dashboard.rsc_fetcher import run_rsc_fetch


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch broker-analysis data from authenticated _rsc routes.")
    parser.add_argument("--code", default="NABIL", help="Stock code to request.")
    parser.add_argument("--broker-number", default="1", help="Broker number to request.")
    return parser


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    args = build_parser().parse_args()
    result = asyncio.run(run_rsc_fetch(code=args.code, broker_number=args.broker_number))
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
