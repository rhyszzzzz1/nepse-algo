from __future__ import annotations

import json
import sys

from src.data.derived_analytics import build_latest_snapshots


def main() -> int:
    as_of_date = sys.argv[1] if len(sys.argv) > 1 else None
    result = build_latest_snapshots(as_of_date=as_of_date)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
