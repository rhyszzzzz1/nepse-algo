import argparse
import time
from pathlib import Path

import pandas as pd
import requests


BASE_URL = "https://sharehubnepal.com/live/api/v2/floorsheet"
PAGE_SIZE = 100
TIMEOUT = 60
PAGE_DELAY_SECONDS = 0.2


def fetch_page(session: requests.Session, date_str: str, page: int) -> dict:
    resp = session.get(
        BASE_URL,
        params={"date": date_str, "page": page, "size": PAGE_SIZE},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    payload = resp.json()
    if not payload.get("success"):
        raise RuntimeError(f"ShareHub request failed for {date_str} page {page}: {payload}")
    return payload["data"]


def fetch_date(date_str: str) -> pd.DataFrame:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://sharehubnepal.com/",
        }
    )

    first = fetch_page(session, date_str, 1)
    rows = list(first.get("content", []))
    total_pages = int(first.get("totalPages", 1) or 1)

    print(
        f"Fetching ShareHub {date_str}: totalItems={first.get('totalItems')} "
        f"totalPages={total_pages}"
    )

    for page in range(2, total_pages + 1):
        if page % 50 == 0 or page == total_pages:
            print(f"  page {page}/{total_pages}")
        time.sleep(PAGE_DELAY_SECONDS)
        payload = fetch_page(session, date_str, page)
        rows.extend(payload.get("content", []))

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(
            columns=["transaction", "symbol", "buyer", "seller", "quantity", "rate", "amount", "date"]
        )

    out = pd.DataFrame(
        {
            "transaction": df["contractId"],
            "symbol": df["symbol"],
            "buyer": df["buyerMemberId"],
            "seller": df["sellerMemberId"],
            "quantity": df["contractQuantity"],
            "rate": df["contractRate"],
            "amount": df["contractAmount"],
            "date": date_str,
        }
    )
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch one date of floorsheet data from ShareHub.")
    parser.add_argument("--date", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    out_path = Path(args.out_dir) / f"date={args.date}" / "data.parquet"
    if out_path.exists() and not args.overwrite:
        print(f"Skip {args.date}: {out_path} already exists")
        return

    df = fetch_date(args.date)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    print(f"Saved {len(df)} rows to {out_path}")


if __name__ == "__main__":
    main()
