from __future__ import annotations

import sqlite3
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests

from src.data.fetcher import fetch_sharehub_index_history, sync_daily_price_into_price_history
from src.data.floorsheet_pipeline import (
    _save_rows,
    compute_broker_summary,
    compute_daily_price,
)


TARGET_DATE = "2026-03-30"
SHAREHUB_FLOORSHEET_URL = "https://sharehubnepal.com/live/api/v2/floorsheet"
CHUKUL_FLOORSHEET_URL = "https://chukul.com/api/data/v2/floorsheet/bydate/"
USER_AGENT = "Mozilla/5.0"
SHAREHUB_WORKERS = 50
CHUKUL_WORKERS = 4
DB_PATH = r"C:\nepse-algo\data\nepse.db"


def _sharehub_date(date_str: str) -> str:
    return f"{int(date_str[:4])}-{int(date_str[5:7])}-{int(date_str[8:10])}"


def _map_sharehub_row(row: dict, fetched_at: str):
    business_date = str(row.get("businessDate") or TARGET_DATE).strip()
    if "T" in business_date:
        business_date = business_date.split("T", 1)[0]
    return (
        business_date,
        str(row.get("symbol") or row.get("stockSymbol") or "").upper(),
        int(float(row.get("buyerMemberId") or 0)),
        int(float(row.get("sellerMemberId") or 0)),
        float(row.get("contractQuantity") or 0),
        float(row.get("contractRate") or 0),
        float(row.get("contractAmount") or 0),
        fetched_at,
    )


def _map_chukul_row(row: dict, fetched_at: str):
    return (
        str(row.get("date") or TARGET_DATE).strip(),
        str(row.get("symbol") or "").upper(),
        int(float(row.get("buyer") or 0)),
        int(float(row.get("seller") or 0)),
        float(row.get("quantity") or 0),
        float(row.get("rate") or 0),
        float(row.get("amount") or 0),
        fetched_at,
    )


def _fetch_json(url: str, params: dict, timeout: int = 30, retries: int = 6) -> dict:
    last_error = None
    for attempt in range(retries):
        try:
            resp = requests.get(
                url,
                params=params,
                timeout=timeout,
                headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            )
            if resp.status_code == 429:
                wait = min(2 ** attempt, 30)
                last_error = RuntimeError(
                    f"HTTP 429 after attempt {attempt + 1} for {url} page={params.get('page') or params.get('currentPage')}"
                )
                print(f"[HTTP] 429 for {url} page={params.get('page') or params.get('currentPage')} wait={wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.SSLError as exc:
            last_error = exc
            wait = min(2 ** attempt, 20)
            print(f"[HTTP] SSL retry for {url} page={params.get('page') or params.get('currentPage')} wait={wait}s")
            time.sleep(wait)
        except requests.RequestException as exc:
            last_error = exc
            if attempt == retries - 1:
                break
            wait = min(2 ** attempt, 20)
            print(f"[HTTP] retry for {url} page={params.get('page') or params.get('currentPage')} wait={wait}s reason={exc}")
            time.sleep(wait)
    if last_error is None:
        last_error = RuntimeError(
            f"Request retries exhausted for {url} page={params.get('page') or params.get('currentPage')}"
        )
    raise last_error


def fetch_sharehub_floorsheet_50(date_str: str) -> int:
    print(f"[ShareHub] Fetching floorsheet for {date_str} with {SHAREHUB_WORKERS} workers...")
    fetched_at = datetime.now().isoformat()
    page_size = 100
    payload = _fetch_json(
        SHAREHUB_FLOORSHEET_URL,
        {"Size": page_size, "currentPage": 1, "date": _sharehub_date(date_str)},
    )
    content = ((payload.get("data") or {}).get("content")) or []
    total_pages = int(((payload.get("data") or {}).get("totalPages")) or 1)
    total_items = int(((payload.get("data") or {}).get("totalItems")) or 0)
    print(f"[ShareHub] total_pages={total_pages} total_items={total_items}")
    total_saved = _save_rows([_map_sharehub_row(row, fetched_at) for row in content])
    print(f"[ShareHub] page 1 saved={total_saved}")

    def fetch_page(page: int):
        data = _fetch_json(
            SHAREHUB_FLOORSHEET_URL,
            {"Size": page_size, "currentPage": page, "date": _sharehub_date(date_str)},
        )
        rows = ((data.get("data") or {}).get("content")) or []
        saved = _save_rows([_map_sharehub_row(row, fetched_at) for row in rows])
        return page, saved, len(rows)

    if total_pages > 1:
        with ThreadPoolExecutor(max_workers=SHAREHUB_WORKERS) as executor:
            futures = {executor.submit(fetch_page, page): page for page in range(2, total_pages + 1)}
            completed = 1
            for future in as_completed(futures):
                page, saved, row_count = future.result()
                total_saved += saved
                completed += 1
                if completed % 50 == 0 or completed == total_pages:
                    print(f"[ShareHub] progress {completed}/{total_pages} pages | rows={row_count} | total_saved={total_saved}")

    print(f"[ShareHub] complete total_saved={total_saved}")
    return total_saved


def fetch_chukul_floorsheet_50(date_str: str) -> int:
    print(f"[Chukul] Fetching floorsheet for {date_str} with {CHUKUL_WORKERS} workers...")
    fetched_at = datetime.now().isoformat()
    page_size = 500
    payload = _fetch_json(CHUKUL_FLOORSHEET_URL, {"date": date_str, "page": 1, "size": page_size})
    rows = payload.get("data") or []
    total_pages = int(payload.get("last_page") or 1)
    total_saved = _save_rows([_map_chukul_row(row, fetched_at) for row in rows])
    print(f"[Chukul] total_pages={total_pages} page1_saved={total_saved}")

    failed_pages = []

    def fetch_page(page: int):
        try:
            data = _fetch_json(CHUKUL_FLOORSHEET_URL, {"date": date_str, "page": page, "size": page_size})
            page_rows = data.get("data") or []
            saved = _save_rows([_map_chukul_row(row, fetched_at) for row in page_rows])
            return page, saved, len(page_rows), None
        except Exception as exc:
            return page, 0, 0, exc

    if total_pages > 1:
        with ThreadPoolExecutor(max_workers=CHUKUL_WORKERS) as executor:
            futures = {executor.submit(fetch_page, page): page for page in range(2, total_pages + 1)}
            completed = 1
            for future in as_completed(futures):
                page, saved, row_count, error = future.result()
                if error is None:
                    total_saved += saved
                else:
                    failed_pages.append(page)
                    print(f"[Chukul] page {page} failed after retries: {error}")
                completed += 1
                if completed % 20 == 0 or completed == total_pages:
                    print(
                        f"[Chukul] progress {completed}/{total_pages} pages | rows={row_count} | "
                        f"total_saved={total_saved} | failed_pages={len(failed_pages)}"
                    )

    if failed_pages:
        print(f"[Chukul] Retrying {len(failed_pages)} failed pages sequentially...")
        for idx, page in enumerate(sorted(failed_pages), start=1):
            try:
                data = _fetch_json(CHUKUL_FLOORSHEET_URL, {"date": date_str, "page": page, "size": page_size}, retries=8)
                page_rows = data.get("data") or []
                saved = _save_rows([_map_chukul_row(row, fetched_at) for row in page_rows])
                total_saved += saved
                if idx % 10 == 0 or idx == len(failed_pages):
                    print(f"[Chukul] retry progress {idx}/{len(failed_pages)} | total_saved={total_saved}")
            except Exception as exc:
                print(f"[Chukul] sequential retry still failed for page {page}: {exc}")

    print(f"[Chukul] complete total_saved={total_saved}")
    return total_saved


def existing_floorsheet_status(date_str: str) -> tuple[int, int]:
    conn = sqlite3.connect(DB_PATH)
    try:
        row_count, symbol_count = conn.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT symbol)
            FROM floor_sheet
            WHERE date = ?
            """,
            (date_str,),
        ).fetchone()
        return int(row_count or 0), int(symbol_count or 0)
    finally:
        conn.close()


def main():
    print("=" * 72)
    print(f"VISIBLE MARCH 30 REFRESH STARTED: {datetime.now().isoformat()}")
    print("=" * 72)

    print("[1/6] Fetch ShareHub index history...")
    print(fetch_sharehub_index_history())

    print("[2/6] Fetch floorsheet...")
    existing_rows, existing_symbols = existing_floorsheet_status(TARGET_DATE)
    if existing_rows and existing_symbols:
        print(
            f"[Floorsheet] existing rows detected for {TARGET_DATE}: "
            f"rows={existing_rows} symbols={existing_symbols} | skipping refetch"
        )
    else:
        saved = 0
        try:
            saved = fetch_sharehub_floorsheet_50(TARGET_DATE)
        except Exception as exc:
            print(f"[ShareHub] floorsheet failed: {exc}")
        if not saved:
            print("[ShareHub] no usable rows; falling back to Chukul")
            saved = fetch_chukul_floorsheet_50(TARGET_DATE)
        final_rows, final_symbols = existing_floorsheet_status(TARGET_DATE)
        print(
            f"[Floorsheet] total saved rows for {TARGET_DATE}: {saved} | "
            f"db_rows={final_rows} symbols={final_symbols}"
        )

    print("[3/6] Recompute broker_summary and daily_price for target date...")
    compute_broker_summary([TARGET_DATE])
    compute_daily_price([TARGET_DATE])

    print("[4/6] Sync latest daily_price into price_history...")
    print(sync_daily_price_into_price_history(TARGET_DATE))

    print("[5/6] Run clean + signals + NEPSE signals...")
    subprocess.run(
        ["python", ".\\src\\pipeline\\run_pipeline.py", "--skip-fetch"],
        check=True,
    )

    print("[6/6] Rebuild derived + ML layers...")
    subprocess.run(["python", ".\\build_derived_analytics.py"], check=True)
    subprocess.run(["python", ".\\build_ml_feature_snapshot.py"], check=True)
    subprocess.run(["python", ".\\export_lightgbm_predictions.py"], check=True)

    print("=" * 72)
    print(f"VISIBLE MARCH 30 REFRESH DONE: {datetime.now().isoformat()}")
    print("=" * 72)
    time.sleep(2)


if __name__ == "__main__":
    main()
