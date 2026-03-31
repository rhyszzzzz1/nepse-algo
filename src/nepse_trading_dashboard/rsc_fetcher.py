from __future__ import annotations

import asyncio
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from playwright.async_api import Browser, BrowserContext, Page, async_playwright


ROOT = Path(__file__).resolve().parents[2]


def _parse_balanced_json(text: str, start_index: int) -> tuple[Any, int]:
    opening = text[start_index]
    closing = "}" if opening == "{" else "]"
    depth = 0
    in_string = False
    escape = False

    for index in range(start_index, len(text)):
        char = text[index]
        if in_string:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
            continue

        if char == '"':
            in_string = True
            continue

        if char == opening:
            depth += 1
        elif char == closing:
            depth -= 1
            if depth == 0:
                snippet = text[start_index:index + 1]
                return json.loads(snippet), index + 1

    raise ValueError("Unbalanced JSON block in RSC payload.")


def _extract_object_with_marker(text: str, marker: str) -> dict[str, Any]:
    index = text.find(marker)
    if index == -1:
        raise ValueError(f"Marker not found: {marker}")
    obj, _ = _parse_balanced_json(text, index)
    if not isinstance(obj, dict):
        raise ValueError(f"Marker did not resolve to an object: {marker}")
    return obj


def _extract_all_objects_with_marker(text: str, marker: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    search_from = 0
    while True:
        index = text.find(marker, search_from)
        if index == -1:
            break
        obj, end_index = _parse_balanced_json(text, index)
        if isinstance(obj, dict):
            items.append(obj)
        search_from = end_index
    return items


def _extract_largest_data_array(text: str) -> list[dict[str, Any]]:
    arrays: list[list[dict[str, Any]]] = []
    search_from = 0
    while True:
        index = text.find('"data":[', search_from)
        if index == -1:
            break
        start = text.find("[", index)
        parsed, end_index = _parse_balanced_json(text, start)
        if isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
            arrays.append(parsed)
        search_from = end_index
    if not arrays:
        return []
    arrays.sort(key=len, reverse=True)
    return arrays[0]


def _resolve_dataset_references(datasets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ref_pattern = re.compile(r"children:(\d+):props:data:(\d+)")
    resolved_sets: list[dict[str, Any]] = []

    for dataset in datasets:
        cloned = dict(dataset)
        resolved_rows: list[Any] = []
        for item in dataset.get("data", []):
            if isinstance(item, str):
                match = ref_pattern.search(item)
                if match:
                    dataset_index = int(match.group(1))
                    row_index = int(match.group(2))
                    if dataset_index < len(resolved_sets):
                        source_rows = resolved_sets[dataset_index].get("data", [])
                        if row_index < len(source_rows) and isinstance(source_rows[row_index], dict):
                            resolved_rows.append(source_rows[row_index])
                            continue
            resolved_rows.append(item)
        cloned["data"] = resolved_rows
        resolved_sets.append(cloned)

    return resolved_sets


@dataclass(slots=True)
class RSCFetchResult:
    url: str
    raw_text: str
    parsed: dict[str, Any]


class BrokerAnalysisRSCFetcher:
    def __init__(
        self,
        email: str | None = None,
        password: str | None = None,
        login_url: str | None = None,
        output_dir: str | os.PathLike[str] | None = None,
        headless: bool = True,
    ) -> None:
        self.email = email or os.getenv("NEPSE_TRADING_EMAIL")
        self.password = password or os.getenv("NEPSE_TRADING_PASSWORD")
        self.login_url = login_url or os.getenv("NEPSE_TRADING_LOGIN_URL", "https://nepsetrading.com/login")
        self.output_dir = Path(output_dir or "data/broker_rsc")
        if not self.output_dir.is_absolute():
            self.output_dir = ROOT / self.output_dir
        self.headless = headless

    async def fetch_all(self, code: str = "NABIL", broker_number: str = "1") -> dict[str, Any]:
        async with self._browser_page() as page:
            await self._login(page)
            sections = await self.fetch_supported_sections(page, code=code, broker_number=broker_number)

        run = {
            "code": code,
            "broker_number": broker_number,
            **sections,
        }
        self.output_dir.mkdir(parents=True, exist_ok=True)
        out = self.output_dir / f"broker_rsc_{code}_{broker_number}.json"
        out.write_text(json.dumps(run, indent=2, ensure_ascii=False), encoding="utf-8")
        return run

    async def fetch_supported_sections(self, page: Page, code: str, broker_number: str) -> dict[str, Any]:
        fetchers = {
            "broker_summary": lambda: self.fetch_broker_summary(page),
            "accumulation_scanner": lambda: self.fetch_accumulation_scanner(page),
            "operator_tracker": lambda: self.fetch_operator_tracker(page),
            "major_broker_trade_share": lambda: self.fetch_major_broker_trade_share(page),
            "broker_wise": lambda: self.fetch_broker_wise(page, code=code, broker_number=broker_number),
            "stock_wise": lambda: self.fetch_stock_wise(page, code=code, broker_number=broker_number),
            "trade_summary": lambda: self.fetch_trade_summary(page, code=code, broker_number=broker_number),
            "broker_holding": lambda: self.fetch_broker_holding(page, code=code),
            "broker_activity_scanner": lambda: self.fetch_broker_activity_scanner(page),
            "broker_smartflow": lambda: self.fetch_broker_smartflow(page),
            "broker_matching_analysis": lambda: self.fetch_broker_matching_analysis(page),
            "datewise_combined_flow": lambda: self.fetch_datewise_combined_flow(page),
        }

        results: dict[str, Any] = {}
        for name, fetcher in fetchers.items():
            try:
                result = await fetcher()
                results[name] = result.parsed
            except Exception as exc:
                results[name] = {
                    "page": name.replace("_", "-"),
                    "error": str(exc),
                }
        return results

    async def fetch_broker_summary(self, page: Page) -> RSCFetchResult:
        url = "https://nepsetrading.com/dashboard/broker-analysis/broker-summary?_rsc=direct"
        text = await self._request_rsc(page, url)
        parsed = {
            "page": "broker-summary",
            "data": _extract_largest_data_array(text),
        }
        return RSCFetchResult(url=url, raw_text=text, parsed=parsed)

    async def fetch_accumulation_scanner(self, page: Page) -> RSCFetchResult:
        url = "https://nepsetrading.com/dashboard/broker-analysis/accumulation-scanner?_rsc=direct"
        text = await self._request_rsc(page, url)
        parsed = {
            "page": "accumulation-scanner",
            "data": _extract_largest_data_array(text),
        }
        return RSCFetchResult(url=url, raw_text=text, parsed=parsed)

    async def fetch_operator_tracker(self, page: Page) -> RSCFetchResult:
        url = "https://nepsetrading.com/dashboard/broker-analysis/operator-tracker?_rsc=direct"
        text = await self._request_rsc(page, url)
        parsed = {
            "page": "operator-tracker",
            "data": _extract_largest_data_array(text),
        }
        return RSCFetchResult(url=url, raw_text=text, parsed=parsed)

    async def fetch_major_broker_trade_share(self, page: Page) -> RSCFetchResult:
        url = "https://nepsetrading.com/dashboard/broker-analysis/major-broker-trade-share?_rsc=direct"
        text = await self._request_rsc(page, url)
        parsed = {
            "page": "major-broker-trade-share",
            "data": _extract_largest_data_array(text),
        }
        return RSCFetchResult(url=url, raw_text=text, parsed=parsed)

    async def fetch_broker_wise(self, page: Page, code: str, broker_number: str) -> RSCFetchResult:
        url = f"https://nepsetrading.com/dashboard/broker-analysis/broker-wise?code={code}&broker_number={broker_number}&_rsc=direct"
        text = await self._request_rsc(page, url)
        parsed = _extract_object_with_marker(text, '{"page":"broker-wise"')
        return RSCFetchResult(url=url, raw_text=text, parsed=parsed)

    async def fetch_stock_wise(self, page: Page, code: str, broker_number: str, view: str = "TreeMap") -> RSCFetchResult:
        url = f"https://nepsetrading.com/dashboard/broker-analysis/stock-wise?view={view}&broker_number={broker_number}&code={code}&_rsc=direct"
        text = await self._request_rsc(page, url)
        parsed = _extract_object_with_marker(text, '{"page":"stock-wise"')
        return RSCFetchResult(url=url, raw_text=text, parsed=parsed)

    async def fetch_trade_summary(self, page: Page, code: str, broker_number: str) -> RSCFetchResult:
        url = f"https://nepsetrading.com/dashboard/broker-analysis/trade-summary?broker_number={broker_number}&code={code}&_rsc=direct"
        text = await self._request_rsc(page, url)
        parsed = {
            "page": "trade-summary",
            "data": _extract_largest_data_array(text),
        }
        return RSCFetchResult(url=url, raw_text=text, parsed=parsed)

    async def fetch_broker_holding(self, page: Page, code: str) -> RSCFetchResult:
        url = f"https://nepsetrading.com/dashboard/broker-analysis/broker-holding?code={code}&_rsc=direct"
        text = await self._request_rsc(page, url)
        datasets = _extract_all_objects_with_marker(text, '{"label":"')
        datasets = [item for item in datasets if "data" in item and isinstance(item["data"], list)]
        parsed = {
            "page": "broker-holding",
            "datasets": _resolve_dataset_references(datasets),
        }
        return RSCFetchResult(url=url, raw_text=text, parsed=parsed)

    async def fetch_broker_activity_scanner(self, page: Page) -> RSCFetchResult:
        url = "https://nepsetrading.com/dashboard/broker-analysis/broker-activity-scanner?_rsc=direct"
        text = await self._request_rsc(page, url)
        return RSCFetchResult(url=url, raw_text=text, parsed={"page": "broker-activity-scanner", "raw": text[:1000]})

    async def fetch_broker_smartflow(self, page: Page) -> RSCFetchResult:
        url = "https://nepsetrading.com/dashboard/broker-analysis/broker-smartflow?_rsc=direct"
        text = await self._request_rsc(page, url)
        return RSCFetchResult(url=url, raw_text=text, parsed={"page": "broker-smartflow", "raw": text[:1000]})

    async def fetch_broker_matching_analysis(self, page: Page) -> RSCFetchResult:
        url = "https://nepsetrading.com/dashboard/broker-analysis/broker-matching-analysis?_rsc=direct"
        text = await self._request_rsc(page, url)
        return RSCFetchResult(url=url, raw_text=text, parsed={"page": "broker-matching-analysis", "raw": text[:1000]})

    async def fetch_datewise_combined_flow(self, page: Page) -> RSCFetchResult:
        url = "https://nepsetrading.com/dashboard/broker-analysis/datewise-combined-flow?_rsc=direct"
        text = await self._request_rsc(page, url)
        return RSCFetchResult(url=url, raw_text=text, parsed={"page": "datewise-combined-flow", "raw": text[:1000]})

    async def _request_rsc(self, page: Page, url: str) -> str:
        response = await page.context.request.get(
            url,
            headers={
                "RSC": "1",
                "Next-Router-State-Tree": '%5B%22%22%2C%7B%7D%2Cnull%2Cnull%2Ctrue%5D',
            },
        )
        if response.status >= 400:
            raise RuntimeError(f"RSC request failed ({response.status}) for {url}")
        return await response.text()

    async def _login(self, page: Page) -> None:
        if not self.email or not self.password:
            raise RuntimeError("Missing NEPSETrading credentials.")
        await page.goto(self.login_url, wait_until="domcontentloaded")
        await page.fill('input[type="email"], input[name="email"]', self.email)
        await page.fill('input[type="password"], input[name="password"]', self.password)
        await page.locator('button[type="submit"]').filter(has_text=re.compile(r"^login$", re.I)).first.click()
        await page.wait_for_url(re.compile(r"/dashboard"))

    def _browser_page(self):
        return _BrowserPageContext(self.headless)


class _BrowserPageContext:
    def __init__(self, headless: bool) -> None:
        self.headless = headless
        self._playwright = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._page: Page | None = None

    async def __aenter__(self) -> Page:
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=self.headless)
        self._context = await self._browser.new_context()
        self._page = await self._context.new_page()
        self._page.set_default_timeout(45000)
        return self._page

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()


async def run_rsc_fetch(code: str = "NABIL", broker_number: str = "1") -> dict[str, Any]:
    fetcher = BrokerAnalysisRSCFetcher()
    return await fetcher.fetch_all(code=code, broker_number=broker_number)


if __name__ == "__main__":
    result = asyncio.run(run_rsc_fetch())
    print(json.dumps(result, indent=2, ensure_ascii=False))
