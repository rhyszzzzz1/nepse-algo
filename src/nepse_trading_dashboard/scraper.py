from __future__ import annotations

import asyncio
import csv
import json
import os
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from playwright.async_api import Browser, BrowserContext, Page, TimeoutError, async_playwright


BROKER_ANALYSIS_SECTIONS: tuple[tuple[str, str], ...] = (
    ("Broker Summary", "broker-summary"),
    ("Broker Activity Scanner", "broker-activity-scanner"),
    ("Broker SmartFlow", "broker-smartflow"),
    ("Accumulation Scanner", "accumulation-scanner"),
    ("Operator Tracker", "operator-tracker"),
    ("Stock Wise", "stock-wise"),
    ("Broker Wise", "broker-wise"),
    ("Broker Holding", "broker-holding"),
    ("Broker Wise Pro", "broker-wise-pro"),
    ("Broker Matching Analysis", "broker-matching-analysis"),
    ("Major Broker Trade Share", "major-broker-trade-share"),
    ("Datewise Combined Flow", "datewise-combined-flow"),
    ("Trade Summary", "trade-summary"),
)

ROOT = Path(__file__).resolve().parents[2]


@dataclass(slots=True)
class SectionCapture:
    name: str
    slug: str
    url: str
    scraped_at: str
    tables: list[dict[str, Any]] = field(default_factory=list)
    cards: list[dict[str, Any]] = field(default_factory=list)
    json_payloads: list[dict[str, Any]] = field(default_factory=list)
    html_path: str | None = None
    screenshot_path: str | None = None
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "slug": self.slug,
            "url": self.url,
            "scraped_at": self.scraped_at,
            "tables": self.tables,
            "cards": self.cards,
            "json_payloads": self.json_payloads,
            "html_path": self.html_path,
            "screenshot_path": self.screenshot_path,
            "notes": self.notes,
        }


@dataclass(slots=True)
class DashboardCapture:
    run_id: str
    started_at: str
    sections: list[SectionCapture] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "started_at": self.started_at,
            "sections": [section.to_dict() for section in self.sections],
        }


class BrokerDashboardScraper:
    def __init__(
        self,
        email: str | None = None,
        password: str | None = None,
        login_url: str | None = None,
        dashboard_url: str | None = None,
        output_dir: str | os.PathLike[str] | None = None,
        headless: bool | None = None,
    ) -> None:
        self.email = email or os.getenv("NEPSE_TRADING_EMAIL")
        self.password = password or os.getenv("NEPSE_TRADING_PASSWORD")
        self.login_url = login_url or os.getenv("NEPSE_TRADING_LOGIN_URL", "https://nepsetrading.com/login")
        self.dashboard_url = dashboard_url or os.getenv("NEPSE_TRADING_DASHBOARD_URL", "https://nepsetrading.com/dashboard")
        self.output_dir = Path(output_dir or os.getenv("NEPSE_TRADING_OUTPUT_DIR", "data/broker_dashboard"))
        if not self.output_dir.is_absolute():
            self.output_dir = ROOT / self.output_dir
        self.headless = headless if headless is not None else os.getenv("NEPSE_TRADING_HEADLESS", "true").strip().lower() not in {"0", "false", "no"}
        self.timeout_ms = 45000
        self.post_click_wait_ms = 2500

    async def scrape(self, section_names: Iterable[str] | None = None) -> DashboardCapture:
        if not self.email or not self.password:
            raise RuntimeError("Missing NEPSETrading credentials.")

        wanted = {item.strip().lower() for item in (section_names or []) if item.strip()}
        sections = [
            (name, slug)
            for name, slug in BROKER_ANALYSIS_SECTIONS
            if not wanted or name.lower() in wanted or slug in wanted
        ]

        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        capture = DashboardCapture(run_id=run_id, started_at=datetime.now(timezone.utc).isoformat())

        async with self._browser_page() as page:
            await self._login(page)
            for name, slug in sections:
                capture.sections.append(await self._capture_section(page, run_id, name, slug))

        self._export_capture(capture)
        return capture

    @asynccontextmanager
    async def _browser_page(self) -> Page:
        async with async_playwright() as playwright:
            browser: Browser = await playwright.chromium.launch(headless=self.headless)
            context: BrowserContext = await browser.new_context()
            page = await context.new_page()
            page.set_default_timeout(self.timeout_ms)
            try:
                yield page
            finally:
                await context.close()
                await browser.close()

    async def _login(self, page: Page) -> None:
        await page.goto(self.login_url, wait_until="domcontentloaded")
        await page.fill('input[type="email"], input[name="email"]', self.email)
        await page.fill('input[type="password"], input[name="password"]', self.password)
        login_button = page.locator('button[type="submit"]').filter(has_text=re.compile(r"^login$", re.I))
        if await login_button.count():
            await login_button.first.click()
        else:
            await page.get_by_role("button", name=re.compile(r"^login$", re.I)).first.click()

        try:
            await page.wait_for_url(re.compile(r"/dashboard"), timeout=self.timeout_ms)
        except TimeoutError as exc:
            await page.wait_for_load_state("networkidle")
            raise RuntimeError(f"Login failed or stayed off-dashboard. Current URL: {page.url}") from exc

    async def _capture_section(self, page: Page, run_id: str, name: str, slug: str) -> SectionCapture:
        target_url = f"{self.dashboard_url}/broker-analysis/{slug}"
        responses: list[dict[str, Any]] = []

        async def on_response(response) -> None:
            content_type = response.headers.get("content-type", "").lower()
            if "application/json" not in content_type:
                return
            try:
                body = await response.json()
            except Exception:
                return
            responses.append({"url": response.url, "status": response.status, "body": body})

        page.on("response", on_response)
        notes: list[str] = []
        try:
            await page.goto(target_url, wait_until="domcontentloaded")
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(self.post_click_wait_ms)

            tables = await page.evaluate(
                """
() => {
  const clean = (text) => (text || '').replace(/\\s+/g, ' ').trim();
  const results = [];
  for (const [index, table] of Array.from(document.querySelectorAll('table')).entries()) {
    let headers = Array.from(table.querySelectorAll('thead th')).map((cell) => clean(cell.textContent));
    if (!headers.length) {
      const firstRow = table.querySelector('tr');
      headers = firstRow ? Array.from(firstRow.querySelectorAll('th,td')).map((cell, i) => clean(cell.textContent) || `column_${i+1}`) : [];
    }
    const bodyRows = Array.from(table.querySelectorAll('tbody tr')).length
      ? Array.from(table.querySelectorAll('tbody tr'))
      : Array.from(table.querySelectorAll('tr')).slice(1);
    const rows = bodyRows.map((row) => {
      const cells = Array.from(row.querySelectorAll('th,td')).map((cell) => clean(cell.textContent));
      const obj = {};
      cells.forEach((value, idx) => {
        obj[headers[idx] || `column_${idx+1}`] = value;
      });
      return obj;
    }).filter((row) => Object.values(row).some(Boolean));
    if (rows.length) results.push({index: index + 1, headers, rows});
  }
  return results;
}
                """
            )
            cards = await page.evaluate(
                """
() => {
  const clean = (text) => (text || '').replace(/\\s+/g, ' ').trim();
  const out = [];
  for (const node of Array.from(document.querySelectorAll('div,section,article')).slice(0, 600)) {
    const texts = Array.from(node.querySelectorAll('h1,h2,h3,h4,h5,h6,p,span,strong'))
      .map((el) => clean(el.textContent))
      .filter(Boolean)
      .slice(0, 4);
    if (texts.length >= 2 && (/[\\d.%]/.test(texts[0]) || /[\\d.%]/.test(texts[1]))) {
      out.push({label: texts[0], value: texts[1]});
    }
    if (out.length >= 50) break;
  }
  return out;
}
                """
            )

            section_dir = self.output_dir / run_id / slug
            section_dir.mkdir(parents=True, exist_ok=True)
            html_path = section_dir / "page.html"
            html_path.write_text(await page.content(), encoding="utf-8")
            screenshot_path = section_dir / "page.png"
            await page.screenshot(path=str(screenshot_path), full_page=True)
            responses_path = section_dir / "responses.json"
            responses_path.write_text(json.dumps(responses, indent=2, ensure_ascii=False), encoding="utf-8")

            if not tables:
                notes.append("No HTML tables found on page.")
            if not responses:
                notes.append("No JSON responses captured after page load.")

            return SectionCapture(
                name=name,
                slug=slug,
                url=page.url,
                scraped_at=datetime.now(timezone.utc).isoformat(),
                tables=tables,
                cards=cards,
                json_payloads=responses,
                html_path=str(html_path),
                screenshot_path=str(screenshot_path),
                notes=notes,
            )
        finally:
            page.remove_listener("response", on_response)

    def _export_capture(self, capture: DashboardCapture) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        json_path = self.output_dir / f"broker_dashboard_{capture.run_id}.json"
        json_path.write_text(json.dumps(capture.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8")

        for section in capture.sections:
            for index, table in enumerate(section.tables, start=1):
                path = self.output_dir / f"{section.slug}_table_{index}_{capture.run_id}.csv"
                headers = list(table.get("headers") or [])
                rows = list(table.get("rows") or [])
                if not headers:
                    for row in rows:
                        for key in row:
                            if key not in headers:
                                headers.append(key)
                with path.open("w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(handle, fieldnames=headers)
                    writer.writeheader()
                    for row in rows:
                        writer.writerow({key: self._stringify(value) for key, value in row.items()})

    @staticmethod
    def _stringify(value: Any) -> str:
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        if value is None:
            return ""
        return str(value)


async def run_dashboard_scrape(section_names: Iterable[str] | None = None) -> DashboardCapture:
    scraper = BrokerDashboardScraper()
    return await scraper.scrape(section_names=section_names)


if __name__ == "__main__":
    result = asyncio.run(run_dashboard_scrape())
    print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
