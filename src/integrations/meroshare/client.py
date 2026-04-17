from __future__ import annotations

import json
import os
from typing import Any

import requests

from .models import (
    HoldingRow,
    LinkedAccount,
    MeroShareCredentials,
    MeroShareSession,
    OwnDetail,
    PurchaseHistoryRow,
    TransactionHistoryRow,
)


def _safe_float(value: Any, default: float | None = 0.0) -> float | None:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class MeroShareClientError(RuntimeError):
    pass


class MeroShareSessionExpired(MeroShareClientError):
    pass


class MeroShareClient:
    def __init__(
        self,
        base_url: str | None = None,
        origin: str | None = None,
        timeout: float = 30.0,
        auth_token: str | None = None,
    ) -> None:
        configured_base = base_url or os.environ.get("MEROSHARE_API_BASE")
        default_bases = ["https://backend.cdsc.com.np/api"]
        self.base_urls = []
        for candidate in ([configured_base] if configured_base else []) + default_bases:
            if not candidate:
                continue
            normalized = candidate.rstrip("/")
            if normalized not in self.base_urls:
                self.base_urls.append(normalized)
        self.base_url = self.base_urls[0]
        self.origin = (origin or os.environ.get("MEROSHARE_ORIGIN") or "https://meroshare.cdsc.com.np").rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/json",
                "Origin": self.origin,
                "Referer": f"{self.origin}/",
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/136.0.0.0 Safari/537.36"
                ),
            }
        )
        if auth_token:
            self.session.headers["Authorization"] = auth_token

    def _url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"

    def _request(self, method: str, path: str, *, expected=(200,), **kwargs) -> Any:
        response = self.session.request(method, self._url(path), timeout=self.timeout, **kwargs)
        if response.status_code in (401, 403):
            raise MeroShareSessionExpired(response.text)
        if response.status_code not in expected:
            raise MeroShareClientError(f"{method} {path} failed via {self.base_url}: {response.status_code} {response.text[:400]}")
        if not response.text:
            return None
        content_type = response.headers.get("content-type", "").lower()
        if "json" in content_type or response.text[:1] in ("{", "["):
            try:
                return response.json()
            except Exception:
                return json.loads(response.text)
        return response.text

    def _extract_table_rows(self, data: Any) -> list[dict[str, Any]]:
        if isinstance(data, dict):
            if isinstance(data.get("object"), list):
                return [row for row in data["object"] if isinstance(row, dict)]
            if isinstance(data.get("transactionView"), list):
                return [row for row in data["transactionView"] if isinstance(row, dict)]
            payload = data.get("data")
            if isinstance(payload, list):
                return [row for row in payload if isinstance(row, dict)]
            if isinstance(payload, dict):
                for key in ("object", "data", "items", "content", "transactionView"):
                    value = payload.get(key)
                    if isinstance(value, list):
                        return [row for row in value if isinstance(row, dict)]
            return []
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
        return []

    def login(self, credentials: MeroShareCredentials) -> MeroShareSession:
        payload = {
            "clientId": int(credentials.client_id),
            "username": str(credentials.username),
            "password": str(credentials.password),
        }
        response = self.session.post(
            self._url("meroShare/auth/"),
            data=json.dumps(payload),
            timeout=self.timeout,
        )
        if response.status_code != 200:
            raise MeroShareClientError(f"Login failed via {self.base_url}: {response.status_code} {response.text[:400]}")
        auth_token = response.headers.get("Authorization") or response.headers.get("authorization")
        if not auth_token:
            raise MeroShareClientError("Login succeeded but no Authorization header was returned.")
        self.session.headers["Authorization"] = auth_token
        return MeroShareSession(auth_token=auth_token)

    def get_depository_participants(self) -> list[dict[str, Any]]:
        data = self._request("GET", "meroShare/capital/")
        rows = data if isinstance(data, list) else []
        return [
            {
                "id": row.get("id"),
                "code": row.get("code"),
                "name": row.get("name"),
            }
            for row in rows
            if row.get("id") and row.get("name")
        ]

    def get_own_detail(self) -> OwnDetail:
        data = self._request("GET", "meroShare/ownDetail/")
        return OwnDetail(
            customer_id=data.get("id"),
            name=data.get("name"),
            demat=data.get("demat"),
            client_code=data.get("clientCode"),
            dp_name=data.get("dpName"),
            boid=data.get("boid") or data.get("demat"),
            raw=data,
        )

    def get_linked_accounts(self, customer_id: int) -> list[LinkedAccount]:
        data = self._request("GET", f"meroShare/dependentCustomer/{customer_id}")
        linked_rows = data or []
        accounts: list[LinkedAccount] = []
        for row in linked_rows:
            kyc = row.get("kycDetail") or {}
            accounts.append(
                LinkedAccount(
                    customer_id=row.get("id"),
                    boid=kyc.get("demat"),
                    name=kyc.get("name"),
                    raw=row,
                )
            )
        return accounts

    def get_wacc_necessities(self) -> dict[str, Any]:
        data = self._request("GET", "myHoldings/wacc/")
        if isinstance(data, dict):
            return data
        return {"data": {"waccResponses": [], "disclaimer": None}}

    def get_holdings_summary(self, boid: str | None = None, demat: str | None = None, isin: str | None = None) -> dict[str, Any]:
        payload = {}
        if boid:
            payload["boid"] = boid
        if demat:
            payload["demat"] = demat
        if isin:
            payload["isin"] = isin
        data = self._request("POST", "myHoldings/summary/", data=json.dumps(payload), expected=(200, 201))
        return data or {}

    def get_holdings(
        self,
        *,
        boid: str | None = None,
        demat: str | None = None,
        isin: str | None = None,
        page: int = 1,
        size: int = 500,
    ) -> list[HoldingRow]:
        payload = {"page": page, "size": size}
        if boid:
            payload["boid"] = boid
        if demat:
            payload["demat"] = demat
        if isin:
            payload["isin"] = isin
        data = self._request("POST", "myHoldings/", data=json.dumps(payload), expected=(200, 201))
        rows = data.get("object") if isinstance(data, dict) and "object" in data else data
        result: list[HoldingRow] = []
        for row in rows or []:
            symbol = row.get("script") or row.get("symbol") or row.get("scrip")
            result.append(
                HoldingRow(
                    symbol=str(symbol or "").upper(),
                    security_name=row.get("companyName") or row.get("securityName") or row.get("name"),
                    quantity=_safe_float(row.get("quantity"), 0.0) or 0.0,
                    wacc=_safe_float(row.get("wacc"), None),
                    market_rate=_safe_float(row.get("rate"), None),
                    market_value=_safe_float(row.get("marketValue"), None),
                    raw=row,
                )
            )
        return result

    def get_portfolio_view(
        self,
        *,
        demat: list[str],
        client_code: str,
        page: int = 1,
        size: int = 500,
        sort_by: str = "script",
        sort_asc: bool = True,
        script: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "demat": demat,
            "clientCode": str(client_code),
            "page": int(page),
            "size": int(size),
            "sortBy": sort_by,
            "sortAsc": bool(sort_asc),
        }
        if script:
            payload["script"] = script.upper()
        data = self._request("POST", "meroShareView/myPortfolio/", data=json.dumps(payload), expected=(200, 201))
        return data if isinstance(data, dict) else {}

    def get_portfolio_holdings(
        self,
        *,
        demat: list[str],
        client_code: str,
        page: int = 1,
        size: int = 500,
        sort_by: str = "script",
        sort_asc: bool = True,
        script: str | None = None,
    ) -> list[HoldingRow]:
        data = self.get_portfolio_view(
            demat=demat,
            client_code=client_code,
            page=page,
            size=size,
            sort_by=sort_by,
            sort_asc=sort_asc,
            script=script,
        )
        rows = data.get("meroShareMyPortfolio") or []
        holdings: list[HoldingRow] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            symbol = row.get("script") or row.get("symbol") or row.get("scrip")
            holdings.append(
                HoldingRow(
                    symbol=str(symbol or "").upper(),
                    security_name=row.get("scriptDesc") or row.get("companyName") or row.get("securityName") or row.get("name"),
                    quantity=_safe_float(row.get("currentBalance"), 0.0) or 0.0,
                    wacc=None,
                    market_rate=_safe_float(row.get("lastTransactionPrice"), None),
                    market_value=_safe_float(
                        row.get("valueOfLastTransPrice"),
                        _safe_float(row.get("valueAsOfLastTransactionPrice"), None),
                    ),
                    raw=row,
                )
            )
        return holdings

    def get_purchase_history(
        self,
        *,
        boid: str | None = None,
        script: str | None = None,
        page: int = 1,
        size: int = 500,
    ) -> list[PurchaseHistoryRow]:
        payload: dict[str, Any] = {"page": page, "size": size}
        if boid:
            payload["boid"] = boid
        if script:
            payload["script"] = script.upper()
        data = self._request("POST", "myPurchase/purchaseHistory/", data=json.dumps(payload), expected=(200, 201))
        rows = self._extract_table_rows(data)
        results: list[PurchaseHistoryRow] = []
        for row in rows or []:
            symbol = row.get("script") or row.get("symbol") or row.get("scrip")
            qty = row.get("quantity") or row.get("qty") or row.get("kitta")
            rate = row.get("rate") or row.get("price")
            amount = row.get("amount") or row.get("totalAmount")
            results.append(
                PurchaseHistoryRow(
                    symbol=str(symbol or "").upper(),
                    transaction_date=row.get("transactionDate") or row.get("date"),
                    quantity=_safe_float(qty, 0.0) or 0.0,
                    rate=_safe_float(rate, None),
                    amount=_safe_float(amount, None),
                    wacc=_safe_float(row.get("wacc"), None),
                    transaction_type=row.get("transactionType") or row.get("type"),
                    raw=row,
                )
            )
        return results

    def get_wacc_summary_report(self, *, boid: str | None = None, page: int = 1, size: int = 500) -> list[PurchaseHistoryRow]:
        payload: dict[str, Any] = {"page": page, "size": size}
        if boid:
            payload["boid"] = boid
        data = self._request("POST", "myPurchase/waccSummaryReport/", data=json.dumps(payload), expected=(200, 201))
        rows = self._extract_table_rows(data)
        results: list[PurchaseHistoryRow] = []
        for row in rows or []:
            symbol = row.get("script") or row.get("symbol") or row.get("scrip")
            qty = row.get("currentQuantity") or row.get("quantity") or row.get("qty")
            rate = row.get("wacc") or row.get("rate")
            amount = row.get("marketValue") or row.get("amount")
            results.append(
                PurchaseHistoryRow(
                    symbol=str(symbol or "").upper(),
                    transaction_date=row.get("transactionDate") or row.get("date"),
                    quantity=_safe_float(qty, 0.0) or 0.0,
                    rate=_safe_float(rate, None),
                    amount=_safe_float(amount, None),
                    wacc=_safe_float(row.get("wacc"), None),
                    transaction_type="WACC",
                    raw=row,
                )
            )
        return results

    def get_wacc_report_via_browser(
        self,
        *,
        demat: str,
        boid: str | None = None,
        client_code: str | None = None,
        page: int = 1,
        size: int = 500,
    ) -> list[PurchaseHistoryRow]:
        from playwright.sync_api import sync_playwright

        auth_token = self.session.headers.get("Authorization")
        if not auth_token:
            raise MeroShareClientError("No active MeroShare authorization token is available for browser WACC fetch.")

        payload: dict[str, Any] = {
            "demat": str(demat),
            "page": int(page),
            "size": int(size),
        }
        if boid:
            payload["boid"] = str(boid)
        if client_code:
            payload["clientCode"] = str(client_code)

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page_obj = browser.new_page()
            try:
                page_obj.goto(f"{self.origin}/", wait_until="domcontentloaded", timeout=int(self.timeout * 1000))
                result = page_obj.evaluate(
                    """
                    async ({ url, payload, authorization }) => {
                      const response = await fetch(url, {
                        method: "POST",
                        headers: {
                          "Accept": "application/json, text/plain, */*",
                          "Content-Type": "application/json",
                          "Authorization": authorization,
                        },
                        body: JSON.stringify(payload),
                      });
                      const text = await response.text();
                      return {
                        status: response.status,
                        contentType: response.headers.get("content-type") || "",
                        text,
                      };
                    }
                    """,
                    {
                        "url": "https://webbackend.cdsc.com.np/api/myPurchase/waccReport/",
                        "payload": payload,
                        "authorization": auth_token,
                    },
                )
            finally:
                browser.close()

        status = int(result.get("status") or 0)
        text = result.get("text") or ""
        content_type = str(result.get("contentType") or "").lower()
        if status not in (200, 201):
            raise MeroShareClientError(f"Browser WACC fetch failed: {status} {text[:400]}")
        if "json" not in content_type and not text[:1] in ("{", "["):
            raise MeroShareClientError(f"Browser WACC fetch returned non-JSON payload: {text[:400]}")
        try:
            data = json.loads(text)
        except Exception as exc:
            raise MeroShareClientError(f"Browser WACC fetch returned invalid JSON: {exc}") from exc

        rows = []
        if isinstance(data, dict):
            maybe_rows = data.get("waccReportResponse")
            if isinstance(maybe_rows, list):
                rows = [row for row in maybe_rows if isinstance(row, dict)]
        elif isinstance(data, list):
            rows = [row for row in data if isinstance(row, dict)]

        results: list[PurchaseHistoryRow] = []
        for row in rows:
            symbol = row.get("scrip") or row.get("script") or row.get("symbol")
            qty = row.get("totalQuantity") or row.get("currentQuantity") or row.get("quantity") or row.get("qty")
            wacc = row.get("averageBuyRate") or row.get("wacc") or row.get("rate")
            amount = row.get("totalCost") or row.get("marketValue") or row.get("amount")
            results.append(
                PurchaseHistoryRow(
                    symbol=str(symbol or "").upper(),
                    transaction_date=row.get("lastModifiedDate") or row.get("transactionDate") or row.get("date"),
                    quantity=_safe_float(qty, 0.0) or 0.0,
                    rate=_safe_float(wacc, None),
                    amount=_safe_float(amount, None),
                    wacc=_safe_float(wacc, None),
                    transaction_type="WACC_BROWSER",
                    raw=row,
                )
            )
        return results

    def get_transaction_history(
        self,
        *,
        boid: str,
        client_code: str,
        script: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
        page: int = 1,
        size: int = 500,
    ) -> list[TransactionHistoryRow]:
        payload: dict[str, Any] = {
            "boid": boid,
            "clientCode": client_code,
            "requestTypeScript": bool(script),
            "page": page,
            "size": size,
        }
        if script:
            payload["script"] = script.upper()
        if from_date:
            payload["fromDate"] = from_date
        if to_date:
            payload["toDate"] = to_date
        data = self._request("POST", "meroShareView/myTransaction/", data=json.dumps(payload), expected=(200, 201))
        rows = self._extract_table_rows(data)
        results: list[TransactionHistoryRow] = []
        for row in rows or []:
            symbol = row.get("script") or row.get("symbol") or row.get("scrip")
            qty = row.get("quantity") or row.get("qty")
            rate = row.get("rate") or row.get("price")
            amount = row.get("amount") or row.get("totalAmount")
            results.append(
                TransactionHistoryRow(
                    symbol=str(symbol or "").upper() if symbol else None,
                    transaction_date=row.get("transactionDate") or row.get("date"),
                    quantity=_safe_float(qty, None),
                    rate=_safe_float(rate, None),
                    amount=_safe_float(amount, None),
                    transaction_type=row.get("transactionType") or row.get("type"),
                    boid=row.get("boid") or boid,
                    client_code=row.get("clientCode") or client_code,
                    raw=row,
                )
            )
        return results
