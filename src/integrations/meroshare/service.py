from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any

from .client import MeroShareClient, MeroShareClientError, MeroShareSessionExpired
from .models import HoldingRow, MeroShareCredentials


def _utcnow() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def ensure_meroshare_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS meroshare_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            label TEXT,
            client_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            account_holder_name TEXT,
            boid TEXT,
            demat TEXT,
            client_code TEXT,
            customer_id INTEGER,
            dp_name TEXT,
            raw_profile_json TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(client_id, username)
        );

        CREATE TABLE IF NOT EXISTS meroshare_sessions (
            account_id INTEGER PRIMARY KEY,
            auth_token TEXT,
            status TEXT NOT NULL DEFAULT 'ACTIVE',
            last_login_at TEXT,
            expires_at TEXT,
            FOREIGN KEY (account_id) REFERENCES meroshare_accounts(id)
        );

        CREATE TABLE IF NOT EXISTS meroshare_linked_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER NOT NULL,
            linked_customer_id INTEGER,
            linked_boid TEXT,
            linked_name TEXT,
            raw_json TEXT,
            synced_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (account_id) REFERENCES meroshare_accounts(id)
        );

        CREATE TABLE IF NOT EXISTS meroshare_holdings_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER NOT NULL,
            snapshot_at TEXT NOT NULL,
            symbol TEXT NOT NULL,
            security_name TEXT,
            quantity REAL NOT NULL DEFAULT 0,
            wacc REAL,
            market_rate REAL,
            market_value REAL,
            source_payload_json TEXT,
            FOREIGN KEY (account_id) REFERENCES meroshare_accounts(id)
        );

        CREATE INDEX IF NOT EXISTS idx_meroshare_holdings_account_snapshot
            ON meroshare_holdings_snapshots(account_id, snapshot_at DESC, symbol);

        CREATE TABLE IF NOT EXISTS meroshare_purchase_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER NOT NULL,
            transaction_date TEXT,
            symbol TEXT NOT NULL,
            quantity REAL NOT NULL DEFAULT 0,
            rate REAL,
            amount REAL,
            wacc REAL,
            transaction_type TEXT,
            raw_json TEXT,
            synced_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (account_id) REFERENCES meroshare_accounts(id)
        );

        CREATE INDEX IF NOT EXISTS idx_meroshare_purchase_account_symbol
            ON meroshare_purchase_history(account_id, symbol, transaction_date DESC);

        CREATE TABLE IF NOT EXISTS meroshare_transaction_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER NOT NULL,
            transaction_date TEXT,
            transaction_type TEXT,
            symbol TEXT,
            quantity REAL,
            rate REAL,
            amount REAL,
            boid TEXT,
            client_code TEXT,
            raw_json TEXT,
            synced_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (account_id) REFERENCES meroshare_accounts(id)
        );

        CREATE INDEX IF NOT EXISTS idx_meroshare_transaction_account_date
            ON meroshare_transaction_history(account_id, transaction_date DESC);

        CREATE TABLE IF NOT EXISTS meroshare_sync_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER NOT NULL,
            sync_type TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL,
            row_count INTEGER NOT NULL DEFAULT 0,
            error_message TEXT,
            FOREIGN KEY (account_id) REFERENCES meroshare_accounts(id)
        );

        CREATE TABLE IF NOT EXISTS portfolio_position_sources (
            portfolio_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            source_type TEXT NOT NULL,
            source_account_id INTEGER,
            source_snapshot_id INTEGER,
            imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (portfolio_id, symbol, source_type),
            FOREIGN KEY (portfolio_id) REFERENCES portfolios(id),
            FOREIGN KEY (source_account_id) REFERENCES meroshare_accounts(id)
        );
        """
    )
    conn.commit()


def _row_to_dict(row):
    return dict(row) if row else None


def _extract_isin(item: Any) -> str | None:
    if isinstance(item, dict):
        value = item.get("isin") or item.get("script") or item.get("scrip")
        return str(value).strip() if value not in (None, "") else None
    if isinstance(item, str):
        value = item.strip()
        return value or None
    return None


def _normalize_boid(boid: str | None, demat: str | None) -> str | None:
    boid_value = str(boid).strip() if boid not in (None, "") else None
    demat_value = str(demat).strip() if demat not in (None, "") else None
    if demat_value and (not boid_value or len(boid_value) < len(demat_value)):
        return demat_value
    return boid_value or demat_value


def _fetch_account_row(conn: sqlite3.Connection, account_id: int):
    return conn.execute(
        """
        SELECT a.*, s.auth_token, s.status AS session_status, s.last_login_at, s.expires_at
        FROM meroshare_accounts a
        LEFT JOIN meroshare_sessions s
          ON s.account_id = a.id
        WHERE a.id = ?
        """,
        (account_id,),
    ).fetchone()


def _record_sync_run(
    conn: sqlite3.Connection,
    account_id: int,
    sync_type: str,
    status: str,
    row_count: int = 0,
    error_message: str | None = None,
    started_at: str | None = None,
) -> None:
    started = started_at or _utcnow()
    conn.execute(
        """
        INSERT INTO meroshare_sync_runs(account_id, sync_type, started_at, finished_at, status, row_count, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (account_id, sync_type, started, _utcnow(), status, int(row_count), error_message),
    )


def _merge_holding_row(
    base: HoldingRow | None,
    incoming: HoldingRow,
    *,
    prefer_quantity: bool = False,
    prefer_market_values: bool = False,
) -> HoldingRow:
    if base is None:
        return incoming
    quantity = incoming.quantity if prefer_quantity and incoming.quantity not in (None, 0) else base.quantity
    if quantity in (None, 0) and incoming.quantity not in (None, 0):
        quantity = incoming.quantity
    wacc = incoming.wacc if incoming.wacc not in (None, 0) else base.wacc
    market_rate = incoming.market_rate if prefer_market_values and incoming.market_rate not in (None, 0) else base.market_rate
    market_value = incoming.market_value if prefer_market_values and incoming.market_value not in (None, 0) else base.market_value
    if market_rate in (None, 0) and incoming.market_rate not in (None, 0):
        market_rate = incoming.market_rate
    if market_value in (None, 0) and incoming.market_value not in (None, 0):
        market_value = incoming.market_value
    security_name = incoming.security_name or base.security_name
    raw = dict(base.raw or {})
    if incoming.raw:
        raw.update(incoming.raw)
    return HoldingRow(
        symbol=base.symbol or incoming.symbol,
        security_name=security_name,
        quantity=quantity or 0.0,
        wacc=wacc,
        market_rate=market_rate,
        market_value=market_value,
        raw=raw,
    )


def _build_wacc_holding(row: PurchaseHistoryRow) -> HoldingRow:
    raw = row.raw or {}
    return HoldingRow(
        symbol=row.symbol,
        security_name=raw.get("companyName") or raw.get("securityName") or raw.get("name") or row.symbol,
        quantity=row.quantity,
        wacc=row.wacc,
        market_rate=row.rate or raw.get("rate"),
        market_value=row.amount,
        raw=raw,
    )


def list_meroshare_accounts(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    ensure_meroshare_tables(conn)
    rows = conn.execute(
        """
        SELECT
            a.*,
            s.status AS session_status,
            s.last_login_at,
            (SELECT MAX(snapshot_at) FROM meroshare_holdings_snapshots hs WHERE hs.account_id = a.id) AS latest_holdings_snapshot,
            (SELECT COUNT(*) FROM meroshare_holdings_snapshots hs WHERE hs.account_id = a.id AND hs.snapshot_at = (
                SELECT MAX(snapshot_at) FROM meroshare_holdings_snapshots hs2 WHERE hs2.account_id = a.id
            )) AS latest_holdings_count
        FROM meroshare_accounts a
        LEFT JOIN meroshare_sessions s
          ON s.account_id = a.id
        WHERE a.is_active = 1
        ORDER BY a.updated_at DESC, a.id DESC
        """
    ).fetchall()
    return [_row_to_dict(row) for row in rows]


def get_meroshare_account(conn: sqlite3.Connection, account_id: int) -> dict[str, Any] | None:
    ensure_meroshare_tables(conn)
    row = _fetch_account_row(conn, account_id)
    if not row:
        return None
    base = _row_to_dict(row)
    base["linked_accounts"] = [
        _row_to_dict(item)
        for item in conn.execute(
            """
            SELECT linked_customer_id, linked_boid, linked_name, synced_at
            FROM meroshare_linked_accounts
            WHERE account_id = ?
            ORDER BY id ASC
            """,
            (account_id,),
        ).fetchall()
    ]
    return base


def connect_meroshare_account(
    conn: sqlite3.Connection,
    *,
    client_id: int,
    username: str,
    password: str,
    label: str | None = None,
) -> dict[str, Any]:
    ensure_meroshare_tables(conn)
    client = MeroShareClient()
    session = client.login(MeroShareCredentials(client_id=client_id, username=username, password=password))
    own_detail = client.get_own_detail()
    normalized_boid = _normalize_boid(own_detail.boid, own_detail.demat)
    linked_accounts = client.get_linked_accounts(own_detail.customer_id) if own_detail.customer_id else []
    now = _utcnow()

    existing = conn.execute(
        "SELECT id FROM meroshare_accounts WHERE client_id = ? AND username = ?",
        (int(client_id), str(username)),
    ).fetchone()
    if existing:
        account_id = int(existing["id"])
        conn.execute(
            """
            UPDATE meroshare_accounts
            SET label = ?, account_holder_name = ?, boid = ?, demat = ?, client_code = ?, customer_id = ?, dp_name = ?,
                raw_profile_json = ?, updated_at = ?, is_active = 1
            WHERE id = ?
            """,
            (
                label or own_detail.name or username,
                own_detail.name,
                normalized_boid,
                own_detail.demat,
                own_detail.client_code,
                own_detail.customer_id,
                own_detail.dp_name,
                json.dumps(own_detail.raw),
                now,
                account_id,
            ),
        )
    else:
        cur = conn.execute(
            """
            INSERT INTO meroshare_accounts(
                label, client_id, username, account_holder_name, boid, demat, client_code, customer_id, dp_name, raw_profile_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                label or own_detail.name or username,
                int(client_id),
                str(username),
                own_detail.name,
                normalized_boid,
                own_detail.demat,
                own_detail.client_code,
                own_detail.customer_id,
                own_detail.dp_name,
                json.dumps(own_detail.raw),
                now,
                now,
            ),
        )
        account_id = int(cur.lastrowid)

    conn.execute(
        """
        INSERT INTO meroshare_sessions(account_id, auth_token, status, last_login_at, expires_at)
        VALUES (?, ?, 'ACTIVE', ?, NULL)
        ON CONFLICT(account_id) DO UPDATE SET
            auth_token=excluded.auth_token,
            status='ACTIVE',
            last_login_at=excluded.last_login_at,
            expires_at=excluded.expires_at
        """,
        (account_id, session.auth_token, now),
    )
    conn.execute("DELETE FROM meroshare_linked_accounts WHERE account_id = ?", (account_id,))
    for linked in linked_accounts:
        conn.execute(
            """
            INSERT INTO meroshare_linked_accounts(account_id, linked_customer_id, linked_boid, linked_name, raw_json, synced_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (account_id, linked.customer_id, linked.boid, linked.name, json.dumps(linked.raw), now),
        )
    conn.commit()
    return get_meroshare_account(conn, account_id) or {}


def _build_client_from_account(conn: sqlite3.Connection, account_id: int, password: str | None = None) -> tuple[MeroShareClient, dict[str, Any]]:
    row = _fetch_account_row(conn, account_id)
    if not row:
        raise MeroShareClientError("MeroShare account not found.")
    account = _row_to_dict(row)
    token = account.get("auth_token")
    if token:
        return MeroShareClient(auth_token=token), account
    if not password:
        raise MeroShareClientError("This account has no active MeroShare session. Provide password to refresh the session.")
    refreshed = connect_meroshare_account(
        conn,
        client_id=int(account["client_id"]),
        username=str(account["username"]),
        password=password,
        label=account.get("label"),
    )
    return MeroShareClient(auth_token=refreshed.get("auth_token")), refreshed


def sync_meroshare_account(
    conn: sqlite3.Connection,
    account_id: int,
    *,
    password: str | None = None,
    include_transactions: bool = True,
) -> dict[str, Any]:
    ensure_meroshare_tables(conn)
    started_at = _utcnow()
    try:
        client, _account = _build_client_from_account(conn, account_id, password=password)
        own_detail = client.get_own_detail()
    except MeroShareSessionExpired:
        if not password:
            _record_sync_run(conn, account_id, "full_sync", "FAILED", error_message="Session expired and password was not provided.", started_at=started_at)
            conn.commit()
            raise
        account_row = _fetch_account_row(conn, account_id)
        account = _row_to_dict(account_row)
        connect_meroshare_account(
            conn,
            client_id=int(account["client_id"]),
            username=str(account["username"]),
            password=password,
            label=account.get("label"),
        )
        client, _account = _build_client_from_account(conn, account_id, password=None)
        own_detail = client.get_own_detail()

    linked_accounts = client.get_linked_accounts(own_detail.customer_id) if own_detail.customer_id else []
    normalized_boid = _normalize_boid(own_detail.boid, own_detail.demat)
    snapshot_at = _utcnow()
    summary: dict[str, Any] = {"source": "portfolio_view"}
    holdings: list[HoldingRow] = []
    holdings_by_symbol: dict[str, HoldingRow] = {}
    base_holding_symbols: set[str] = set()

    portfolio_payload: dict[str, Any] = {}
    if own_detail.demat and own_detail.client_code:
        try:
            portfolio_payload = client.get_portfolio_view(
                demat=[str(own_detail.demat)],
                client_code=str(own_detail.client_code),
                page=1,
                size=500,
                sort_by="script",
                sort_asc=True,
            )
            holdings = client.get_portfolio_holdings(
                demat=[str(own_detail.demat)],
                client_code=str(own_detail.client_code),
                page=1,
                size=500,
                sort_by="script",
                sort_asc=True,
            )
            summary.update(
                {
                    "total_items": portfolio_payload.get("totalItems"),
                    "total_value_last_transaction": portfolio_payload.get("totalValueOfLastTransPrice")
                    or portfolio_payload.get("totalValueAsOfLastTransactionPrice"),
                    "total_value_previous_close": portfolio_payload.get("totalValueOfPrevClosingPrice")
                    or portfolio_payload.get("totalValueAsOfPreviousClosingPrice"),
                }
            )
        except MeroShareClientError as exc:
            summary.update({"portfolio_view_error": str(exc)})
        else:
            for holding in holdings:
                if holding.symbol:
                    holdings_by_symbol[holding.symbol] = holding
                    base_holding_symbols.add(holding.symbol)

    direct_holdings: list[HoldingRow] = []
    if normalized_boid or own_detail.demat:
        try:
            direct_holdings = client.get_holdings(
                boid=normalized_boid,
                demat=own_detail.demat,
                page=1,
                size=500,
            )
        except MeroShareClientError as exc:
            summary.update({"my_holdings_error": str(exc)})
        else:
            for holding in direct_holdings:
                if not holding.symbol:
                    continue
                base_holding_symbols.add(holding.symbol)
                holdings_by_symbol[holding.symbol] = _merge_holding_row(
                    holdings_by_symbol.get(holding.symbol),
                    holding,
                    prefer_quantity=True,
                    prefer_market_values=True,
                )

    wacc_rows = []
    if normalized_boid:
        try:
            wacc_rows = client.get_wacc_summary_report(boid=normalized_boid, page=1, size=500)
        except MeroShareClientError:
            wacc_rows = []
    if not wacc_rows and own_detail.demat:
        try:
            wacc_rows = client.get_wacc_report_via_browser(
                demat=own_detail.demat,
                boid=normalized_boid,
                client_code=own_detail.client_code,
                page=1,
                size=500,
            )
            if wacc_rows:
                summary["wacc_source"] = "browser_fallback"
        except MeroShareClientError as exc:
            summary["wacc_browser_error"] = str(exc)
    elif wacc_rows:
        summary["wacc_source"] = "direct_api"

    purchase_history_rows = []
    if normalized_boid:
        try:
            purchase_history_rows = client.get_purchase_history(boid=normalized_boid, page=1, size=500)
        except MeroShareClientError:
            purchase_history_rows = []

    if wacc_rows:
        for row in wacc_rows:
            if not row.symbol:
                continue
            if base_holding_symbols and row.symbol not in base_holding_symbols:
                continue
            holdings_by_symbol[row.symbol] = _merge_holding_row(
                holdings_by_symbol.get(row.symbol),
                _build_wacc_holding(row),
                prefer_quantity=False,
                prefer_market_values=False,
            )
    if not holdings_by_symbol and wacc_rows:
        summary["source"] = "wacc_summary_fallback"
        summary["fallback_reason"] = "portfolio view returned no holdings for the account."

    if purchase_history_rows:
        latest_purchase_by_symbol: dict[str, PurchaseHistoryRow] = {}
        for row in purchase_history_rows:
            if not row.symbol:
                continue
            existing = latest_purchase_by_symbol.get(row.symbol)
            if existing is None:
                latest_purchase_by_symbol[row.symbol] = row
                continue
            existing_date = existing.transaction_date or ""
            incoming_date = row.transaction_date or ""
            if incoming_date >= existing_date:
                latest_purchase_by_symbol[row.symbol] = row
        for symbol, row in latest_purchase_by_symbol.items():
            if base_holding_symbols and symbol not in base_holding_symbols:
                continue
            raw = row.raw or {}
            purchase_holding = HoldingRow(
                symbol=symbol,
                security_name=raw.get("companyName") or raw.get("securityName") or raw.get("name") or symbol,
                quantity=row.quantity,
                wacc=row.wacc or row.rate,
                market_rate=None,
                market_value=None,
                raw=raw,
            )
            holdings_by_symbol[symbol] = _merge_holding_row(
                holdings_by_symbol.get(symbol),
                purchase_holding,
                prefer_quantity=False,
                prefer_market_values=False,
            )

    holdings = sorted(holdings_by_symbol.values(), key=lambda item: ((item.market_value or 0), item.quantity, item.symbol), reverse=True)
    transactions = []
    if include_transactions and normalized_boid and own_detail.client_code:
        try:
            transactions = client.get_transaction_history(boid=normalized_boid, client_code=own_detail.client_code, page=1, size=500)
        except MeroShareClientError:
            transactions = []

    conn.execute(
        """
        UPDATE meroshare_accounts
        SET account_holder_name = ?, boid = ?, demat = ?, client_code = ?, customer_id = ?, dp_name = ?, raw_profile_json = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            own_detail.name,
            normalized_boid,
            own_detail.demat,
            own_detail.client_code,
            own_detail.customer_id,
            own_detail.dp_name,
            json.dumps(own_detail.raw),
            snapshot_at,
            account_id,
        ),
    )
    conn.execute("DELETE FROM meroshare_linked_accounts WHERE account_id = ?", (account_id,))
    for linked in linked_accounts:
        conn.execute(
            """
            INSERT INTO meroshare_linked_accounts(account_id, linked_customer_id, linked_boid, linked_name, raw_json, synced_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (account_id, linked.customer_id, linked.boid, linked.name, json.dumps(linked.raw), snapshot_at),
        )
    conn.execute("DELETE FROM meroshare_holdings_snapshots WHERE account_id = ? AND snapshot_at = ?", (account_id, snapshot_at))
    for holding in holdings:
        conn.execute(
            """
            INSERT INTO meroshare_holdings_snapshots(
                account_id, snapshot_at, symbol, security_name, quantity, wacc, market_rate, market_value, source_payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                account_id,
                snapshot_at,
                holding.symbol,
                holding.security_name,
                holding.quantity,
                holding.wacc,
                holding.market_rate,
                holding.market_value,
                json.dumps(holding.raw),
            ),
        )

    purchase_rows_to_store = []
    if wacc_rows:
        purchase_rows_to_store.extend(wacc_rows)
    if purchase_history_rows:
        purchase_rows_to_store.extend(purchase_history_rows)
    if purchase_rows_to_store:
        conn.execute("DELETE FROM meroshare_purchase_history WHERE account_id = ?", (account_id,))
        for row in purchase_rows_to_store:
            conn.execute(
                """
                INSERT INTO meroshare_purchase_history(
                    account_id, transaction_date, symbol, quantity, rate, amount, wacc, transaction_type, raw_json, synced_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    account_id,
                    row.transaction_date,
                    row.symbol,
                    row.quantity,
                    row.rate,
                    row.amount,
                    row.wacc,
                    row.transaction_type,
                    json.dumps(row.raw),
                    snapshot_at,
                ),
            )

    if transactions:
        conn.execute("DELETE FROM meroshare_transaction_history WHERE account_id = ?", (account_id,))
        for row in transactions:
            conn.execute(
                """
                INSERT INTO meroshare_transaction_history(
                    account_id, transaction_date, transaction_type, symbol, quantity, rate, amount, boid, client_code, raw_json, synced_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    account_id,
                    row.transaction_date,
                    row.transaction_type,
                    row.symbol,
                    row.quantity,
                    row.rate,
                    row.amount,
                    row.boid,
                    row.client_code,
                    json.dumps(row.raw),
                    snapshot_at,
                ),
            )

    _record_sync_run(conn, account_id, "full_sync", "SUCCESS", row_count=len(holdings), started_at=started_at)
    conn.commit()
    return {
        "account": get_meroshare_account(conn, account_id),
        "summary": summary,
        "snapshot_at": snapshot_at,
        "holdings_count": len(holdings),
        "purchase_rows": len(purchase_rows_to_store),
        "transaction_rows": len(transactions),
    }


def get_meroshare_holdings(conn: sqlite3.Connection, account_id: int, latest_only: bool = True) -> dict[str, Any]:
    ensure_meroshare_tables(conn)
    if latest_only:
        snapshot_row = conn.execute(
            "SELECT MAX(snapshot_at) AS snapshot_at FROM meroshare_holdings_snapshots WHERE account_id = ?",
            (account_id,),
        ).fetchone()
        snapshot_at = snapshot_row["snapshot_at"] if snapshot_row else None
        if not snapshot_at:
            return {"account_id": account_id, "snapshot_at": None, "holdings": [], "count": 0}
        rows = conn.execute(
            """
            SELECT *
            FROM meroshare_holdings_snapshots
            WHERE account_id = ? AND snapshot_at = ?
            ORDER BY market_value DESC, quantity DESC, symbol ASC
            """,
            (account_id, snapshot_at),
        ).fetchall()
        return {
            "account_id": account_id,
            "snapshot_at": snapshot_at,
            "holdings": [_row_to_dict(row) for row in rows],
            "count": len(rows),
        }
    rows = conn.execute(
        """
        SELECT *
        FROM meroshare_holdings_snapshots
        WHERE account_id = ?
        ORDER BY snapshot_at DESC, market_value DESC, symbol ASC
        """,
        (account_id,),
    ).fetchall()
    return {"account_id": account_id, "holdings": [_row_to_dict(row) for row in rows], "count": len(rows)}


def get_meroshare_purchase_history(conn: sqlite3.Connection, account_id: int) -> dict[str, Any]:
    ensure_meroshare_tables(conn)
    rows = conn.execute(
        """
        SELECT *
        FROM meroshare_purchase_history
        WHERE account_id = ?
        ORDER BY transaction_date DESC, id DESC
        """,
        (account_id,),
    ).fetchall()
    return {"account_id": account_id, "rows": [_row_to_dict(row) for row in rows], "count": len(rows)}


def get_meroshare_transactions(conn: sqlite3.Connection, account_id: int) -> dict[str, Any]:
    ensure_meroshare_tables(conn)
    rows = conn.execute(
        """
        SELECT *
        FROM meroshare_transaction_history
        WHERE account_id = ?
        ORDER BY transaction_date DESC, id DESC
        """,
        (account_id,),
    ).fetchall()
    return {"account_id": account_id, "rows": [_row_to_dict(row) for row in rows], "count": len(rows)}


def import_meroshare_holdings_to_portfolio(conn: sqlite3.Connection, portfolio_id: int, account_id: int) -> dict[str, Any]:
    ensure_meroshare_tables(conn)

    def resolve_import_metrics(symbol: str, snapshot_row: sqlite3.Row | None = None) -> tuple[float, str, str | None]:
        if snapshot_row is not None and snapshot_row["wacc"] not in (None, ""):
            return float(snapshot_row["wacc"]), "Imported from MeroShare holdings snapshot WACC", None

        history_rows = conn.execute(
            """
            SELECT transaction_date, quantity, rate, amount, wacc, transaction_type
            FROM meroshare_purchase_history
            WHERE account_id = ? AND symbol = ?
            ORDER BY
                CASE WHEN UPPER(COALESCE(transaction_type, '')) = 'WACC' THEN 0 ELSE 1 END,
                transaction_date DESC,
                id DESC
            """,
            (account_id, symbol),
        ).fetchall()

        opened_at = None
        if history_rows:
            dated = [row["transaction_date"] for row in history_rows if row["transaction_date"]]
            opened_at = min(dated) if dated else None

        for row in history_rows:
            wacc = row["wacc"]
            rate = row["rate"]
            qty = float(row["quantity"] or 0)
            amount = float(row["amount"] or 0)
            if wacc not in (None, ""):
                return float(wacc), "Imported from MeroShare WACC summary", opened_at
            if qty > 0 and rate not in (None, ""):
                return float(rate), "Imported from MeroShare purchase history", opened_at
            if qty > 0 and amount > 0:
                return amount / qty, "Imported from MeroShare purchase history amount", opened_at

        existing_position = conn.execute(
            "SELECT avg_cost, opened_at FROM portfolio_positions WHERE portfolio_id = ? AND symbol = ?",
            (portfolio_id, symbol),
        ).fetchone()
        if existing_position and float(existing_position["avg_cost"] or 0) > 0:
            return float(existing_position["avg_cost"]), "Kept existing portfolio cost basis", existing_position["opened_at"] or opened_at
        return 0.0, "Imported from MeroShare without cost basis data", opened_at

    portfolio = conn.execute("SELECT * FROM portfolios WHERE id = ?", (portfolio_id,)).fetchone()
    if not portfolio:
        raise MeroShareClientError("Portfolio not found.")
    snapshot_row = conn.execute(
        "SELECT MAX(snapshot_at) AS snapshot_at FROM meroshare_holdings_snapshots WHERE account_id = ?",
        (account_id,),
    ).fetchone()
    snapshot_at = snapshot_row["snapshot_at"] if snapshot_row else None
    if not snapshot_at:
        raise MeroShareClientError("No synced MeroShare holdings are available for this account.")

    rows = conn.execute(
        """
        SELECT *
        FROM meroshare_holdings_snapshots
        WHERE account_id = ? AND snapshot_at = ?
        ORDER BY symbol ASC
        """,
        (account_id, snapshot_at),
    ).fetchall()
    snapshot_symbols = {str(row["symbol"]) for row in rows}
    stale_symbols = [
        row["symbol"]
        for row in conn.execute(
            """
            SELECT symbol
            FROM portfolio_position_sources
            WHERE portfolio_id = ? AND source_type = 'meroshare' AND source_account_id = ?
            """,
            (portfolio_id, account_id),
        ).fetchall()
        if row["symbol"] not in snapshot_symbols
    ]
    for symbol in stale_symbols:
        conn.execute(
            "DELETE FROM portfolio_position_sources WHERE portfolio_id = ? AND symbol = ? AND source_type = 'meroshare'",
            (portfolio_id, symbol),
        )
        conn.execute(
            "DELETE FROM portfolio_positions WHERE portfolio_id = ? AND symbol = ?",
            (portfolio_id, symbol),
        )
    imported = 0
    for row in rows:
        qty = float(row["quantity"] or 0)
        avg_cost, note_prefix, opened_at = resolve_import_metrics(str(row["symbol"]), row)
        conn.execute(
            """
            INSERT INTO portfolio_positions(portfolio_id, symbol, qty, avg_cost, opened_at, notes)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(portfolio_id, symbol)
            DO UPDATE SET qty=excluded.qty, avg_cost=excluded.avg_cost, opened_at=COALESCE(excluded.opened_at, portfolio_positions.opened_at), notes=excluded.notes
            """,
            (
                portfolio_id,
                row["symbol"],
                qty,
                avg_cost,
                opened_at,
                f"{note_prefix} | MeroShare account {account_id} snapshot {snapshot_at}",
            ),
        )
        conn.execute(
            """
            INSERT INTO portfolio_position_sources(portfolio_id, symbol, source_type, source_account_id, source_snapshot_id, imported_at)
            VALUES (?, ?, 'meroshare', ?, ?, ?)
            ON CONFLICT(portfolio_id, symbol, source_type)
            DO UPDATE SET source_account_id=excluded.source_account_id, source_snapshot_id=excluded.source_snapshot_id, imported_at=excluded.imported_at
            """,
            (portfolio_id, row["symbol"], account_id, row["id"], _utcnow()),
        )
        imported += 1

    conn.commit()
    return {
        "portfolio_id": portfolio_id,
        "account_id": account_id,
        "snapshot_at": snapshot_at,
        "imported_count": imported,
    }
