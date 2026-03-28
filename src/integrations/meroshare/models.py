from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class MeroShareCredentials:
    client_id: int
    username: str
    password: str


@dataclass(slots=True)
class MeroShareSession:
    auth_token: str
    created_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: datetime | None = None
    status: str = "ACTIVE"


@dataclass(slots=True)
class OwnDetail:
    customer_id: int | None
    name: str | None
    demat: str | None
    client_code: str | None
    dp_name: str | None
    boid: str | None
    raw: dict[str, Any]


@dataclass(slots=True)
class LinkedAccount:
    customer_id: int | None
    boid: str | None
    name: str | None
    raw: dict[str, Any]


@dataclass(slots=True)
class HoldingRow:
    symbol: str
    security_name: str | None
    quantity: float
    wacc: float | None
    market_rate: float | None
    market_value: float | None
    raw: dict[str, Any]


@dataclass(slots=True)
class PurchaseHistoryRow:
    symbol: str
    transaction_date: str | None
    quantity: float
    rate: float | None
    amount: float | None
    wacc: float | None
    transaction_type: str | None
    raw: dict[str, Any]


@dataclass(slots=True)
class TransactionHistoryRow:
    symbol: str | None
    transaction_date: str | None
    quantity: float | None
    rate: float | None
    amount: float | None
    transaction_type: str | None
    boid: str | None
    client_code: str | None
    raw: dict[str, Any]
