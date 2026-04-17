# src/api/app.py  — v2026.03.13
import os
import sys

# Must happen before any other imports — force SQLite
os.environ["DATABASE_URL"] = ""
os.environ.pop("DATABASE_URL", None)
os.environ.pop("POSTGRES_URL", None)
os.environ.pop("TURSO_DATABASE_URL", None)
os.environ.pop("TURSO_AUTH_TOKEN", None)

# Fix import paths for Railway
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "src"))

import sqlite3
import re
import json
import requests
from datetime import datetime, date
from flask import Flask, jsonify, request
from flask_cors import CORS
from src.integrations.meroshare import (
    MeroShareClient,
    MeroShareClientError,
    MeroShareSessionExpired,
    connect_meroshare_account,
    ensure_meroshare_tables,
    get_meroshare_account,
    get_meroshare_holdings,
    get_meroshare_purchase_history,
    get_meroshare_transactions,
    import_meroshare_holdings_to_portfolio,
    list_meroshare_accounts,
    sync_meroshare_account,
)

app = Flask(__name__)
CORS(app, origins=["*"])

SHAREHUB_TODAYS_PRICE_URL = "https://sharehubnepal.com/live/api/v2/nepselive/todays-price"

# ── CONFIG ────────────────────────────────────────────────────────────────────
try:
    from config import (API_PORT, FLASK_DEBUG,
                        DEFAULT_STOP_LOSS_PCT, DEFAULT_TAKE_PROFIT_PCT,
                        DEFAULT_MAX_HOLD_DAYS, DEFAULT_INITIAL_CAPITAL,
                        DEFAULT_POSITION_SIZE_PCT)
except ImportError:
    API_PORT = 5000; FLASK_DEBUG = False
    DEFAULT_STOP_LOSS_PCT = 5.0; DEFAULT_TAKE_PROFIT_PCT = 10.0
    DEFAULT_MAX_HOLD_DAYS = 15;  DEFAULT_INITIAL_CAPITAL = 100_000
    DEFAULT_POSITION_SIZE_PCT = 10.0

# ── DB HELPER ─────────────────────────────────────────────────────────────────
if "RAILWAY_VOLUME_MOUNT_PATH" in os.environ:
    DB_PATH = os.path.join(os.environ["RAILWAY_VOLUME_MOUNT_PATH"], "nepse.db")
else:
    DB_PATH = os.path.join(ROOT, "data", "nepse.db")

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=60.0)
    conn.row_factory = sqlite3.Row
    return conn


with get_db() as _bootstrap_conn:
    ensure_meroshare_tables(_bootstrap_conn)

# -- ADMIN KEY (Change this if you want) --
SCRAPE_KEY = "antigravity_trigger_2026"

def rows_to_list(cursor_rows):
    return [dict(r) for r in cursor_rows]


def table_exists(conn, table_name):
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def get_table_max_date(conn, table_name):
    if not table_exists(conn, table_name):
        return None
    row = conn.execute(f"SELECT MAX(date) FROM {table_name}").fetchone()
    return row[0] if row and row[0] else None


def get_data_freshness(conn):
    return {
        "price_history": get_table_max_date(conn, "price_history"),
        "floor_sheet": get_table_max_date(conn, "floor_sheet"),
        "broker_summary": get_table_max_date(conn, "broker_summary"),
        "daily_price": get_table_max_date(conn, "daily_price"),
        "market_summary": get_table_max_date(conn, "market_summary"),
    }


def clamp_probability(value):
    try:
        val = float(value)
    except (TypeError, ValueError):
        return 0.0
    if val > 1:
        val = val / 100.0
    return max(0.0, min(1.0, val))


def confidence_band(probability):
    if probability >= 0.75:
        return "high"
    if probability >= 0.6:
        return "medium"
    return "low"


def heuristic_prediction_label(signal_text, total_score):
    signal = (signal_text or "").upper()
    score = float(total_score or 0)
    if signal == "BUY" or score >= 70:
        return "BUY"
    if signal == "HOLD" and score < 45:
        return "HOLD"
    if score >= 50:
        return "WATCH"
    return "IGNORE"


def build_prediction_drivers(row):
    candidates = [
        ("broker_accumulation", row.get("broker_accumulation"), "positive"),
        ("liquidity_spike", row.get("liquidity_spike"), "positive"),
        ("pump_score", row.get("pump_score"), "positive"),
        ("total_score", row.get("total_score"), "positive"),
        ("volume_score", row.get("volume_score"), "positive"),
        ("rsi_score", row.get("rsi_score"), "positive"),
        ("broker_distribution", row.get("broker_distribution"), "negative"),
    ]
    normalized = []
    for feature, value, direction in candidates:
        try:
            impact = float(value)
        except (TypeError, ValueError):
            continue
        if impact == 0:
            continue
        normalized.append({
            "feature": feature,
            "impact": round(impact, 4),
            "direction": direction,
        })
    normalized.sort(key=lambda item: abs(item["impact"]), reverse=True)
    return normalized[:4]


def fetch_heuristic_prediction_rows(conn, symbol=None, batch_date=None, limit=25):
    latest_signals_date = get_table_max_date(conn, "signals")
    target_date = batch_date or latest_signals_date
    if not target_date or not table_exists(conn, "signals"):
        return []

    query = """
        SELECT
            s.symbol,
            s.date,
            s.close,
            s.total_score,
            s.signal,
            s.market_condition,
            s.volume_score,
            s.rsi_score,
            ns.pump_score,
            ns.liquidity_spike,
            ns.broker_accumulation,
            ns.broker_distribution,
            ns.net_broker_flow,
            ns.volatility_regime,
            dp.volume
        FROM signals s
        LEFT JOIN nepse_signals ns
            ON ns.symbol = s.symbol AND ns.date = s.date
        LEFT JOIN daily_price dp
            ON dp.symbol = s.symbol AND dp.date = s.date
        WHERE s.date = ?
    """
    params = [target_date]

    if symbol:
        query += " AND s.symbol = ?"
        params.append(symbol.upper())

    query += " ORDER BY s.total_score DESC, s.symbol ASC"
    if limit:
        query += " LIMIT ?"
        params.append(int(limit))

    return rows_to_list(conn.execute(query, params).fetchall())


def serialize_heuristic_prediction(row):
    probability = clamp_probability(row.get("total_score"))
    return {
        "symbol": row.get("symbol"),
        "company_name": row.get("symbol"),
        "prediction": heuristic_prediction_label(row.get("signal"), row.get("total_score")),
        "probability": round(probability, 4),
        "confidence_band": confidence_band(probability),
        "threshold": 0.6,
        "expected_horizon_days": 7,
        "top_drivers": build_prediction_drivers(row),
        "snapshot": {
            "close": row.get("close"),
            "volume": row.get("volume"),
            "sector": None,
            "market_condition": row.get("market_condition"),
        },
    }


def latest_model_registry_row(conn):
    if not table_exists(conn, "ml_model_registry"):
        return None
    row = conn.execute(
        """
        SELECT *
        FROM ml_model_registry
        ORDER BY created_at DESC, model_key DESC
        LIMIT 1
        """
    ).fetchone()
    return row_to_dict(row)


def latest_ml_prediction_batch_date(conn, model_key=None):
    if not table_exists(conn, "ml_predictions_daily"):
        return None
    if model_key:
        row = conn.execute(
            "SELECT MAX(batch_date) AS batch_date FROM ml_predictions_daily WHERE model_key = ?",
            (model_key,),
        ).fetchone()
    else:
        row = conn.execute("SELECT MAX(batch_date) AS batch_date FROM ml_predictions_daily").fetchone()
    return row["batch_date"] if row and row["batch_date"] else None


def parse_json_field(value, fallback):
    if not value:
        return fallback
    try:
        return json.loads(value)
    except Exception:
        return fallback


def serialize_model_prediction(row):
    snapshot = parse_json_field(row.get("snapshot_json"), {})
    feature_snapshot = parse_json_field(row.get("features_json"), {})
    top_drivers = parse_json_field(row.get("top_drivers_json"), [])
    return {
        "symbol": row.get("symbol"),
        "company_name": row.get("company_name") or row.get("symbol"),
        "prediction": row.get("prediction"),
        "probability": round(float(row.get("probability") or 0), 6),
        "confidence_band": row.get("confidence_band"),
        "threshold": float(row.get("threshold") or 0),
        "expected_horizon_days": int(row.get("expected_horizon_days") or 5),
        "rank": int(row.get("rank") or 0),
        "top_drivers": top_drivers,
        "snapshot": snapshot,
        "features": feature_snapshot,
        "model_key": row.get("model_key"),
        "target": row.get("target"),
        "validation_metrics": {
            "precision": float(row.get("validation_precision") or 0),
            "recall": float(row.get("validation_recall") or 0),
            "accuracy": float(row.get("validation_accuracy") or 0),
        } if row.get("target") else None,
    }


def fetch_model_prediction_rows(conn, symbol=None, batch_date=None, model_key=None, signal=None, limit=25):
    if not table_exists(conn, "ml_predictions_daily"):
        return []
    resolved_model_key = model_key
    if not resolved_model_key:
        latest_model = latest_model_registry_row(conn)
        resolved_model_key = latest_model["model_key"] if latest_model else None
    target_date = batch_date or latest_ml_prediction_batch_date(conn, resolved_model_key)
    if not target_date:
        return []

    query = """
        SELECT
            mpd.*,
            mr.target,
            mr.validation_precision,
            mr.validation_recall,
            mr.validation_accuracy,
            COALESCE(ac.company_name, mpd.symbol) AS company_name
        FROM ml_predictions_daily mpd
        LEFT JOIN ml_model_registry mr
          ON mr.model_key = mpd.model_key
        LEFT JOIN about_company ac
          ON ac.symbol = mpd.symbol
        WHERE mpd.batch_date = ?
    """
    params = [target_date]
    if resolved_model_key:
        query += " AND mpd.model_key = ?"
        params.append(resolved_model_key)
    if symbol:
        query += " AND mpd.symbol = ?"
        params.append(symbol.upper())
    if signal:
        query += " AND UPPER(mpd.prediction) = ?"
        params.append(signal.upper())
    query += " ORDER BY mpd.rank ASC, mpd.probability DESC, mpd.symbol ASC LIMIT ?"
    params.append(int(limit))
    return rows_to_list(conn.execute(query, params).fetchall())


def compute_broker_edge_windows(conn, threshold=0.6):
    windows = []
    window_specs = [(3, 5), (5, 7), (10, 10)]
    if not table_exists(conn, "broker_summary") or not table_exists(conn, "daily_price"):
        return windows

    for lookback_days, forward_days in window_specs:
        row = conn.execute(
            """
            WITH broker_day AS (
                SELECT
                    symbol,
                    date,
                    AVG(CASE WHEN net_qty > 0 THEN 1.0 ELSE 0.0 END) AS accumulation_ratio
                FROM broker_summary
                GROUP BY symbol, date
            ),
            aligned AS (
                SELECT
                    dp.symbol,
                    dp.date,
                    dp.close,
                    AVG(bd.accumulation_ratio) OVER (
                        PARTITION BY dp.symbol
                        ORDER BY dp.date
                        ROWS BETWEEN ? PRECEDING AND CURRENT ROW
                    ) AS lookback_accumulation,
                    ROW_NUMBER() OVER (
                        PARTITION BY dp.symbol
                        ORDER BY dp.date
                    ) AS row_num,
                    LEAD(dp.close, ?) OVER (
                        PARTITION BY dp.symbol
                        ORDER BY dp.date
                    ) AS future_close
                FROM daily_price dp
                JOIN broker_day bd
                    ON bd.symbol = dp.symbol AND bd.date = dp.date
            ),
            samples AS (
                SELECT
                    symbol,
                    date,
                    lookback_accumulation,
                    ((future_close - close) / NULLIF(close, 0)) * 100.0 AS forward_return
                FROM aligned
                WHERE row_num >= ? AND future_close IS NOT NULL
            )
            SELECT
                COUNT(*) AS sample_count,
                AVG(CASE WHEN forward_return > 0 THEN 1.0 ELSE 0.0 END) AS positive_rate,
                AVG(forward_return) AS avg_forward_return,
                AVG(forward_return) FILTER (WHERE forward_return IS NOT NULL) AS median_proxy
            FROM samples
            WHERE lookback_accumulation >= ?
            """,
            (lookback_days - 1, forward_days, lookback_days, threshold),
        ).fetchone()

        sample_count = int(row["sample_count"] or 0)
        windows.append({
            "lookback_days": lookback_days,
            "forward_days": forward_days,
            "threshold": threshold,
            "sample_count": sample_count,
            "positive_rate": round(float(row["positive_rate"] or 0), 4) if sample_count else 0.0,
            "avg_forward_return": round(float(row["avg_forward_return"] or 0), 4) if sample_count else 0.0,
            "median_forward_return": round(float(row["median_proxy"] or 0), 4) if sample_count else 0.0,
        })

    return windows


DERIVED_SCANNERS = {
    "broker_activity",
    "accumulation",
    "broker_smartflow",
    "best_volume_transaction",
    "best_gainer_loser",
    "smc_scan",
    "support_resistance",
    "best_of_best",
}

MARKET_BREADTH_CATEGORIES = {
    "advanced",
    "declined",
    "unchanged",
    "positive-circuit",
    "negative-circuit",
}


def get_latest_snapshot_date(conn, table_name, requested_date=None):
    if requested_date:
        return requested_date
    return get_table_max_date(conn, table_name)


def row_to_dict(row):
    return dict(row) if row else None


def get_table_columns(conn, table_name):
    if not table_exists(conn, table_name):
        return set()
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}


def build_companies_catalog(conn):
    company_columns = get_table_columns(conn, "companies")
    latest_snapshot_date = get_table_max_date(conn, "stock_factor_snapshot")

    catalog = {}

    if company_columns:
        select_parts = ["symbol"]
        if "name" in company_columns:
            select_parts.append("name")
        elif "label" in company_columns:
            select_parts.append("label")
        if "sector" in company_columns:
            select_parts.append("sector")
        company_rows = conn.execute(
            f"SELECT {', '.join(select_parts)} FROM companies ORDER BY symbol"
        ).fetchall()
        for row in company_rows:
            symbol = row["symbol"]
            label = row["name"] if "name" in row.keys() else row["label"] if "label" in row.keys() else symbol
            sector = row["sector"] if "sector" in row.keys() else None
            catalog[symbol] = {
                "symbol": symbol,
                "name": label or symbol,
                "sector": sector or "Unknown",
            }

    if latest_snapshot_date:
        snapshot_rows = conn.execute(
            """
            SELECT DISTINCT symbol, security_name, sector
            FROM stock_factor_snapshot
            WHERE date = ?
            ORDER BY symbol
            """,
            (latest_snapshot_date,),
        ).fetchall()
        for row in snapshot_rows:
            symbol = row["symbol"]
            entry = catalog.setdefault(
                symbol,
                {"symbol": symbol, "name": symbol, "sector": "Unknown"},
            )
            if row["security_name"]:
                entry["name"] = row["security_name"]
            if row["sector"]:
                entry["sector"] = row["sector"]

    live_symbol_rows = conn.execute(
        """
        SELECT symbol FROM (
            SELECT DISTINCT symbol FROM daily_price
            UNION
            SELECT DISTINCT symbol FROM price_history
            UNION
            SELECT DISTINCT symbol FROM stock_factor_snapshot
        )
        ORDER BY symbol
        """
    ).fetchall()
    for row in live_symbol_rows:
        symbol = row["symbol"]
        catalog.setdefault(
            symbol,
            {"symbol": symbol, "name": symbol, "sector": "Unknown"},
        )

    return [catalog[symbol] for symbol in sorted(catalog)]


def normalize_floorsheet_trade_rows(trades):
    normalized = []
    for trade in trades:
        row = dict(trade)
        if row.get("buyer_broker") is None:
            row["buyer_broker"] = "-"
        if row.get("seller_broker") is None:
            row["seller_broker"] = "-"
        normalized.append(row)
    return normalized


def sanitize_meroshare_account(account):
    if not account:
        return None
    hidden_keys = {
        "auth_token",
        "raw_profile_json",
    }
    sanitized = {k: v for k, v in account.items() if k not in hidden_keys}
    linked = sanitized.get("linked_accounts")
    if isinstance(linked, list):
        sanitized["linked_accounts"] = [
            {k: v for k, v in item.items() if k not in {"raw_json"}}
            for item in linked
        ]
    return sanitized


def build_market_breadth_dataset(conn, latest_daily_date, previous_daily_date):
    if not latest_daily_date or not previous_daily_date:
        return []
    query = """
        WITH latest_market AS (
            SELECT symbol, open, high, low, close, volume, price_change_pct
            FROM clean_price_history
            WHERE date = ?
        ),
        latest_with_previous AS (
            SELECT
                lm.*,
                CASE
                    WHEN lm.price_change_pct IS NULL THEN NULL
                    WHEN ABS(100.0 + lm.price_change_pct) < 0.000001 THEN NULL
                    ELSE lm.close / (1.0 + (lm.price_change_pct / 100.0))
                END AS previous_close
            FROM latest_market lm
        ),
        latest_activity AS (
            SELECT symbol, open, high, low, volume, amount, trades
            FROM daily_price
            WHERE date = ?
        ),
        last_trade AS (
            SELECT symbol, quantity AS ltv
            FROM (
                SELECT
                    symbol,
                    quantity,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol
                        ORDER BY page_no DESC, row_no_in_page DESC, id DESC
                    ) AS rn
                FROM floor_sheet
                WHERE date = ?
            ) ranked
            WHERE rn = 1
        )
        SELECT
            lm.symbol,
            COALESCE(ac.company_name, lm.symbol) AS company_name,
            ac.sector,
            ROUND(lm.close, 4) AS ltp,
            ROUND(lm.close - lm.previous_close, 4) AS change,
            ROUND(lm.price_change_pct, 4) AS change_percent,
            ROUND(COALESCE(la.open, lm.open), 4) AS open,
            ROUND(COALESCE(la.high, lm.high), 4) AS high,
            ROUND(COALESCE(la.low, lm.low), 4) AS low,
            ROUND(COALESCE(la.volume, lm.volume), 4) AS volume,
            ROUND(COALESCE(la.amount, 0), 4) AS turnover,
            ROUND(COALESCE(lt.ltv, 0), 4) AS ltv,
            ROUND(lm.previous_close, 4) AS previous_close
        FROM latest_with_previous lm
        LEFT JOIN latest_activity la
          ON la.symbol = lm.symbol
        LEFT JOIN last_trade lt
          ON lt.symbol = lm.symbol
        LEFT JOIN about_company ac
          ON ac.symbol = lm.symbol
    """
    return rows_to_list(
        conn.execute(
            query,
            (latest_daily_date, latest_daily_date, latest_daily_date),
        ).fetchall()
    )


def fetch_latest_scanner_rows(conn, scanner_name, batch_date=None, limit=200, sector=None, signal=None):
    if scanner_name not in DERIVED_SCANNERS:
        return []
    target_date = get_latest_snapshot_date(conn, "scanner_results_daily", batch_date)
    if not target_date:
        return []
    where = ["date = ?", "scanner_name = ?"]
    params = [target_date, scanner_name]
    if sector:
        where.append("sector = ?")
        params.append(sector)
    if signal:
        where.append("UPPER(COALESCE(signal, '')) = ?")
        params.append(signal.upper())
    query = f"""
        SELECT *
        FROM scanner_results_daily
        WHERE {' AND '.join(where)}
        ORDER BY rank ASC, score DESC, symbol ASC
        LIMIT ?
    """
    params.append(limit)
    return rows_to_list(conn.execute(query, params).fetchall())


def compute_portfolio_health(conn, portfolio_id):
    def brokerage_rate(amount):
        amount = float(amount or 0)
        if amount <= 50000:
            return 0.0036
        if amount <= 500000:
            return 0.0033
        if amount <= 2000000:
            return 0.0031
        if amount <= 10000000:
            return 0.0027
        return 0.0024

    def brokerage_fee(amount):
        amount = float(amount or 0)
        fee = amount * brokerage_rate(amount)
        return max(fee, 10.0)

    def buy_side_charges(amount):
        amount = float(amount or 0)
        return {
            "broker_fee": round(brokerage_fee(amount), 4),
            "sebon_fee": round(amount * 0.00015, 4),
            "dp_fee": 25.0,
            "transfer_fee": 5.0,
        }

    def sell_side_charges(amount):
        amount = float(amount or 0)
        return {
            "broker_fee": round(brokerage_fee(amount), 4),
            "sebon_fee": round(amount * 0.00015, 4),
            "dp_fee": 25.0,
        }

    def sum_charges(charges):
        return round(sum(float(v or 0) for v in charges.values()), 4)

    def holding_period_days(opened_at):
        if not opened_at:
            return None
        normalized = str(opened_at).strip()
        for parser in (datetime.fromisoformat,):
            try:
                opened = parser(normalized.replace("Z", "+00:00"))
                return max((date.today() - opened.date()).days, 0)
            except Exception:
                continue
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                opened = datetime.strptime(normalized, fmt)
                return max((date.today() - opened.date()).days, 0)
            except Exception:
                continue
        return None

    positions = rows_to_list(
        conn.execute(
            """
            SELECT
                pp.portfolio_id,
                pp.symbol,
                pp.qty,
                pp.avg_cost,
                pp.opened_at,
                pp.notes,
                sfs.close,
                sfs.change_pct,
                sfs.composite_score,
                sfs.risk_score,
                sfs.signal_label,
                sfs.sector
            FROM portfolio_positions pp
            LEFT JOIN stock_factor_snapshot sfs
              ON sfs.symbol = pp.symbol
             AND sfs.date = (SELECT MAX(date) FROM stock_factor_snapshot)
            WHERE pp.portfolio_id = ?
            ORDER BY pp.symbol
            """,
            (portfolio_id,),
        ).fetchall()
    )
    enriched = []
    total_cost = 0.0
    total_value = 0.0
    total_receivable = 0.0
    total_buy_fees = 0.0
    total_sell_fees = 0.0
    total_tax = 0.0
    total_day_pnl = 0.0
    total_qty = 0.0
    known_cost_position_count = 0
    composite_weight = 0.0
    risk_weight = 0.0
    for row in positions:
        qty = float(row.get("qty") or 0)
        avg_cost = float(row.get("avg_cost") or 0)
        close = float(row.get("close") or 0)
        change_pct = float(row.get("change_pct") or 0)
        cost_basis_known = avg_cost > 0
        gross_buy = qty * avg_cost
        gross_sale = qty * close
        buy_charges = buy_side_charges(gross_buy) if qty > 0 and avg_cost > 0 else {
            "broker_fee": 0.0,
            "sebon_fee": 0.0,
            "dp_fee": 0.0,
            "transfer_fee": 0.0,
        }
        sell_charges = sell_side_charges(gross_sale) if qty > 0 and close > 0 else {
            "broker_fee": 0.0,
            "sebon_fee": 0.0,
            "dp_fee": 0.0,
        }
        buy_fee_total = sum_charges(buy_charges)
        sell_fee_total = sum_charges(sell_charges)
        cost = gross_buy + buy_fee_total
        value = gross_sale
        pre_tax_pnl = gross_sale - sell_fee_total - cost if cost_basis_known else None
        held_days = holding_period_days(row.get("opened_at"))
        cgt_rate = 0.05 if held_days is not None and held_days > 365 else 0.075
        capital_gains_tax = round(max(pre_tax_pnl, 0.0) * cgt_rate, 4) if pre_tax_pnl is not None else 0.0
        receivable = gross_sale - sell_fee_total - capital_gains_tax if cost_basis_known else None
        pnl = (receivable - cost) if cost_basis_known and receivable is not None else None
        pnl_pct = (pnl / cost * 100.0) if cost_basis_known and cost and pnl is not None else None
        previous_close = close / (1.0 + (change_pct / 100.0)) if close and abs(100.0 + change_pct) > 1e-9 else close
        day_pnl = (close - previous_close) * qty if close and previous_close else 0.0

        if cost_basis_known:
            total_cost += cost
            total_receivable += receivable or 0.0
            total_buy_fees += buy_fee_total
            total_sell_fees += sell_fee_total
            total_tax += capital_gains_tax
            known_cost_position_count += 1
        total_value += value
        total_day_pnl += day_pnl
        total_qty += qty
        composite_weight += float(row.get("composite_score") or 0) * value
        risk_weight += float(row.get("risk_score") or 0) * value
        row.update(
            {
                "cost_basis_known": cost_basis_known,
                "gross_cost": round(gross_buy, 4),
                "gross_sale_value": round(gross_sale, 4),
                "buy_broker_fee": round(buy_charges["broker_fee"], 4),
                "buy_sebon_fee": round(buy_charges["sebon_fee"], 4),
                "buy_dp_fee": round(buy_charges["dp_fee"], 4),
                "buy_transfer_fee": round(buy_charges["transfer_fee"], 4),
                "buy_fee_total": round(buy_fee_total, 4),
                "sell_broker_fee": round(sell_charges["broker_fee"], 4),
                "sell_sebon_fee": round(sell_charges["sebon_fee"], 4),
                "sell_dp_fee": round(sell_charges["dp_fee"], 4),
                "sell_fee_total": round(sell_fee_total, 4),
                "estimated_capital_gains_tax": round(capital_gains_tax, 4),
                "receivable_amount": round(receivable, 4) if receivable is not None else None,
                "market_value": round(value, 4),
                "cost_basis": round(cost, 4) if cost_basis_known else None,
                "pre_tax_pnl": round(pre_tax_pnl, 4) if pre_tax_pnl is not None else None,
                "pnl": round(pnl, 4) if pnl is not None else None,
                "pnl_pct": round(pnl_pct, 4) if pnl_pct is not None else None,
                "day_pnl": round(day_pnl, 4),
                "holding_days": held_days,
                "capital_gains_tax_rate": cgt_rate,
            }
        )
        enriched.append(row)

    total_pnl = total_receivable - total_cost
    avg_composite = (composite_weight / total_value) if total_value else 0.0
    avg_risk = (risk_weight / total_value) if total_value else 0.0
    return {
        "portfolio_id": portfolio_id,
        "positions": enriched,
        "summary": {
            "total_cost": round(total_cost, 4),
            "market_value": round(total_value, 4),
            "receivable_amount": round(total_receivable, 4),
            "total_buy_fees": round(total_buy_fees, 4),
            "total_sell_fees": round(total_sell_fees, 4),
            "estimated_capital_gains_tax": round(total_tax, 4),
            "total_pnl": round(total_pnl, 4),
            "total_pnl_pct": round((total_pnl / total_cost * 100.0), 4) if total_cost else 0.0,
            "day_pnl": round(total_day_pnl, 4),
            "avg_composite_score": round(avg_composite, 4),
            "avg_risk_score": round(avg_risk, 4),
            "position_count": len(enriched),
            "known_cost_position_count": known_cost_position_count,
            "current_units": round(total_qty, 4),
            "sold_units": 0.0,
            "sold_value": 0.0,
            "dividend": 0.0,
            "realized_pnl": 0.0,
        },
    }


def detect_db_backend(conn):
    """Best-effort backend detection for health diagnostics."""
    # Standard local sqlite3 connection
    if isinstance(conn, sqlite3.Connection):
        return "sqlite"

    # Turso wrappers from src.data.db_factory (old + new paths)
    if hasattr(conn, "raw_conn") or hasattr(conn, "client"):
        return "turso"

    t = type(conn)
    module_name = getattr(t, "__module__", "").lower()
    class_name = getattr(t, "__name__", "").lower()
    if "libsql" in module_name or "libsql" in class_name or "clientconnection" in class_name:
        return "turso"

    return "unknown"


def floor_sheet_table_exists(conn):
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='floor_sheet'"
    ).fetchone()
    return row is not None


def get_latest_floor_sheet_date(conn):
    row = conn.execute("SELECT MAX(date) FROM floor_sheet").fetchone()
    return row[0] if row and row[0] else None


def parse_pagination(default_limit=5000, max_limit=20000):
    page = max(int(request.args.get("page", 1)), 1)
    limit = max(int(request.args.get("limit", default_limit)), 1)
    limit = min(limit, max_limit)
    offset = (page - 1) * limit
    return page, limit, offset


def resolve_date_window(conn, table_name, symbol=None, days=30):
    start_date = request.args.get("startDate", "").strip()
    end_date = request.args.get("endDate", "").strip()

    where = []
    params = []
    if symbol:
        where.append("symbol = ?")
        params.append(symbol)

    if start_date and end_date:
        where.append("date BETWEEN ? AND ?")
        params.extend([start_date, end_date])
        return start_date, end_date, where, params

    where_clause = f"WHERE {' AND '.join(where)}" if where else ""
    cutoff = conn.execute(
        f"""
        SELECT date
        FROM (
            SELECT DISTINCT date
            FROM {table_name}
            {where_clause}
            ORDER BY date DESC
            LIMIT ?
        )
        ORDER BY date ASC
        LIMIT 1
        """,
        [*params, days],
    ).fetchone()
    if not cutoff:
        return None, None, where, params

    since = cutoff[0]
    latest_query = f"SELECT MAX(date) FROM {table_name} {where_clause}"
    latest = conn.execute(latest_query, params).fetchone()[0]
    return since, latest, where, params


def discover_endpoints():
    """Build endpoint list dynamically from registered Flask routes."""
    endpoints = []
    for rule in app.url_map.iter_rules():
        if rule.endpoint == "static":
            continue

        path = re.sub(r"<([^>]+)>", lambda m: f"<{m.group(1).upper()}>", rule.rule)
        methods = sorted(m for m in rule.methods if m not in {"HEAD", "OPTIONS"})
        for method in methods:
            endpoints.append(f"{method} {path}")

    return sorted(endpoints, key=lambda e: (e.split(" ", 1)[1], e))


# ── ROOT INDEX ────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return jsonify({
        "name":    "NEPSE Algorithmic Trading API",
        "version": "1.0",
        "status":  "running",
        "endpoints": discover_endpoints(),
    })


# ── HEALTH ────────────────────────────────────────────────────────────────────
@app.route("/api/health")
def health():
    try:
        conn = get_db()
        backend = detect_db_backend(conn)
        try:
            price_rows = conn.execute("SELECT COUNT(*) FROM price_history").fetchone()[0]
            clean_rows = conn.execute("SELECT COUNT(*) FROM clean_price_history").fetchone()[0]
            companies  = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
        except Exception:
            price_rows = 0; clean_rows = 0; companies = 0
        finally:
            conn.close()

        env_vars = {k: v for k, v in os.environ.items() if "VOL" in k.upper() or "RAILWAY" in k.upper()}
        turso_url_present = bool(os.environ.get("TURSO_DATABASE_URL"))
        turso_token_present = bool(os.environ.get("TURSO_AUTH_TOKEN"))
        return jsonify({
            "status": "ok",
            "db_path": DB_PATH,
            "db_source": backend,
            "turso_config_present": turso_url_present and turso_token_present,
            "turso_url_present": turso_url_present,
            "turso_token_present": turso_token_present,
            "time": datetime.now().isoformat(),
            "env": env_vars,
            "db": {"companies": companies, "price_rows": price_rows, "clean_rows": clean_rows},
        })
    except Exception as e:
        import traceback
        return jsonify({"status": "error", "error": str(e), "traceback": traceback.format_exc()}), 500


# ── COMPANY DETAILS (Merolagani Info) ─────────────────────────────────────────
@app.route("/api/company/<symbol>")
def get_company_about(symbol):
    conn = get_db()
    try:
        symbol = symbol.upper()
        # Check if table exists
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        if "about_company" not in tables:
            return jsonify({"error": "about_company table not found"}), 404
            
        row = conn.execute("SELECT * FROM about_company WHERE symbol = ?", (symbol,)).fetchone()
        if not row:
            return jsonify({"error": f"No about info for '{symbol}'"}), 404
            
        return jsonify({"symbol": symbol, "about": dict(row)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route("/api/company/<symbol>/news")
def get_company_news(symbol):
    conn = get_db()
    try:
        symbol = symbol.upper()
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        if "news" not in tables:
            return jsonify({"error": "news table not found"}), 404
            
        rows = rows_to_list(conn.execute("SELECT * FROM news WHERE symbol = ?", (symbol,)).fetchall())
        return jsonify({"symbol": symbol, "news": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route("/api/company/<symbol>/dividends")
def get_company_dividends(symbol):
    conn = get_db()
    try:
        symbol = symbol.upper()
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        if "dividend" not in tables:
            return jsonify({"error": "dividend table not found"}), 404
            
        rows = rows_to_list(conn.execute("SELECT * FROM dividend WHERE symbol = ?", (symbol,)).fetchall())
        return jsonify({"symbol": symbol, "dividends": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

# -- REMOTE SCRAPE TRIGGER --
@app.route("/api/scrape/start", methods=["POST", "GET"])
def trigger_scrape():
    # Basic security
    key = request.args.get("key")
    if not key and request.is_json:
        key = request.json.get("key")
    
    if key != SCRAPE_KEY:
        return jsonify({"status": "unauthorized"}), 401
        
    import threading
    import subprocess
    
    def run_scraper():
        print("[Admin] Starting remote-triggered scrape...")
        try:
            # Run the pipeline script as a subprocess
            result = subprocess.run([sys.executable, "src/data/merolagani_pipeline.py"], 
                                    capture_output=True, text=True)
            print("[Admin] Scrape Complete.")
            print(result.stdout)
        except Exception as e:
            print(f"[Admin] Scrape Failed: {e}")

    thread = threading.Thread(target=run_scraper)
    thread.start()
    
    return jsonify({
        "status": "triggered",
        "message": "Scraper started in background on Railway server.",
        "time": datetime.now().isoformat()
    })


# ── MARKET OVERVIEW ───────────────────────────────────────────────────────────
@app.route("/api/market/overview")
def market_overview():
    conn = get_db()
    try:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        latest_daily_date = get_table_max_date(conn, "daily_price")
        latest_market_summary = conn.execute(
            "SELECT * FROM market_summary ORDER BY date DESC LIMIT 1"
        ).fetchone()
        previous_daily_date = None
        if latest_daily_date:
            previous_daily_date = conn.execute(
                "SELECT MAX(date) FROM daily_price WHERE date < ?",
                (latest_daily_date,),
            ).fetchone()[0]

        market_totals = {}
        breadth = {
            "advanced": 0,
            "declined": 0,
            "unchanged": 0,
            "positive_circuit": 0,
            "negative_circuit": 0,
            "compared_symbols": 0,
        }
        top_gainers = []
        top_losers = []
        top_turnover = []
        top_traded = []
        top_transactions = []
        sector_leaders = []
        marquee_strip = []
        recent_news = []
        public_offerings = []
        bulk_transactions = []
        recent_announcements = []
        proposed_dividends = []

        if latest_daily_date and previous_daily_date:
            market_totals_row = conn.execute(
                """
                SELECT
                    ? AS date,
                    SUM(amount) AS total_turnover,
                    SUM(volume) AS total_volume,
                    SUM(trades) AS total_transactions,
                    COUNT(*) AS total_scrips_traded
                FROM daily_price
                WHERE date = ?
                """,
                (latest_daily_date, latest_daily_date),
            ).fetchone()
            market_totals = dict(market_totals_row) if market_totals_row else {}

            breadth_row = conn.execute(
                """
                WITH latest AS (
                    SELECT symbol, close, high, low
                    FROM daily_price
                    WHERE date = ?
                ),
                previous AS (
                    SELECT ph.symbol, ph.close
                    FROM price_history ph
                    JOIN (
                        SELECT symbol, MAX(date) AS previous_date
                        FROM price_history
                        WHERE date < ?
                        GROUP BY symbol
                    ) prev
                      ON prev.symbol = ph.symbol
                     AND prev.previous_date = ph.date
                )
                SELECT
                    COUNT(*) AS compared_symbols,
                    SUM(CASE WHEN latest.close > previous.close THEN 1 ELSE 0 END) AS advanced,
                    SUM(CASE WHEN latest.close < previous.close THEN 1 ELSE 0 END) AS declined,
                    SUM(CASE WHEN latest.close = previous.close THEN 1 ELSE 0 END) AS unchanged,
                    SUM(
                        CASE
                            WHEN about_company.symbol IS NULL THEN 0
                            WHEN previous.close IS NOT NULL
                             AND previous.close <> 0
                             AND ((latest.close - previous.close) / previous.close) * 100.0 >= 9.95
                             AND latest.close >= latest.high
                            THEN 1 ELSE 0
                        END
                    ) AS positive_circuit,
                    SUM(
                        CASE
                            WHEN about_company.symbol IS NULL THEN 0
                            WHEN previous.close IS NOT NULL
                             AND previous.close <> 0
                             AND ((latest.close - previous.close) / previous.close) * 100.0 <= -9.95
                             AND latest.close <= latest.low
                            THEN 1 ELSE 0
                        END
                    ) AS negative_circuit
                FROM latest
                JOIN previous ON previous.symbol = latest.symbol
                LEFT JOIN about_company ON about_company.symbol = latest.symbol
                """,
                (latest_daily_date, latest_daily_date),
            ).fetchone()
            if breadth_row:
                breadth = dict(breadth_row)

            try:
                resp = requests.get(
                    SHAREHUB_TODAYS_PRICE_URL,
                    params={
                        "queryKey[0]": "todayPrice",
                        "queryKey[1][order]": "",
                        "queryKey[1][sortBy]": "",
                        "queryKey[1][searchText]": "",
                        "queryKey[1][page]": 1,
                        "queryKey[1][limit]": 500,
                        "queryKey[1][sortDirection]": "",
                    },
                    timeout=20,
                )
                resp.raise_for_status()
                payload = resp.json()
                items = payload.get("data") or payload.get("items") or payload.get("results") or []
                live_breadth = {
                    "advanced": 0,
                    "declined": 0,
                    "unchanged": 0,
                    "positive_circuit": 0,
                    "negative_circuit": 0,
                    "compared_symbols": 0,
                }
                for item in items:
                    prev_close = item.get("previousDayClosePrice")
                    ltp = item.get("ltp")
                    change_pct = item.get("changePercent")
                    high = item.get("highPrice")
                    low = item.get("lowPrice")
                    if prev_close is None or ltp is None:
                        continue
                    live_breadth["compared_symbols"] += 1
                    if ltp > prev_close:
                        live_breadth["advanced"] += 1
                    elif ltp < prev_close:
                        live_breadth["declined"] += 1
                    else:
                        live_breadth["unchanged"] += 1
                    if (
                        prev_close not in (None, 0)
                        and change_pct is not None
                        and high is not None
                        and low is not None
                    ):
                        if change_pct >= 9.95 and ltp >= high:
                            live_breadth["positive_circuit"] += 1
                        if change_pct <= -9.95 and ltp <= low:
                            live_breadth["negative_circuit"] += 1
                if live_breadth["compared_symbols"] > 0:
                    breadth = live_breadth
            except Exception:
                pass

            movers_query = """
                WITH latest_market AS (
                    SELECT symbol, close
                    FROM clean_price_history
                    WHERE date = ?
                ),
                previous_market AS (
                    SELECT symbol, close AS previous_close
                    FROM clean_price_history
                    WHERE date = ?
                ),
                latest_activity AS (
                    SELECT symbol, volume, amount, trades
                    FROM daily_price
                    WHERE date = ?
                )
                SELECT
                    latest_market.symbol,
                    latest_market.close,
                    latest_activity.volume,
                    latest_activity.amount,
                    latest_activity.trades,
                    previous_market.previous_close,
                    ROUND(latest_market.close - previous_market.previous_close, 4) AS change,
                    ROUND(((latest_market.close - previous_market.previous_close) / NULLIF(previous_market.previous_close, 0)) * 100.0, 4) AS change_percent,
                    about_company.company_name,
                    about_company.sector
                FROM latest_market
                JOIN previous_market ON previous_market.symbol = latest_market.symbol
                LEFT JOIN latest_activity ON latest_activity.symbol = latest_market.symbol
                LEFT JOIN about_company ON about_company.symbol = latest_market.symbol
            """

            top_gainers = rows_to_list(conn.execute(
                movers_query + " ORDER BY change_percent DESC LIMIT 10",
                (latest_daily_date, previous_daily_date, latest_daily_date)
            ).fetchall())
            top_losers = rows_to_list(conn.execute(
                movers_query + " ORDER BY change_percent ASC LIMIT 10",
                (latest_daily_date, previous_daily_date, latest_daily_date)
            ).fetchall())
            top_turnover = rows_to_list(conn.execute(
                movers_query + " ORDER BY latest_activity.amount DESC LIMIT 10",
                (latest_daily_date, previous_daily_date, latest_daily_date)
            ).fetchall())
            top_traded = rows_to_list(conn.execute(
                movers_query + " ORDER BY latest_activity.volume DESC LIMIT 10",
                (latest_daily_date, previous_daily_date, latest_daily_date)
            ).fetchall())
            top_transactions = rows_to_list(conn.execute(
                movers_query + " ORDER BY latest_activity.trades DESC LIMIT 10",
                (latest_daily_date, previous_daily_date, latest_daily_date)
            ).fetchall())

            sector_leaders = rows_to_list(conn.execute(
                """
                WITH latest AS (
                    SELECT dp.symbol, cp.close, dp.amount
                    FROM daily_price dp
                    JOIN clean_price_history cp
                      ON cp.symbol = dp.symbol AND cp.date = dp.date
                    WHERE dp.date = ?
                ),
                previous AS (
                    SELECT symbol, close AS previous_close
                    FROM clean_price_history
                    WHERE date = ?
                )
                SELECT
                    about_company.sector,
                    COUNT(*) AS scrip_count,
                    SUM(latest.amount) AS turnover,
                    ROUND(AVG(((latest.close - previous.previous_close) / NULLIF(previous.previous_close, 0)) * 100.0), 4) AS avg_change_percent
                FROM latest
                JOIN previous ON previous.symbol = latest.symbol
                LEFT JOIN about_company ON about_company.symbol = latest.symbol
                WHERE about_company.sector IS NOT NULL AND about_company.sector <> ''
                GROUP BY about_company.sector
                ORDER BY turnover DESC
                LIMIT 12
                """,
                (latest_daily_date, previous_daily_date),
            ).fetchall())

            marquee_strip = rows_to_list(conn.execute(
                """
                WITH latest AS (
                    SELECT dp.symbol, cp.close, dp.amount
                    FROM daily_price dp
                    JOIN clean_price_history cp
                      ON cp.symbol = dp.symbol AND cp.date = dp.date
                    WHERE dp.date = ?
                ),
                previous AS (
                    SELECT ph.symbol, ph.close AS previous_close
                    FROM clean_price_history ph
                    JOIN (
                        SELECT symbol, MAX(date) AS previous_date
                        FROM clean_price_history
                        WHERE date < ?
                        GROUP BY symbol
                    ) prev
                      ON prev.symbol = ph.symbol
                     AND prev.previous_date = ph.date
                )
                SELECT
                    latest.symbol,
                    ROUND(latest.close, 4) AS ltp,
                    ROUND(latest.close - previous.previous_close, 4) AS change,
                    ROUND(((latest.close - previous.previous_close) / NULLIF(previous.previous_close, 0)) * 100.0, 4) AS change_percent,
                    COALESCE(about_company.company_name, latest.symbol) AS company_name
                FROM latest
                JOIN previous ON previous.symbol = latest.symbol
                LEFT JOIN about_company ON about_company.symbol = latest.symbol
                ORDER BY ABS(change_percent) DESC, latest.amount DESC, latest.symbol ASC
                LIMIT 40
                """,
                (latest_daily_date, latest_daily_date),
            ).fetchall())

        if "sharehub_news_feed" in tables:
            recent_news = rows_to_list(conn.execute(
                """
                SELECT
                    post_id,
                    slug,
                    profile_name,
                    profile_image_url,
                    user_name,
                    is_profile_verified,
                    title,
                    summary,
                    media_url,
                    launch_url,
                    time_ago,
                    published_date
                FROM sharehub_news_feed
                WHERE COALESCE(media_type, 'News') = 'News'
                ORDER BY published_date DESC, post_id DESC
                LIMIT 8
                """
            ).fetchall())

        if "sharehub_public_offerings" in tables:
            public_offerings = rows_to_list(conn.execute(
                """
                SELECT
                    detail_slug,
                    symbol,
                    company_name,
                    ratio,
                    units,
                    price,
                    opening_date,
                    closing_date,
                    status,
                    detail_url,
                    offering_type
                FROM sharehub_public_offerings
                ORDER BY
                    CASE
                        WHEN closing_date IS NOT NULL
                         AND closing_date >= COALESCE(?, closing_date)
                        THEN 0 ELSE 1
                    END,
                    opening_date ASC,
                    closing_date ASC,
                    id DESC
                LIMIT 8
                """,
                (latest_daily_date,),
            ).fetchall())

        if "sharehub_announcements" in tables:
            recent_announcements = rows_to_list(conn.execute(
                """
                SELECT
                    announcement_id,
                    title,
                    symbol,
                    security_name,
                    icon_url,
                    subtitle,
                    announcement_date,
                    attachment_url,
                    news_url,
                    source,
                    category,
                    type
                FROM sharehub_announcements
                ORDER BY announcement_date DESC, announcement_id DESC
                LIMIT 8
                """
            ).fetchall())

        if "dividend" in tables:
            proposed_dividends = rows_to_list(conn.execute(
                """
                WITH ranked AS (
                    SELECT
                        d.symbol,
                        d.fiscal_year,
                        d.cash_dividend,
                        d.bonus_share,
                        d.right_share,
                        d.fetched_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY d.symbol
                            ORDER BY COALESCE(d.fetched_at, '') DESC, COALESCE(d.fiscal_year, '') DESC
                        ) AS rn
                    FROM dividend d
                    WHERE COALESCE(d.cash_dividend, '') <> ''
                       OR COALESCE(d.bonus_share, '') <> ''
                       OR COALESCE(d.right_share, '') <> ''
                )
                SELECT
                    ranked.symbol,
                    COALESCE(ac.company_name, ranked.symbol) AS company_name,
                    ranked.fiscal_year,
                    ranked.cash_dividend,
                    ranked.bonus_share,
                    ranked.right_share,
                    ranked.fetched_at
                FROM ranked
                LEFT JOIN about_company ac ON ac.symbol = ranked.symbol
                WHERE ranked.rn = 1
                ORDER BY
                    CASE WHEN COALESCE(ranked.cash_dividend, '') <> '' THEN 0 ELSE 1 END,
                    CASE WHEN COALESCE(ranked.bonus_share, '') <> '' THEN 0 ELSE 1 END,
                    ranked.fetched_at DESC,
                    ranked.symbol ASC
                LIMIT 12
                """
            ).fetchall())

        latest_floor_date = get_table_max_date(conn, "floor_sheet")
        if latest_floor_date:
            bulk_transactions = rows_to_list(conn.execute(
                """
                SELECT
                    fs.symbol,
                    fs.date,
                    fs.buyer_broker,
                    fs.seller_broker,
                    fs.quantity,
                    fs.rate,
                    fs.amount,
                    COALESCE(ac.company_name, fs.symbol) AS company_name
                FROM floor_sheet fs
                LEFT JOIN about_company ac ON ac.symbol = fs.symbol
                WHERE fs.date = ?
                  AND COALESCE(fs.quantity, 0) > 5000
                ORDER BY fs.amount DESC, fs.quantity DESC, fs.symbol ASC
                LIMIT 20
                """,
                (latest_floor_date,),
            ).fetchall())

        latest_sector_date = conn.execute(
            "SELECT MAX(date) FROM sector_index"
        ).fetchone()[0]
        previous_sector_date = conn.execute(
            "SELECT MAX(date) FROM sector_index WHERE date < ?",
            (latest_sector_date,),
        ).fetchone()[0] if latest_sector_date else None
        sectors = rows_to_list(conn.execute(
            """
            SELECT
                current.sector,
                current.value,
                previous.value AS previous_value,
                ROUND(current.value - COALESCE(previous.value, current.value), 4) AS point_change,
                ROUND(((current.value - COALESCE(previous.value, current.value)) / NULLIF(previous.value, 0)) * 100.0, 4) AS change_percent
            FROM sector_index current
            LEFT JOIN sector_index previous
                ON previous.sector = current.sector AND previous.date = ?
            WHERE current.date = ?
            ORDER BY current.sector ASC
            """,
            (previous_sector_date, latest_sector_date)
        ).fetchall()) if latest_sector_date else []

        sig_date = conn.execute(
            "SELECT MAX(date) FROM signals"
        ).fetchone()[0]
        breakdown = rows_to_list(conn.execute("""
            SELECT signal, COUNT(*) AS count
            FROM signals
            WHERE date = ?
            GROUP BY signal
        """, (sig_date,)).fetchall()) if sig_date else []

        return jsonify({
            "as_of": latest_daily_date,
            "market_summary": dict(latest_market_summary) if latest_market_summary else {},
            "derived_market_summary": market_totals,
            "breadth": breadth,
            "sector_indices": sectors,
            "sector_overview": sector_leaders,
            "sector_index_date": latest_sector_date,
            "top_gainers": top_gainers,
            "top_losers": top_losers,
            "top_turnover": top_turnover,
            "top_traded_shares": top_traded,
            "top_transactions": top_transactions,
            "marquee_strip": marquee_strip,
            "recent_news": recent_news,
            "public_offerings": public_offerings,
            "bulk_transactions": bulk_transactions,
            "recent_announcements": recent_announcements,
            "proposed_dividends": proposed_dividends,
            "signal_date": sig_date,
            "signal_breakdown": breakdown,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/home/news")
def home_news():
    conn = get_db()
    try:
        limit = max(1, min(int(request.args.get("limit", 50)), 200))
        offset = max(0, int(request.args.get("offset", 0)))
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "sharehub_news_feed" not in tables:
            return jsonify({"items": [], "count": 0})

        rows = rows_to_list(conn.execute(
            """
            SELECT
                post_id,
                slug,
                profile_name,
                profile_image_url,
                user_name,
                is_profile_verified,
                title,
                summary,
                media_url,
                launch_url,
                time_ago,
                published_date
            FROM sharehub_news_feed
            WHERE COALESCE(media_type, 'News') = 'News'
            ORDER BY published_date DESC, post_id DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        ).fetchall())
        count = conn.execute(
            "SELECT COUNT(*) FROM sharehub_news_feed WHERE COALESCE(media_type, 'News') = 'News'"
        ).fetchone()[0]
        return jsonify({"items": rows, "count": count, "limit": limit, "offset": offset})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/home/public-offerings")
def home_public_offerings():
    conn = get_db()
    try:
        limit = max(1, min(int(request.args.get("limit", 100)), 250))
        offset = max(0, int(request.args.get("offset", 0)))
        offering_type = (request.args.get("type") or "").strip().upper()
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "sharehub_public_offerings" not in tables:
            return jsonify({"items": [], "count": 0})

        where = ""
        params = []
        if offering_type:
            where = "WHERE UPPER(COALESCE(offering_type, '')) LIKE ?"
            params.append(f"%{offering_type}%")

        rows = rows_to_list(conn.execute(
            f"""
            SELECT
                detail_slug,
                symbol,
                company_name,
                ratio,
                units,
                price,
                opening_date,
                closing_date,
                status,
                detail_url,
                offering_type
            FROM sharehub_public_offerings
            {where}
            ORDER BY
                CASE
                    WHEN closing_date IS NOT NULL
                     AND closing_date >= COALESCE((SELECT MAX(date) FROM daily_price), closing_date)
                    THEN 0 ELSE 1
                END,
                opening_date ASC,
                closing_date ASC,
                id DESC
            LIMIT ? OFFSET ?
            """,
            (*params, limit, offset),
        ).fetchall())
        count = conn.execute(
            f"SELECT COUNT(*) FROM sharehub_public_offerings {where}",
            params,
        ).fetchone()[0]
        return jsonify({"items": rows, "count": count, "limit": limit, "offset": offset, "type": offering_type or None})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/home/bulk-transactions")
def home_bulk_transactions():
    conn = get_db()
    try:
        limit = max(1, min(int(request.args.get("limit", 100)), 300))
        offset = max(0, int(request.args.get("offset", 0)))
        target_date = request.args.get("date") or get_table_max_date(conn, "floor_sheet")
        rows = rows_to_list(conn.execute(
            """
            SELECT
                fs.symbol,
                fs.date,
                fs.buyer_broker,
                fs.seller_broker,
                fs.quantity,
                fs.rate,
                fs.amount,
                COALESCE(ac.company_name, fs.symbol) AS company_name
            FROM floor_sheet fs
            LEFT JOIN about_company ac ON ac.symbol = fs.symbol
            WHERE fs.date = ?
              AND COALESCE(fs.quantity, 0) > 5000
            ORDER BY fs.amount DESC, fs.quantity DESC, fs.symbol ASC
            LIMIT ? OFFSET ?
            """,
            (target_date, limit, offset),
        ).fetchall()) if target_date else []
        count = conn.execute(
            "SELECT COUNT(*) FROM floor_sheet WHERE date = ? AND COALESCE(quantity, 0) > 5000",
            (target_date,),
        ).fetchone()[0] if target_date else 0
        return jsonify({"items": rows, "count": count, "date": target_date, "limit": limit, "offset": offset})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/home/announcements")
def home_announcements():
    conn = get_db()
    try:
        limit = max(1, min(int(request.args.get("limit", 50)), 200))
        offset = max(0, int(request.args.get("offset", 0)))
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "sharehub_announcements" not in tables:
            return jsonify({"items": [], "count": 0})

        rows = rows_to_list(conn.execute(
            """
            SELECT
                announcement_id,
                title,
                symbol,
                security_name,
                icon_url,
                subtitle,
                announcement_date,
                attachment_url,
                news_url,
                source,
                category,
                type
            FROM sharehub_announcements
            ORDER BY announcement_date DESC, announcement_id DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        ).fetchall())
        count = conn.execute("SELECT COUNT(*) FROM sharehub_announcements").fetchone()[0]
        return jsonify({"items": rows, "count": count, "limit": limit, "offset": offset})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/home/dividends")
def home_dividends():
    conn = get_db()
    try:
        limit = max(1, min(int(request.args.get("limit", 100)), 250))
        offset = max(0, int(request.args.get("offset", 0)))
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "dividend" not in tables:
            return jsonify({"items": [], "count": 0})

        ranked_cte = """
            WITH ranked AS (
                SELECT
                    d.symbol,
                    d.fiscal_year,
                    d.cash_dividend,
                    d.bonus_share,
                    d.right_share,
                    d.fetched_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY d.symbol
                        ORDER BY COALESCE(d.fetched_at, '') DESC, COALESCE(d.fiscal_year, '') DESC
                    ) AS rn
                FROM dividend d
                WHERE COALESCE(d.cash_dividend, '') <> ''
                   OR COALESCE(d.bonus_share, '') <> ''
                   OR COALESCE(d.right_share, '') <> ''
            )
        """
        rows = rows_to_list(conn.execute(
            f"""
            {ranked_cte}
            SELECT
                ranked.symbol,
                COALESCE(ac.company_name, ranked.symbol) AS company_name,
                ranked.fiscal_year,
                ranked.cash_dividend,
                ranked.bonus_share,
                ranked.right_share,
                ranked.fetched_at
            FROM ranked
            LEFT JOIN about_company ac ON ac.symbol = ranked.symbol
            WHERE ranked.rn = 1
            ORDER BY
                CASE WHEN COALESCE(ranked.cash_dividend, '') <> '' THEN 0 ELSE 1 END,
                CASE WHEN COALESCE(ranked.bonus_share, '') <> '' THEN 0 ELSE 1 END,
                ranked.fetched_at DESC,
                ranked.symbol ASC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        ).fetchall())
        count = conn.execute(
            f"""
            {ranked_cte}
            SELECT COUNT(*) FROM ranked WHERE rn = 1
            """
        ).fetchone()[0]
        return jsonify({"items": rows, "count": count, "limit": limit, "offset": offset})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/market/breadth/<string:category>")
def market_breadth_category(category):
    conn = get_db()
    try:
        normalized = category.strip().lower()
        if normalized not in MARKET_BREADTH_CATEGORIES:
            return jsonify({
                "error": f"Unknown breadth category '{category}'.",
                "available": sorted(MARKET_BREADTH_CATEGORIES),
            }), 404

        latest_daily_date = get_table_max_date(conn, "daily_price")
        previous_daily_date = None
        if latest_daily_date:
            previous_daily_date = conn.execute(
                "SELECT MAX(date) FROM daily_price WHERE date < ?",
                (latest_daily_date,),
            ).fetchone()[0]

        rows = build_market_breadth_dataset(conn, latest_daily_date, previous_daily_date)
        def include_row(row):
            change = float(row.get("change") or 0)
            change_percent = float(row.get("change_percent") or 0)
            ltp = float(row.get("ltp") or 0)
            high = float(row.get("high") or 0)
            low = float(row.get("low") or 0)
            if normalized == "advanced":
                return change > 0
            if normalized == "declined":
                return change < 0
            if normalized == "unchanged":
                return change == 0
            if normalized == "positive-circuit":
                return change_percent >= 9.95 and ltp >= high
            if normalized == "negative-circuit":
                return change_percent <= -9.95 and ltp <= low
            return False

        filtered = [row for row in rows if include_row(row)]

        if normalized in {"declined", "negative-circuit"}:
            filtered.sort(key=lambda item: (item.get("change_percent") or 0))
        elif normalized == "unchanged":
            filtered.sort(key=lambda item: (item.get("symbol") or ""))
        else:
            filtered.sort(key=lambda item: (item.get("change_percent") or 0), reverse=True)

        return jsonify({
            "category": normalized,
            "as_of": latest_daily_date,
            "previous_date": previous_daily_date,
            "count": len(filtered),
            "rows": filtered,
            "columns": [
                "symbol",
                "company_name",
                "sector",
                "ltp",
                "change",
                "change_percent",
                "open",
                "high",
                "low",
                "volume",
                "turnover",
                "ltv",
                "previous_close",
            ],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ── SIGNALS LIST ──────────────────────────────────────────────────────────────
@app.route("/api/signals")
def get_signals():
    conn = get_db()
    try:
        date   = request.args.get("date")
        signal = request.args.get("signal", "").upper()
        limit  = int(request.args.get("limit", 100))

        if not date:
            date = conn.execute("SELECT MAX(date) FROM signals").fetchone()[0]

        query  = "SELECT * FROM signals WHERE date = ?"
        params = [date]
        if signal in ("BUY", "SELL", "HOLD"):
            query += " AND signal = ?"; params.append(signal)
        query += " ORDER BY total_score DESC LIMIT ?"
        params.append(limit)

        rows = rows_to_list(conn.execute(query, params).fetchall())
        return jsonify({"date": date, "count": len(rows), "signals": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ── NEPSE SIGNALS FOR ONE SYMBOL ──────────────────────────────────────────────
@app.route("/api/signals/<symbol>")
def get_nepse_signals_for_symbol(symbol):
    conn = get_db()
    try:
        symbol = symbol.upper()
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        if "nepse_signals" not in tables:
            return jsonify({"error": "nepse_signals table not found"}), 404

        row = conn.execute("""
            SELECT * FROM nepse_signals
            WHERE  symbol = ?
            ORDER  BY date DESC LIMIT 1
        """, (symbol,)).fetchone()

        if not row:
            return jsonify({"error": f"No nepse signals for '{symbol}'"}), 404

        return jsonify({"symbol": symbol, "signals": dict(row)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ── SINGLE STOCK ──────────────────────────────────────────────────────────────
@app.route("/api/stock/<symbol>")
def get_stock(symbol):
    conn = get_db()
    try:
        symbol = symbol.upper()
        prices = rows_to_list(conn.execute("""
             SELECT c.date,
                    c.open,
                    c.high,
                    c.low,
                    c.close,
                    c.volume,
                    c.price_change_pct,
                    c.volume_ratio,
                    c.atr14,
                    c.market_condition
             FROM   clean_price_history c
             WHERE  c.symbol = ?
             ORDER  BY c.date DESC
        """, (symbol,)).fetchall())
        if not prices:
            prices = rows_to_list(conn.execute("""
                 SELECT p.date,
                        p.open,
                        p.high,
                        p.low,
                        p.close,
                        p.volume,
                        NULL AS price_change_pct,
                        NULL AS volume_ratio,
                        NULL AS atr14,
                        NULL AS market_condition
                 FROM   price_history p
                 WHERE  p.symbol = ?
                 ORDER  BY p.date DESC
            """, (symbol,)).fetchall())
        prices.reverse()

        latest_signal = conn.execute("""
            SELECT * FROM signals
            WHERE  symbol = ?
            ORDER  BY date DESC LIMIT 1
        """, (symbol,)).fetchone()

        company = next(
            (row for row in build_companies_catalog(conn) if row["symbol"] == symbol),
            None,
        )

        return jsonify({
            "symbol":        symbol,
            "company":       company if company else {},
            "price_history": prices,
            "latest_signal": dict(latest_signal) if latest_signal else {},
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ── OHLCV ─────────────────────────────────────────────────────────────────────
@app.route("/api/stock/<symbol>/ohlcv")
def get_ohlcv(symbol):
    conn = get_db()
    try:
        symbol = symbol.upper()
        rows = conn.execute("""
            SELECT c.date,
                   c.open,
                   c.high,
                   c.low,
                   c.close,
                   c.volume
            FROM   clean_price_history c
            WHERE  c.symbol = ?
            ORDER  BY c.date DESC
        """, (symbol,)).fetchall()
        rows = list(reversed(rows))

        return jsonify({
            "symbol": symbol,
            "dates":  [r["date"]   for r in rows],
            "open":   [r["open"]   for r in rows],
            "high":   [r["high"]   for r in rows],
            "low":    [r["low"]    for r in rows],
            "close":  [r["close"]  for r in rows],
            "volume": [r["volume"] for r in rows],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ── PRICE ALIAS ───────────────────────────────────────────────────────────────
@app.route("/api/price/<symbol>")
def get_price(symbol):
    conn = get_db()
    try:
        symbol = symbol.upper()
        rows = conn.execute("""
             SELECT c.date,
                    c.open,
                    c.high,
                    c.low,
                    c.close,
                    c.volume,
                    c.market_condition
             FROM   clean_price_history c
             WHERE  c.symbol = ?
             ORDER  BY c.date DESC
        """, (symbol,)).fetchall()

        if not rows:
            rows = conn.execute("""
                 SELECT p.date,
                        p.open,
                        p.high,
                        p.low,
                        p.close,
                        p.volume,
                        NULL AS market_condition
                 FROM   price_history p
                 WHERE  p.symbol = ?
                 ORDER  BY p.date DESC
            """, (symbol,)).fetchall()

        if not rows:
            return jsonify({"error": f"Symbol '{symbol}' not found"}), 404

        data = [
            {"date": r[0], "open": r[1], "high": r[2], "low": r[3],
             "close": r[4], "volume": r[5], "market_condition": r[6]}
            for r in reversed(rows)
        ]
        return jsonify({"symbol": symbol, "days": len(data), "prices": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ── BACKTEST RESULTS ──────────────────────────────────────────────────────────
@app.route("/api/backtest")
def get_backtest():
    conn = get_db()
    try:
        sort_col = request.args.get("sort", "total_return_pct")
        order    = "ASC" if request.args.get("order", "desc").lower() == "asc" else "DESC"
        limit    = int(request.args.get("limit", 50))

        allowed_cols = {"total_return_pct", "sharpe_ratio", "win_rate_pct",
                        "max_drawdown_pct", "total_trades", "avg_return_pct"}
        if sort_col not in allowed_cols:
            sort_col = "total_return_pct"

        rows = rows_to_list(conn.execute(f"""
            SELECT symbol_filter AS symbol, start_date, end_date,
                   total_trades, win_rate_pct, avg_return_pct,
                   total_return_pct, max_drawdown_pct, sharpe_ratio,
                   starting_capital, ending_capital
            FROM   backtest_summary
            ORDER  BY {sort_col} {order}
            LIMIT  ?
        """, (limit,)).fetchall())

        return jsonify({"count": len(rows), "results": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/backtest/<symbol>")
def get_backtest_symbol(symbol):
    conn = get_db()
    try:
        symbol = symbol.upper()
        summary = conn.execute("""
            SELECT * FROM backtest_summary
            WHERE  symbol_filter = ?
            ORDER  BY created_at DESC LIMIT 1
        """, (symbol,)).fetchone()

        trades = rows_to_list(conn.execute("""
            SELECT * FROM backtest_trades
            WHERE  symbol = ?
            ORDER  BY buy_date ASC
        """, (symbol,)).fetchall())

        return jsonify({
            "symbol":  symbol,
            "summary": dict(summary) if summary else {},
            "trades":  trades,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ── SCREENER ──────────────────────────────────────────────────────────────────
@app.route("/api/screener")
def screener():
    conn = get_db()
    try:
        signal    = request.args.get("signal", "").upper()
        condition = request.args.get("market_condition", "").lower()
        min_rsi   = request.args.get("min_rsi",    type=float)
        max_rsi   = request.args.get("max_rsi",    type=float)
        min_vol   = request.args.get("min_volume_ratio", type=float)
        min_score = request.args.get("min_score",  type=int)
        limit     = int(request.args.get("limit", 50))

        sig_date = conn.execute("SELECT MAX(date) FROM signals").fetchone()[0]

        query  = "SELECT * FROM signals WHERE date = ?"
        params = [sig_date]

        if signal in ("BUY", "SELL", "HOLD"):
            query += " AND signal = ?";            params.append(signal)
        if condition in ("bull", "bear", "sideways"):
            query += " AND market_condition = ?";  params.append(condition)
        if min_rsi is not None:
            query += " AND rsi14 >= ?";            params.append(min_rsi)
        if max_rsi is not None:
            query += " AND rsi14 <= ?";            params.append(max_rsi)
        if min_vol is not None:
            query += " AND volume_ratio >= ?";     params.append(min_vol)
        if min_score is not None:
            query += " AND total_score >= ?";      params.append(min_score)

        query += " ORDER BY total_score DESC LIMIT ?"
        params.append(limit)

        rows = rows_to_list(conn.execute(query, params).fetchall())
        return jsonify({"date": sig_date, "count": len(rows), "results": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ── COMPANIES ─────────────────────────────────────────────────────────────────
@app.route("/api/companies")
def get_companies():
    conn = get_db()
    try:
        sector = request.args.get("sector", "")
        rows = build_companies_catalog(conn)
        if sector:
            rows = [row for row in rows if row.get("sector") == sector]
        return jsonify({"count": len(rows), "companies": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ── SECTORS ───────────────────────────────────────────────────────────────────
@app.route("/api/sectors")
def get_sectors():
    conn = get_db()
    try:
        latest = conn.execute("SELECT MAX(date) FROM sector_index").fetchone()[0]
        rows = rows_to_list(conn.execute(
            "SELECT sector, value FROM sector_index WHERE date = ? ORDER BY value DESC",
            (latest,)
        ).fetchall()) if latest else []
        return jsonify({"date": latest, "sectors": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ── OPTIMIZER ─────────────────────────────────────────────────────────────────
@app.route("/api/indices")
def get_indices():
    conn = get_db()
    try:
        latest_date = conn.execute("SELECT MAX(date) FROM sector_index").fetchone()[0]
        previous_date = conn.execute(
            "SELECT MAX(date) FROM sector_index WHERE date < ?",
            (latest_date,),
        ).fetchone()[0] if latest_date else None
        rows = rows_to_list(conn.execute(
            """
            SELECT
                current.sector AS name,
                current.date,
                current.value,
                previous.value AS previous_value,
                ROUND(current.value - COALESCE(previous.value, current.value), 4) AS point_change,
                ROUND(((current.value - COALESCE(previous.value, current.value)) / NULLIF(previous.value, 0)) * 100.0, 4) AS change_percent
            FROM sector_index current
            LEFT JOIN sector_index previous
                ON previous.sector = current.sector AND previous.date = ?
            WHERE current.date = ?
            ORDER BY CASE WHEN current.sector = 'NEPSE' THEN 0 ELSE 1 END, current.sector ASC
            """,
            (previous_date, latest_date),
        ).fetchall()) if latest_date else []
        return jsonify({
            "date": latest_date,
            "previous_date": previous_date,
            "count": len(rows),
            "indices": rows,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/indices/<path:index_name>/history")
def get_index_history(index_name):
    conn = get_db()
    try:
        index_name = index_name.strip()
        limit = max(30, min(int(request.args.get("limit", 600)), 5000))

        exists = conn.execute(
            "SELECT 1 FROM sector_index WHERE sector = ? LIMIT 1",
            (index_name,),
        ).fetchone()
        if not exists:
            return jsonify({"error": f"Unknown index '{index_name}'"}), 404

        history_desc = rows_to_list(conn.execute(
            """
            SELECT
                date,
                sector AS name,
                value
            FROM sector_index
            WHERE sector = ?
            ORDER BY date DESC
            LIMIT ?
            """,
            (index_name, limit),
        ).fetchall())
        history = list(reversed(history_desc))

        previous_value = None
        for row in history:
            value = row.get("value")
            row["previous_value"] = previous_value
            if previous_value in (None, 0) or value is None:
                row["point_change"] = None
                row["change_percent"] = None
            else:
                row["point_change"] = round(value - previous_value, 4)
                row["change_percent"] = round(((value - previous_value) / previous_value) * 100.0, 4)
            previous_value = value

        latest = history[-1] if history else None
        earliest = history[0] if history else None
        high_row = max(history, key=lambda item: float(item.get("value") or float("-inf"))) if history else None
        low_row = min(history, key=lambda item: float(item.get("value") or float("inf"))) if history else None

        period_change = None
        period_change_percent = None
        if earliest and latest and earliest.get("value") not in (None, 0) and latest.get("value") is not None:
            period_change = round(float(latest["value"]) - float(earliest["value"]), 4)
            period_change_percent = round((period_change / float(earliest["value"])) * 100.0, 4)

        return jsonify({
            "index": index_name,
            "count": len(history),
            "history": history,
            "latest": latest,
            "stats": {
                "from_date": earliest.get("date") if earliest else None,
                "to_date": latest.get("date") if latest else None,
                "latest_value": latest.get("value") if latest else None,
                "latest_change": latest.get("point_change") if latest else None,
                "latest_change_percent": latest.get("change_percent") if latest else None,
                "period_change": period_change,
                "period_change_percent": period_change_percent,
                "high_value": high_row.get("value") if high_row else None,
                "high_date": high_row.get("date") if high_row else None,
                "low_value": low_row.get("value") if low_row else None,
                "low_date": low_row.get("date") if low_row else None,
            },
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/optimizer/leaderboard")
def optimizer_leaderboard():
    conn = get_db()
    try:
        limit    = int(request.args.get("limit", 50))
        ind_type = request.args.get("indicator_type", "").lower()

        query  = "SELECT * FROM optimizer_best"
        params = []
        if ind_type in ("trend", "momentum", "volatility", "volume"):
            query += " WHERE indicator_type = ?"; params.append(ind_type)
        query += " ORDER BY composite_score DESC LIMIT ?"; params.append(limit)

        rows = rows_to_list(conn.execute(query, params).fetchall())
        return jsonify({"count": len(rows), "leaderboard": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/optimizer/<symbol>")
def optimizer_symbol(symbol):
    conn = get_db()
    try:
        symbol = symbol.upper()
        best = conn.execute(
            "SELECT * FROM optimizer_best WHERE symbol = ?", (symbol,)
        ).fetchone()
        all_results = rows_to_list(conn.execute("""
            SELECT indicator, indicator_type, total_trades, winning_trades,
                   winrate, profit_factor, avg_win_pct, avg_loss_pct,
                   max_drawdown, total_return_pct, consistency, composite_score
            FROM   optimizer_results
            WHERE  symbol = ?
            ORDER  BY composite_score DESC
        """, (symbol,)).fetchall())
        return jsonify({
            "symbol":      symbol,
            "best":        dict(best) if best else {},
            "all_results": all_results,
            "count":       len(all_results),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ── TRADING RULES ─────────────────────────────────────────────────────────────
@app.route("/api/rules/<symbol>")
def get_rules_for_symbol(symbol):
    conn = get_db()
    try:
        symbol    = symbol.upper()
        limit     = int(request.args.get("limit", 20))
        rule_type = request.args.get("type", "").lower()

        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        if "trading_rules" not in tables:
            return jsonify({"symbol": symbol, "count": 0, "rules": [],
                            "message": "Run analysis first to generate rules"})

        query  = "SELECT * FROM trading_rules WHERE symbol = ?"
        params = [symbol]
        if rule_type in ("single", "combination"):
            query += " AND rule_type = ?"; params.append(rule_type)
        query += " ORDER BY weighted_score DESC LIMIT ?"; params.append(limit)

        rows = rows_to_list(conn.execute(query, params).fetchall())
        return jsonify({"symbol": symbol, "count": len(rows), "rules": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/rules")
def get_rules_summary():
    conn = get_db()
    try:
        limit = int(request.args.get("limit", 50))
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        if "trading_rules" not in tables:
            return jsonify({"count": 0, "symbols": [],
                            "message": "trading_rules table not yet created"})

        total  = conn.execute("SELECT COUNT(*) FROM trading_rules").fetchone()[0]
        by_type = rows_to_list(conn.execute("""
            SELECT rule_type, COUNT(*) AS n FROM trading_rules GROUP BY rule_type
        """).fetchall())
        symbols = rows_to_list(conn.execute("""
            SELECT symbol, COUNT(*) AS rule_count,
                   MAX(weighted_score) AS top_score,
                   AVG(weighted_score) AS avg_score
            FROM   trading_rules
            GROUP  BY symbol
            ORDER  BY rule_count DESC LIMIT ?
        """, (limit,)).fetchall())

        return jsonify({
            "total_rules":  total,
            "by_type":      by_type,
            "symbols":      symbols,
            "symbol_count": len(symbols),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ── MARKET SUMMARY ────────────────────────────────────────────────────────────
@app.route("/api/market-summary")
def market_summary_simple():
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT * FROM market_summary ORDER BY date DESC LIMIT 1"
        ).fetchone()
        return jsonify({"market_summary": dict(row) if row else {}})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ── TOP MOVERS ────────────────────────────────────────────────────────────────
@app.route("/api/top-movers")
def top_movers():
    conn = get_db()
    try:
        latest_date = conn.execute(
            "SELECT MAX(date) FROM clean_price_history"
        ).fetchone()[0]

        if not latest_date:
            return jsonify({"date": None, "gainers": [], "losers": []}), 200

        gainers = rows_to_list(conn.execute("""
            SELECT symbol, close, price_change_pct, volume, market_condition
            FROM   clean_price_history
            WHERE  date = ? AND price_change_pct IS NOT NULL
            ORDER  BY price_change_pct DESC LIMIT 5
        """, (latest_date,)).fetchall())

        losers = rows_to_list(conn.execute("""
            SELECT symbol, close, price_change_pct, volume, market_condition
            FROM   clean_price_history
            WHERE  date = ? AND price_change_pct IS NOT NULL
            ORDER  BY price_change_pct ASC LIMIT 5
        """, (latest_date,)).fetchall())

        return jsonify({"date": latest_date, "gainers": gainers, "losers": losers})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ── INIT DATA ─────────────────────────────────────────────────────────────────
_init_status = {"status": "idle", "step": None, "error": None}

@app.route("/api/init-data", methods=["POST", "GET"])
def init_data():
    """Re-fetch all data. Runs in background. Poll /api/init-status for progress."""
    global _init_status
    if _init_status.get("status") == "running":
        return jsonify({"status": "already_running", "step": _init_status.get("step")})

    def run():
        global _init_status
        _init_status = {"status": "running", "step": "starting", "error": None}
        try:
            _init_status["step"] = "importing modules"
            # Ensure src is on path
            import sys, os
            src_dir = os.path.join(ROOT, "src")
            if src_dir not in sys.path:
                sys.path.insert(0, src_dir)

            from data.fetcher import create_tables, fetch_company_list, fetch_all_price_histories
            from data.cleaner import create_clean_table, clean_all_symbols
            from signals.nepse_signals import create_signals_table, calculate_all_signals

            _init_status["step"] = "create_tables"
            create_tables()
            _init_status["step"] = "fetch_company_list"
            fetch_company_list()
            _init_status["step"] = "fetch_all_price_histories"
            fetch_all_price_histories(max_workers=5)
            _init_status["step"] = "create_clean_table"
            create_clean_table()
            _init_status["step"] = "clean_all_symbols"
            clean_all_symbols(max_workers=5)
            _init_status["step"] = "create_signals_table"
            create_signals_table()
            _init_status["step"] = "calculate_all_signals"
            calculate_all_signals(max_workers=5)

            _init_status = {"status": "done", "step": "complete", "error": None}
            print("✅ Init data complete")
        except Exception as e:
            import traceback
            err = traceback.format_exc()
            _init_status = {"status": "error", "step": _init_status.get("step"), "error": str(e), "traceback": err}
            print(f"❌ Init data error at step '{_init_status['step']}': {e}")
            print(err)

    import threading
    threading.Thread(target=run, daemon=True).start()
    return jsonify({
        "status": "started",
        "message": "Data fetch started. Poll /api/init-status for live progress."
    })


@app.route("/api/init-status")
def init_status():
    """Check the status of a running init-data job."""
    return jsonify(_init_status)


# ── UPLOAD DB (one-time) ───────────────────────────────────────────────────────
@app.route("/api/upload-db", methods=["POST"])
def upload_db():
    """
    Upload a pre-populated SQLite database directly to the volume.
    Requires header: X-Upload-Token: nepse-upload-2026
    Usage: curl -X POST -H 'X-Upload-Token: nepse-upload-2026' \
                --data-binary @nepse.db \
                https://.../api/upload-db
    """
    if request.headers.get("X-Upload-Token") != "nepse-upload-2026":
        return jsonify({"error": "Unauthorized"}), 401

    from db import DB_PATH
    import shutil

    tmp_path = DB_PATH + ".tmp"
    try:
        data = request.get_data()
        if len(data) < 1024:
            return jsonify({"error": "File too small — did the upload work?"}), 400

        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        with open(tmp_path, "wb") as f:
            f.write(data)

        # Validate it's a real SQLite DB
        import sqlite3 as _sqlite3
        conn = _sqlite3.connect(tmp_path)
        companies = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
        price_rows = conn.execute("SELECT COUNT(*) FROM price_history").fetchone()[0]
        conn.close()

        shutil.move(tmp_path, DB_PATH)

        return jsonify({
            "status": "ok",
            "db_path": DB_PATH,
            "size_mb": round(len(data) / 1024 / 1024, 1),
            "companies": companies,
            "price_rows": price_rows,
        })
    except Exception as e:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        return jsonify({"error": str(e)}), 500


# ── BACKGROUND JOB TRACKER ────────────────────────────────────────────────────
import threading
_jobs: dict = {}
_job_lock = threading.Lock()

def _job_id(symbol: str) -> str:
    return f"backtest_{symbol.upper()}"


# ── RUN BACKTEST ──────────────────────────────────────────────────────────────
@app.route("/api/run-backtest", methods=["POST"])
def run_backtest_endpoint():
    try:
        body   = request.get_json(force=True) or {}
        symbol = str(body.get("symbol", "")).upper()
        if not symbol:
            return jsonify({"error": "symbol is required"}), 400

        sl   = float(body.get("stop_loss_pct",   DEFAULT_STOP_LOSS_PCT))
        tp   = float(body.get("take_profit_pct", DEFAULT_TAKE_PROFIT_PCT))
        hold = int(body.get("max_hold_days",     DEFAULT_MAX_HOLD_DAYS))

        job_id = _job_id(symbol)
        with _job_lock:
            if _jobs.get(job_id, {}).get("status") == "running":
                return jsonify({"status": "already_running", "job_id": job_id}), 409
            _jobs[job_id] = {"status": "running", "symbol": symbol,
                             "result": None, "error": None}

        def _run():
            try:
                from engine.scorer import run_full_pipeline
                cfg = {
                    "stop_loss_pct":     sl,
                    "take_profit_pct":   tp,
                    "max_hold_days":     hold,
                    "initial_capital":   DEFAULT_INITIAL_CAPITAL,
                    "position_size_pct": DEFAULT_POSITION_SIZE_PCT,
                }
                rules = run_full_pipeline(symbol, cfg, verbose=False)
                with _job_lock:
                    _jobs[job_id]["status"] = "done"
                    _jobs[job_id]["result"] = {
                        "rules_saved": len(rules),
                        "top_rule":    rules[0]["rule_name"] if rules else None,
                        "top_score":   rules[0]["weighted_score"] if rules else None,
                    }
            except Exception as e:
                with _job_lock:
                    _jobs[job_id]["status"] = "error"
                    _jobs[job_id]["error"]  = str(e)

        threading.Thread(target=_run, daemon=True).start()

        return jsonify({
            "status":  "running",
            "job_id":  job_id,
            "symbol":  symbol,
            "message": f"Full pipeline started for {symbol} "
                       f"(SL={sl}% TP={tp}% hold={hold}d). "
                       f"Poll GET /api/run-backtest/{symbol} for status.",
        }), 202

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/run-backtest/<symbol>")
def get_backtest_job_status(symbol):
    job_id = _job_id(symbol)
    with _job_lock:
        job = _jobs.get(job_id)
    if not job:
        return jsonify({"status": "not_found",
                        "message": f"No job found for {symbol.upper()}"}), 404
    return jsonify(job)


# ── BROKER ACCUMULATION ───────────────────────────────────────────────────────
@app.route("/api/broker/update", methods=["POST"])
def broker_update():
    """Trigger daily floor sheet fetch + broker_summary recompute (background)."""
    def _run():
        try:
            try:
                from src.data.floorsheet_pipeline import run_daily_update
            except ImportError:
                import importlib
                sys.path.insert(0, os.path.join(ROOT, "src", "data"))
                run_daily_update = importlib.import_module("floorsheet_pipeline").run_daily_update
            run_daily_update()
        except Exception as e:
            import traceback
            print(f"[broker/update] ERROR: {e}")
            traceback.print_exc()
    import threading
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "started", "message": "Daily floor sheet update running in background"})


@app.route("/api/floorsheet/<symbol>")
def floorsheet_symbol(symbol):
    days = int(request.args.get("days", 30))
    sym = symbol.upper()
    page, limit, offset = parse_pagination()
    conn = get_db()
    try:
        if not floor_sheet_table_exists(conn):
            return jsonify({"error": "floor_sheet table not found"}), 404

        since, latest_date, _, _ = resolve_date_window(conn, "floor_sheet", symbol=sym, days=days)
        if not since:
            return jsonify({"error": f"No floorsheet data for '{sym}'"}), 404

        end_date = request.args.get("endDate", "").strip() or latest_date
        total_count = conn.execute(
            "SELECT COUNT(*) FROM floor_sheet WHERE symbol=? AND date BETWEEN ? AND ?",
            (sym, since, end_date)
        ).fetchone()[0]
        dates_count = conn.execute(
            "SELECT COUNT(DISTINCT date) FROM floor_sheet WHERE symbol=? AND date BETWEEN ? AND ?",
            (sym, since, end_date)
        ).fetchone()[0]

        trades = normalize_floorsheet_trade_rows(rows_to_list(conn.execute("""
            SELECT id, date, symbol, buyer_broker, seller_broker, quantity, rate, amount, fetched_at
            FROM floor_sheet
            WHERE symbol=? AND date BETWEEN ? AND ?
            ORDER BY date DESC, id DESC
            LIMIT ? OFFSET ?
        """, (sym, since, end_date, limit, offset)).fetchall()))

        return jsonify({
            "symbol": sym,
            "days": days,
            "since": since,
            "end_date": end_date,
            "latest_date": latest_date,
            "dates_count": dates_count,
            "page": page,
            "limit": limit,
            "count": len(trades),
            "total_count": total_count,
            "has_more": offset + len(trades) < total_count,
            "trades": trades,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/floorsheet/<symbol>/<date_str>")
def floorsheet_symbol_by_date(symbol, date_str):
    sym = symbol.upper()
    page, limit, offset = parse_pagination(default_limit=2000, max_limit=50000)
    conn = get_db()
    try:
        if not floor_sheet_table_exists(conn):
            return jsonify({"error": "floor_sheet table not found"}), 404

        total_count = conn.execute(
            "SELECT COUNT(*) FROM floor_sheet WHERE symbol=? AND date=?",
            (sym, date_str)
        ).fetchone()[0]
        if total_count == 0:
            return jsonify({"error": f"No floorsheet data for '{sym}' on '{date_str}'"}), 404

        trades = normalize_floorsheet_trade_rows(rows_to_list(conn.execute("""
            SELECT id, date, symbol, buyer_broker, seller_broker, quantity, rate, amount, fetched_at
            FROM floor_sheet
            WHERE symbol=? AND date=?
            ORDER BY id ASC
            LIMIT ? OFFSET ?
        """, (sym, date_str, limit, offset)).fetchall()))

        return jsonify({
            "symbol": sym,
            "date": date_str,
            "page": page,
            "limit": limit,
            "count": len(trades),
            "total_count": total_count,
            "has_more": offset + len(trades) < total_count,
            "trades": trades,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/floorsheet/date/<date_str>")
def floorsheet_by_date(date_str):
    page, limit, offset = parse_pagination()
    conn = get_db()
    try:
        if not floor_sheet_table_exists(conn):
            return jsonify({"error": "floor_sheet table not found"}), 404

        total_count = conn.execute(
            "SELECT COUNT(*) FROM floor_sheet WHERE date=?", (date_str,)
        ).fetchone()[0]
        if total_count == 0:
            return jsonify({"error": f"No floorsheet data for '{date_str}'"}), 404

        symbol_count = conn.execute(
            "SELECT COUNT(DISTINCT symbol) FROM floor_sheet WHERE date=?", (date_str,)
        ).fetchone()[0]
        trades = normalize_floorsheet_trade_rows(rows_to_list(conn.execute("""
            SELECT id, date, symbol, buyer_broker, seller_broker, quantity, rate, amount, fetched_at
            FROM floor_sheet
            WHERE date=?
            ORDER BY symbol ASC, id ASC
            LIMIT ? OFFSET ?
        """, (date_str, limit, offset)).fetchall()))

        return jsonify({
            "date": date_str,
            "symbol_count": symbol_count,
            "page": page,
            "limit": limit,
            "count": len(trades),
            "total_count": total_count,
            "has_more": offset + len(trades) < total_count,
            "trades": trades,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/floorsheet/latest")
def floorsheet_latest():
    page, limit, offset = parse_pagination()
    conn = get_db()
    try:
        if not floor_sheet_table_exists(conn):
            return jsonify({"error": "floor_sheet table not found"}), 404

        latest_date = get_latest_floor_sheet_date(conn)
        if not latest_date:
            return jsonify({"error": "No floorsheet data available"}), 404

        total_count = conn.execute(
            "SELECT COUNT(*) FROM floor_sheet WHERE date=?", (latest_date,)
        ).fetchone()[0]
        symbol_count = conn.execute(
            "SELECT COUNT(DISTINCT symbol) FROM floor_sheet WHERE date=?", (latest_date,)
        ).fetchone()[0]
        trades = normalize_floorsheet_trade_rows(rows_to_list(conn.execute("""
            SELECT id, date, symbol, buyer_broker, seller_broker, quantity, rate, amount, fetched_at
            FROM floor_sheet
            WHERE date=?
            ORDER BY symbol ASC, id ASC
            LIMIT ? OFFSET ?
        """, (latest_date, limit, offset)).fetchall()))

        return jsonify({
            "date": latest_date,
            "symbol_count": symbol_count,
            "page": page,
            "limit": limit,
            "count": len(trades),
            "total_count": total_count,
            "has_more": offset + len(trades) < total_count,
            "trades": trades,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/broker/summary/<symbol>")
def broker_summary_route(symbol):
    """Broker accumulation data for a stock. ?days=30&top=10&broker=<id>"""
    days   = int(request.args.get("days",  30))
    top_n  = int(request.args.get("top",   10))
    broker = request.args.get("broker")
    sym    = symbol.upper()
    conn   = get_db()
    try:
        since, latest_date, _, _ = resolve_date_window(conn, "broker_summary", symbol=sym, days=days)
        if not since:
            return jsonify({"error": f"No broker summary data for '{sym}'"}), 404

        end_date = request.args.get("endDate", "").strip() or latest_date

        if broker:
            history = rows_to_list(conn.execute("""
                SELECT date, broker, buy_qty, sell_qty, net_qty,
                       buy_amount, sell_amount, net_amount,
                       trades_as_buyer, trades_as_seller
                FROM broker_summary
                WHERE symbol=? AND broker=? AND date BETWEEN ? AND ?
                ORDER BY date DESC
            """, (sym, int(broker), since, end_date)).fetchall())
            return jsonify({"symbol": sym, "broker": int(broker), "since": since, "end_date": end_date, "history": history})

        broker_rows = rows_to_list(conn.execute("""
            SELECT broker,
                   SUM(buy_qty) AS buy_qty,
                   SUM(sell_qty) AS sell_qty,
                   SUM(net_qty) AS net_qty,
                   SUM(buy_amount) AS buy_amount,
                   SUM(sell_amount) AS sell_amount,
                   SUM(net_amount) AS net_amount,
                   SUM(trades_as_buyer) AS buy_trans,
                   SUM(trades_as_seller) AS sell_trans
            FROM broker_summary
            WHERE symbol=? AND date BETWEEN ? AND ?
            GROUP BY broker
        """, (sym, since, end_date)).fetchall())

        total_buy_qty = sum(float(row["buy_qty"] or 0) for row in broker_rows)
        total_sell_qty = sum(float(row["sell_qty"] or 0) for row in broker_rows)

        brokers = []
        for row in broker_rows:
            buy_qty = float(row["buy_qty"] or 0)
            sell_qty = float(row["sell_qty"] or 0)
            buy_amount = float(row["buy_amount"] or 0)
            sell_amount = float(row["sell_amount"] or 0)
            net_qty = float(row["net_qty"] or 0)
            brokers.append({
                "broker": row["broker"],
                "buy_qty": buy_qty,
                "sell_qty": sell_qty,
                "net_qty": net_qty,
                "buy_amount": buy_amount,
                "sell_amount": sell_amount,
                "net_amount": float(row["net_amount"] or 0),
                "buy_trans": int(row["buy_trans"] or 0),
                "sell_trans": int(row["sell_trans"] or 0),
                "avg_buy": round(buy_amount / buy_qty, 4) if buy_qty else 0,
                "avg_sell": round(sell_amount / sell_qty, 4) if sell_qty else 0,
                "buy_qty_pct": round((buy_qty / total_buy_qty) * 100, 4) if total_buy_qty else 0,
                "sell_qty_pct": round((sell_qty / total_sell_qty) * 100, 4) if total_sell_qty else 0,
            })

        brokers.sort(key=lambda item: item["net_qty"], reverse=True)
        top_buyers = brokers[:top_n]
        top_sellers = sorted(brokers, key=lambda item: item["net_qty"])[:top_n]

        daily = rows_to_list(conn.execute("""
            SELECT date, SUM(buy_qty) AS total_buy_qty, SUM(sell_qty) AS total_sell_qty,
                   SUM(net_qty) AS net_qty, COUNT(DISTINCT broker) AS active_brokers
            FROM broker_summary
            WHERE symbol=? AND date BETWEEN ? AND ?
            GROUP BY date ORDER BY date ASC
        """, (sym, since, end_date)).fetchall())

        return jsonify({"symbol": sym, "days": days, "since": since, "end_date": end_date,
                        "top_buyers": top_buyers, "top_sellers": top_sellers, "brokers": brokers,
                        "daily_flow": daily})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/broker/fetch/<symbol>", methods=["POST"])
def broker_fetch_route(symbol):
    """
    Trigger a floorsheet backfill for a single symbol.
    POST /api/broker/fetch/NABIL?floor_start=2026-01-01&floor_end=2026-03-17
    Runs synchronously — may take several minutes for large symbols.
    """
    sym = symbol.upper()
    floor_start = request.args.get("floor_start", "2026-01-01")
    floor_end   = request.args.get("floor_end",   datetime.now().strftime("%Y-%m-%d"))

    try:
        import subprocess
        pipeline_path = os.path.join(ROOT, "src", "data", "merolagani_pipeline.py")
        result = subprocess.run(
            [
                sys.executable, pipeline_path,
                "--symbols", sym,
                "--floor-start", floor_start,
                "--floor-end",   floor_end,
                "--skip-ohlcv",
            ],
            capture_output=True, text=True, timeout=600,
            cwd=ROOT,
        )
        if result.returncode != 0:
            return jsonify({"symbol": sym, "status": "error",
                            "stderr": result.stderr[-2000:]}), 500
        return jsonify({"symbol": sym, "status": "ok",
                        "floor_start": floor_start, "floor_end": floor_end,
                        "stdout": result.stdout[-2000:]})
    except subprocess.TimeoutExpired:
        return jsonify({"symbol": sym, "status": "timeout",
                        "message": "Pipeline took >10 min. Check logs."}), 504
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/broker/leaderboard")
def broker_leaderboard():
    """Which brokers are accumulating the most? ?days=30&top=20"""
    days  = int(request.args.get("days", 30))
    top_n = int(request.args.get("top",  20))
    conn  = get_db()
    try:
        cutoff = conn.execute(
            "SELECT date FROM (SELECT DISTINCT date FROM broker_summary ORDER BY date DESC LIMIT ?) ORDER BY date ASC LIMIT 1",
            (days,)
        ).fetchone()
        since = cutoff[0] if cutoff else "2020-01-01"
        rows = rows_to_list(conn.execute("""
            SELECT broker, COUNT(DISTINCT symbol) AS stocks_accumulated,
                   SUM(net_qty) AS total_net_qty, SUM(net_amount) AS total_net_amount,
                   SUM(buy_qty) AS total_buy_qty, SUM(sell_qty) AS total_sell_qty
            FROM broker_summary WHERE date>=?
            GROUP BY broker ORDER BY total_net_qty DESC LIMIT ?
        """, (since, top_n)).fetchall())
        return jsonify({"since": since, "days": days, "leaderboard": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ── MAIN ──────────────────────────────────────────────────────────────────────
@app.route("/api/ml/status")
def ml_status():
    conn = get_db()
    try:
        freshness = get_data_freshness(conn)
        latest_signals_date = get_table_max_date(conn, "signals")
        latest_batch_date = latest_ml_prediction_batch_date(conn)
        registry_row = latest_model_registry_row(conn)
        status = "research" if latest_signals_date else "unavailable"
        active_model = None

        if registry_row:
            status = "model_ready"
            active_model = {
                "model_key": registry_row.get("model_key"),
                "model_type": registry_row.get("model_type"),
                "trained_through": registry_row.get("trained_through"),
                "created_at": registry_row.get("created_at"),
                "feature_set_version": "ml_feature_snapshot_v1",
                "label_key": registry_row.get("target"),
                "prediction_batch_date": latest_batch_date,
                "validation_metrics": {
                    "precision": float(registry_row.get("validation_precision") or 0),
                    "recall": float(registry_row.get("validation_recall") or 0),
                    "accuracy": float(registry_row.get("validation_accuracy") or 0),
                },
            }
        elif latest_signals_date:
            active_model = {
                "model_key": "heuristic_signal_stack",
                "model_type": "rules_heuristic",
                "trained_through": latest_signals_date,
                "created_at": datetime.now().isoformat(),
                "feature_set_version": "signals+nepse_signals",
                "label_key": "pending_ml_training",
            }

        return jsonify({
            "status": status,
            "active_model": active_model,
            "data_freshness": freshness,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/ml/export/latest", methods=["POST"])
def ml_export_latest():
    target = request.args.get("target", "label_up_5d_4pct")
    batch_date = request.args.get("date")
    model_key = request.args.get("model_key")
    try:
        from src.ml.prediction_export import export_latest_lightgbm_predictions

        result = export_latest_lightgbm_predictions(
            target=target,
            batch_date=batch_date,
            model_key=model_key,
        )
        return jsonify(result.__dict__)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ml/scores/latest")
@app.route("/api/ml/predictions/latest")
def ml_predictions_latest():
    conn = get_db()
    try:
        limit = int(request.args.get("limit", 25))
        signal = request.args.get("signal", "").upper().strip()
        model_key = request.args.get("model_key")
        batch_date = request.args.get("date")
        rows = fetch_model_prediction_rows(
            conn,
            batch_date=batch_date,
            model_key=model_key,
            signal=signal or None,
            limit=max(limit, 1),
        )
        if rows:
            items = [serialize_model_prediction(row) for row in rows]
            effective_model_key = rows[0].get("model_key")
            effective_batch_date = rows[0].get("batch_date")
        else:
            rows = fetch_heuristic_prediction_rows(conn, batch_date=batch_date, limit=max(limit, 1))
            items = [serialize_heuristic_prediction(row) for row in rows]
            if signal:
                items = [item for item in items if item["prediction"].upper() == signal]
            effective_model_key = "heuristic_signal_stack"
            effective_batch_date = rows[0]["date"] if rows else get_table_max_date(conn, "signals")

        return jsonify({
            "date": effective_batch_date,
            "model_key": effective_model_key,
            "items": items[:limit],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/ml/scores/<symbol>")
@app.route("/api/ml/predict/<symbol>")
def ml_predict_symbol(symbol):
    conn = get_db()
    try:
        date = request.args.get("date")
        model_key = request.args.get("model_key")
        rows = fetch_model_prediction_rows(conn, symbol=symbol, batch_date=date, model_key=model_key, limit=1)
        if rows:
            row = rows[0]
            prediction = serialize_model_prediction(row)
            return jsonify({
                "symbol": row.get("symbol"),
                "date": row.get("batch_date"),
                "model_key": row.get("model_key"),
                "target": row.get("target"),
                "prediction": prediction["prediction"],
                "probability": prediction["probability"],
                "threshold": prediction["threshold"],
                "confidence_band": prediction["confidence_band"],
                "top_drivers": prediction["top_drivers"],
                "features": prediction["features"],
                "snapshot": prediction["snapshot"],
                "validation_metrics": prediction["validation_metrics"],
            })

        rows = fetch_heuristic_prediction_rows(conn, symbol=symbol, batch_date=date, limit=1)
        if not rows:
            return jsonify({"error": f"No prediction payload for '{symbol.upper()}'"}), 404

        row = rows[0]
        prediction = serialize_heuristic_prediction(row)
        return jsonify({
            "symbol": row.get("symbol"),
            "date": row.get("date"),
            "model_key": "heuristic_signal_stack",
            "prediction": prediction["prediction"],
            "probability": prediction["probability"],
            "threshold": prediction["threshold"],
            "confidence_band": prediction["confidence_band"],
            "top_drivers": prediction["top_drivers"],
            "features": {
                "total_score": row.get("total_score"),
                "signal": row.get("signal"),
                "market_condition": row.get("market_condition"),
                "pump_score": row.get("pump_score"),
                "liquidity_spike": row.get("liquidity_spike"),
                "broker_accumulation": row.get("broker_accumulation"),
                "broker_distribution": row.get("broker_distribution"),
                "net_broker_flow": row.get("net_broker_flow"),
                "volatility_regime": row.get("volatility_regime"),
                "volume_score": row.get("volume_score"),
                "rsi_score": row.get("rsi_score"),
                "close": row.get("close"),
                "volume": row.get("volume"),
            },
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/ml/walk-forward")
def ml_walk_forward():
    conn = get_db()
    try:
        registry_row = latest_model_registry_row(conn)
        if registry_row and registry_row.get("meta_path") and os.path.exists(registry_row["meta_path"]):
            meta = json.loads(open(registry_row["meta_path"], "r", encoding="utf-8").read())
            windows = meta.get("walk_forward", [])
            aggregate = {
                "validation_precision": float((meta.get("validation_metrics") or {}).get("precision", 0)),
                "validation_recall": float((meta.get("validation_metrics") or {}).get("recall", 0)),
                "validation_accuracy": float((meta.get("validation_metrics") or {}).get("accuracy", 0)),
            }
            return jsonify({
                "model_key": registry_row.get("model_key"),
                "label_key": registry_row.get("target"),
                "windows": windows,
                "aggregate": aggregate,
                "as_of": latest_ml_prediction_batch_date(conn, registry_row.get("model_key")),
            })

        latest_signals_date = get_table_max_date(conn, "signals")
        return jsonify({
            "model_key": "heuristic_signal_stack",
            "label_key": "forward_return_gt_0",
            "windows": [],
            "aggregate": {},
            "as_of": latest_signals_date,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/research/broker-edge")
def broker_edge_research():
    conn = get_db()
    try:
        threshold = float(request.args.get("threshold", 0.6))
        windows = compute_broker_edge_windows(conn, threshold=threshold)
        as_of = get_table_max_date(conn, "broker_summary") or get_table_max_date(conn, "daily_price")
        return jsonify({
            "as_of": as_of,
            "windows": windows,
            "by_sector": [],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/scanners")
def scanners_index():
    conn = get_db()
    try:
        as_of = get_latest_snapshot_date(conn, "scanner_results_daily")
        counts = rows_to_list(
            conn.execute(
                """
                SELECT scanner_name, COUNT(*) AS count, MAX(score) AS top_score
                FROM scanner_results_daily
                WHERE date = ?
                GROUP BY scanner_name
                ORDER BY scanner_name
                """,
                (as_of,),
            ).fetchall()
        ) if as_of else []
        return jsonify({"as_of": as_of, "scanners": counts})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/scanners/<scanner_name>")
def scanner_results(scanner_name):
    conn = get_db()
    try:
        limit = int(request.args.get("limit", 200))
        sector = request.args.get("sector", "").strip() or None
        signal = request.args.get("signal", "").strip() or None
        date = request.args.get("date")
        scanner_name = scanner_name.strip().lower()
        if scanner_name not in DERIVED_SCANNERS:
            return jsonify({"error": f"Unknown scanner '{scanner_name}'"}), 404
        rows = fetch_latest_scanner_rows(
            conn,
            scanner_name=scanner_name,
            batch_date=date,
            limit=max(1, min(limit, 500)),
            sector=sector,
            signal=signal,
        )
        as_of = date or get_latest_snapshot_date(conn, "scanner_results_daily")
        return jsonify({"as_of": as_of, "scanner_name": scanner_name, "count": len(rows), "items": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/sector-rotation")
def sector_rotation():
    conn = get_db()
    try:
        date = request.args.get("date")
        as_of = get_latest_snapshot_date(conn, "sector_factor_snapshot", date)
        rows = rows_to_list(
            conn.execute(
                """
                SELECT *
                FROM sector_factor_snapshot
                WHERE date = ?
                ORDER BY rotation_score DESC, sector ASC
                """,
                (as_of,),
            ).fetchall()
        ) if as_of else []
        return jsonify({"as_of": as_of, "count": len(rows), "sectors": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/sectors/best")
def best_sectors():
    conn = get_db()
    try:
        date = request.args.get("date")
        limit = int(request.args.get("limit", 10))
        as_of = get_latest_snapshot_date(conn, "sector_factor_snapshot", date)
        rows = rows_to_list(
            conn.execute(
                """
                SELECT *
                FROM sector_factor_snapshot
                WHERE date = ?
                ORDER BY sector_strength_score DESC, rotation_score DESC, sector ASC
                LIMIT ?
                """,
                (as_of, max(1, min(limit, 50))),
            ).fetchall()
        ) if as_of else []
        return jsonify({"as_of": as_of, "count": len(rows), "sectors": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/support-resistance")
def support_resistance_scanner():
    conn = get_db()
    try:
        date = request.args.get("date")
        limit = int(request.args.get("limit", 200))
        as_of = get_latest_snapshot_date(conn, "support_resistance_levels", date)
        rows = rows_to_list(
            conn.execute(
                """
                SELECT srl.*, sfs.security_name, sfs.sector, sfs.close, sfs.change_pct,
                       sfs.signal_label, sfs.composite_score, sfs.risk_score
                FROM support_resistance_levels srl
                LEFT JOIN stock_factor_snapshot sfs
                  ON sfs.date = srl.date AND sfs.symbol = srl.symbol
                WHERE srl.date = ?
                ORDER BY sfs.support_resistance_score DESC, sfs.composite_score DESC, srl.symbol ASC
                LIMIT ?
                """,
                (as_of, max(1, min(limit, 500))),
            ).fetchall()
        ) if as_of else []
        return jsonify({"as_of": as_of, "count": len(rows), "items": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/support-resistance/<symbol>")
def support_resistance_symbol(symbol):
    conn = get_db()
    try:
        date = request.args.get("date")
        as_of = get_latest_snapshot_date(conn, "support_resistance_levels", date)
        row = conn.execute(
            """
            SELECT srl.*, sfs.security_name, sfs.sector, sfs.close, sfs.change_pct,
                   sfs.signal_label, sfs.composite_score, sfs.risk_score
            FROM support_resistance_levels srl
            LEFT JOIN stock_factor_snapshot sfs
              ON sfs.date = srl.date AND sfs.symbol = srl.symbol
            WHERE srl.date = ? AND srl.symbol = ?
            """,
            (as_of, symbol.upper()),
        ).fetchone() if as_of else None
        if not row:
            return jsonify({"error": f"No support/resistance snapshot for '{symbol.upper()}'"}), 404
        return jsonify({"as_of": as_of, "item": dict(row)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/broker-analytics/<symbol>")
def broker_analytics_symbol(symbol):
    conn = get_db()
    try:
        date = request.args.get("date")
        limit = int(request.args.get("limit", 100))
        mode = request.args.get("mode", "smart_money").strip().lower()
        order_map = {
            "smart_money": "smart_money_score DESC, net_qty DESC",
            "accumulation": "accumulation_score DESC, net_qty DESC",
            "distribution": "distribution_score DESC, net_qty ASC",
            "participation": "participation_pct DESC, gross_qty DESC",
        }
        order_sql = order_map.get(mode, order_map["smart_money"])
        as_of = get_latest_snapshot_date(conn, "broker_analytics_daily", date)
        rows = rows_to_list(
            conn.execute(
                f"""
                SELECT *
                FROM broker_analytics_daily
                WHERE date = ? AND symbol = ?
                ORDER BY {order_sql}, broker ASC
                LIMIT ?
                """,
                (as_of, symbol.upper(), max(1, min(limit, 500))),
            ).fetchall()
        ) if as_of else []
        summary = conn.execute(
            """
            SELECT
                COUNT(*) AS broker_count,
                SUM(buy_qty) AS total_buy_qty,
                SUM(sell_qty) AS total_sell_qty,
                SUM(net_qty) AS total_net_qty,
                SUM(gross_turnover) AS gross_turnover,
                AVG(smart_money_score) AS avg_smart_money_score
            FROM broker_analytics_daily
            WHERE date = ? AND symbol = ?
            """,
            (as_of, symbol.upper()),
        ).fetchone() if as_of else None
        return jsonify({
            "as_of": as_of,
            "symbol": symbol.upper(),
            "mode": mode,
            "summary": row_to_dict(summary),
            "count": len(rows),
            "items": rows,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/stock-profile/<symbol>")
def stock_profile_v2(symbol):
    conn = get_db()
    try:
        date = request.args.get("date")
        as_of = get_latest_snapshot_date(conn, "stock_factor_snapshot", date)
        snapshot = conn.execute(
            "SELECT * FROM stock_factor_snapshot WHERE date = ? AND symbol = ?",
            (as_of, symbol.upper()),
        ).fetchone() if as_of else None
        if not snapshot:
            return jsonify({"error": f"No stock factor snapshot for '{symbol.upper()}'"}), 404

        levels = conn.execute(
            "SELECT * FROM support_resistance_levels WHERE date = ? AND symbol = ?",
            (as_of, symbol.upper()),
        ).fetchone()
        top_buyers = rows_to_list(
            conn.execute(
                """
                SELECT broker, buy_qty, net_qty, buy_amount, smart_money_score, participation_pct
                FROM broker_analytics_daily
                WHERE date = ? AND symbol = ?
                ORDER BY accumulation_score DESC, net_qty DESC
                LIMIT 5
                """,
                (as_of, symbol.upper()),
            ).fetchall()
        )
        top_sellers = rows_to_list(
            conn.execute(
                """
                SELECT broker, sell_qty, net_qty, sell_amount, smart_money_score, participation_pct
                FROM broker_analytics_daily
                WHERE date = ? AND symbol = ?
                ORDER BY distribution_score DESC, net_qty ASC
                LIMIT 5
                """,
                (as_of, symbol.upper()),
            ).fetchall()
        )
        company = conn.execute("SELECT * FROM about_company WHERE symbol = ?", (symbol.upper(),)).fetchone()
        news_rows = rows_to_list(
            conn.execute(
                "SELECT * FROM news WHERE symbol = ? ORDER BY fetched_at DESC LIMIT 5",
                (symbol.upper(),),
            ).fetchall()
        ) if table_exists(conn, "news") else []
        dividend_rows = rows_to_list(
            conn.execute(
                "SELECT * FROM dividend WHERE symbol = ? ORDER BY fetched_at DESC LIMIT 5",
                (symbol.upper(),),
            ).fetchall()
        ) if table_exists(conn, "dividend") else []

        return jsonify({
            "as_of": as_of,
            "symbol": symbol.upper(),
            "snapshot": dict(snapshot),
            "support_resistance": row_to_dict(levels),
            "broker": {"top_buyers": top_buyers, "top_sellers": top_sellers},
            "company": row_to_dict(company),
            "news": news_rows,
            "dividends": dividend_rows,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/meroshare/accounts", methods=["GET", "POST"])
def meroshare_accounts_route():
    conn = get_db()
    try:
        ensure_meroshare_tables(conn)
        if request.method == "POST":
            body = request.get_json(force=True) or {}
            client_id = body.get("client_id")
            username = str(body.get("username", "")).strip()
            password = body.get("password")
            label = str(body.get("label", "")).strip() or None
            if client_id in (None, "") or not username or not password:
                return jsonify({"error": "client_id, username, and password are required"}), 400
            account = connect_meroshare_account(
                conn,
                client_id=int(client_id),
                username=username,
                password=str(password),
                label=label,
            )
            return jsonify({"account": sanitize_meroshare_account(account), "status": "connected"})
        rows = [sanitize_meroshare_account(row) for row in list_meroshare_accounts(conn)]
        return jsonify({"accounts": rows, "count": len(rows)})
    except MeroShareClientError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/meroshare/dps", methods=["GET"])
def meroshare_dps_route():
    try:
        client = MeroShareClient()
        rows = client.get_depository_participants()
        return jsonify({"participants": rows, "count": len(rows)})
    except MeroShareClientError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/meroshare/accounts/<int:account_id>", methods=["GET"])
def meroshare_account_detail_route(account_id):
    conn = get_db()
    try:
        ensure_meroshare_tables(conn)
        account = get_meroshare_account(conn, account_id)
        if not account:
            return jsonify({"error": "MeroShare account not found"}), 404
        return jsonify({"account": sanitize_meroshare_account(account)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/meroshare/accounts/<int:account_id>/sync", methods=["POST"])
def meroshare_account_sync_route(account_id):
    conn = get_db()
    try:
        ensure_meroshare_tables(conn)
        body = request.get_json(force=True) or {}
        password = body.get("password")
        include_transactions = bool(body.get("include_transactions", True))
        result = sync_meroshare_account(
            conn,
            account_id,
            password=str(password) if password else None,
            include_transactions=include_transactions,
        )
        if result.get("account"):
            result["account"] = sanitize_meroshare_account(result["account"])
        return jsonify(result)
    except MeroShareSessionExpired as e:
        return jsonify(
            {
                "error": "MeroShare session expired. Provide password to refresh the session.",
                "details": str(e),
            }
        ), 401
    except MeroShareClientError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/meroshare/accounts/<int:account_id>/holdings")
def meroshare_account_holdings_route(account_id):
    conn = get_db()
    try:
        ensure_meroshare_tables(conn)
        latest_only = str(request.args.get("latest_only", "true")).lower() not in {"0", "false", "no"}
        return jsonify(get_meroshare_holdings(conn, account_id, latest_only=latest_only))
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/meroshare/accounts/<int:account_id>/purchase-history")
def meroshare_account_purchase_history_route(account_id):
    conn = get_db()
    try:
        ensure_meroshare_tables(conn)
        return jsonify(get_meroshare_purchase_history(conn, account_id))
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/meroshare/accounts/<int:account_id>/transactions")
def meroshare_account_transactions_route(account_id):
    conn = get_db()
    try:
        ensure_meroshare_tables(conn)
        return jsonify(get_meroshare_transactions(conn, account_id))
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/portfolios/<int:portfolio_id>/import-meroshare/<int:account_id>", methods=["POST"])
def portfolio_import_meroshare_route(portfolio_id, account_id):
    conn = get_db()
    try:
        ensure_meroshare_tables(conn)
        return jsonify(import_meroshare_holdings_to_portfolio(conn, portfolio_id, account_id))
    except MeroShareClientError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/watchlists", methods=["GET", "POST"])
def watchlists_route():
    conn = get_db()
    try:
        if request.method == "POST":
            body = request.get_json(force=True) or {}
            name = str(body.get("name", "")).strip()
            if not name:
                return jsonify({"error": "name is required"}), 400
            conn.execute("INSERT INTO watchlists(name) VALUES (?)", (name,))
            conn.commit()
        rows = rows_to_list(
            conn.execute(
                """
                SELECT w.*, COUNT(wi.symbol) AS item_count
                FROM watchlists w
                LEFT JOIN watchlist_items wi
                  ON wi.watchlist_id = w.id
                GROUP BY w.id
                ORDER BY w.created_at DESC, w.id DESC
                """
            ).fetchall()
        )
        return jsonify({"watchlists": rows, "count": len(rows)})
    except sqlite3.IntegrityError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/watchlists/<int:watchlist_id>", methods=["GET", "POST", "DELETE"])
def watchlist_detail_route(watchlist_id):
    conn = get_db()
    try:
        if request.method == "DELETE":
            conn.execute("DELETE FROM watchlist_items WHERE watchlist_id = ?", (watchlist_id,))
            conn.execute("DELETE FROM watchlists WHERE id = ?", (watchlist_id,))
            conn.commit()
            return jsonify({"status": "deleted", "watchlist_id": watchlist_id})

        if request.method == "POST":
            body = request.get_json(force=True) or {}
            symbol = str(body.get("symbol", "")).upper().strip()
            notes = body.get("notes")
            if not symbol:
                return jsonify({"error": "symbol is required"}), 400
            conn.execute(
                """
                INSERT INTO watchlist_items(watchlist_id, symbol, notes)
                VALUES (?, ?, ?)
                ON CONFLICT(watchlist_id, symbol) DO UPDATE SET notes=excluded.notes
                """,
                (watchlist_id, symbol, notes),
            )
            conn.commit()

        header = conn.execute("SELECT * FROM watchlists WHERE id = ?", (watchlist_id,)).fetchone()
        if not header:
            return jsonify({"error": "watchlist not found"}), 404
        items = rows_to_list(
            conn.execute(
                """
                SELECT wi.*, sfs.security_name, sfs.sector, sfs.close, sfs.change_pct,
                       sfs.signal_label, sfs.composite_score
                FROM watchlist_items wi
                LEFT JOIN stock_factor_snapshot sfs
                  ON sfs.symbol = wi.symbol
                 AND sfs.date = (SELECT MAX(date) FROM stock_factor_snapshot)
                WHERE wi.watchlist_id = ?
                ORDER BY wi.added_at DESC, wi.symbol ASC
                """,
                (watchlist_id,),
            ).fetchall()
        )
        return jsonify({"watchlist": dict(header), "items": items, "count": len(items)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/watchlists/<int:watchlist_id>/items/<symbol>", methods=["DELETE"])
def watchlist_item_delete_route(watchlist_id, symbol):
    conn = get_db()
    try:
        conn.execute(
            "DELETE FROM watchlist_items WHERE watchlist_id = ? AND symbol = ?",
            (watchlist_id, symbol.upper()),
        )
        conn.commit()
        return jsonify({"status": "deleted", "watchlist_id": watchlist_id, "symbol": symbol.upper()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/portfolios", methods=["GET", "POST"])
def portfolios_route():
    conn = get_db()
    try:
        if request.method == "POST":
            body = request.get_json(force=True) or {}
            name = str(body.get("name", "")).strip()
            if not name:
                return jsonify({"error": "name is required"}), 400
            conn.execute("INSERT INTO portfolios(name) VALUES (?)", (name,))
            conn.commit()
        rows = rows_to_list(
            conn.execute(
                """
                SELECT p.*, COUNT(pp.symbol) AS position_count
                FROM portfolios p
                LEFT JOIN portfolio_positions pp
                  ON pp.portfolio_id = p.id
                GROUP BY p.id
                ORDER BY p.created_at DESC, p.id DESC
                """
            ).fetchall()
        )
        return jsonify({"portfolios": rows, "count": len(rows)})
    except sqlite3.IntegrityError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/portfolios/<int:portfolio_id>", methods=["GET", "POST", "DELETE"])
def portfolio_detail_route(portfolio_id):
    conn = get_db()
    try:
        if request.method == "DELETE":
            conn.execute("DELETE FROM portfolio_positions WHERE portfolio_id = ?", (portfolio_id,))
            conn.execute("DELETE FROM portfolios WHERE id = ?", (portfolio_id,))
            conn.commit()
            return jsonify({"status": "deleted", "portfolio_id": portfolio_id})

        if request.method == "POST":
            body = request.get_json(force=True) or {}
            symbol = str(body.get("symbol", "")).upper().strip()
            qty = float(body.get("qty", 0) or 0)
            avg_cost = float(body.get("avg_cost", 0) or 0)
            notes = body.get("notes")
            if not symbol:
                return jsonify({"error": "symbol is required"}), 400
            conn.execute(
                """
                INSERT INTO portfolio_positions(portfolio_id, symbol, qty, avg_cost, notes)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(portfolio_id, symbol)
                DO UPDATE SET qty=excluded.qty, avg_cost=excluded.avg_cost, notes=excluded.notes
                """,
                (portfolio_id, symbol, qty, avg_cost, notes),
            )
            conn.commit()

        header = conn.execute("SELECT * FROM portfolios WHERE id = ?", (portfolio_id,)).fetchone()
        if not header:
            return jsonify({"error": "portfolio not found"}), 404
        health = compute_portfolio_health(conn, portfolio_id)
        return jsonify({"portfolio": dict(header), **health})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/portfolios/<int:portfolio_id>/positions/<symbol>", methods=["DELETE"])
def portfolio_position_delete_route(portfolio_id, symbol):
    conn = get_db()
    try:
        conn.execute(
            "DELETE FROM portfolio_positions WHERE portfolio_id = ? AND symbol = ?",
            (portfolio_id, symbol.upper()),
        )
        conn.commit()
        return jsonify({"status": "deleted", "portfolio_id": portfolio_id, "symbol": symbol.upper()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/health-check/<int:portfolio_id>")
def portfolio_health_check_route(portfolio_id):
    conn = get_db()
    try:
        header = conn.execute("SELECT * FROM portfolios WHERE id = ?", (portfolio_id,)).fetchone()
        if not header:
            return jsonify({"error": "portfolio not found"}), 404
        return jsonify(compute_portfolio_health(conn, portfolio_id))
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


if __name__ == "__main__":
    print("=" * 55)
    print("NEPSE API Server")
    print("=" * 55)
    port = int(os.environ.get("PORT", API_PORT))
    app.run(host="0.0.0.0", debug=FLASK_DEBUG, port=port, use_reloader=False)
