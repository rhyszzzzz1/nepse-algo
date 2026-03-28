from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd

from src.db import get_db


DATE_FMT = "%Y-%m-%d"
SECTOR_NORMALIZATION = {
    "Commercial Banks": "Banking",
    "BANKINGIND": "Banking",
    "Development Banks": "Development Bank",
    "DEVBANKIND": "Development Bank",
    "Finance": "Finance",
    "FINANCEIND": "Finance",
    "Hotels And Tourism": "Hotels And Tourism",
    "HOTELIND": "Hotels And Tourism",
    "Hydro Power": "Hydro Power",
    "Hydropower": "Hydro Power",
    "HYDROPOWIND": "Hydro Power",
    "Investment": "Investment",
    "INVIDX": "Investment",
    "Life Insurance": "Life Insurance",
    "LIFEINSUIND": "Life Insurance",
    "Manufacturing And Processing": "Manufacturing And Processing",
    "MANUFACTUREIND": "Manufacturing And Processing",
    "Microfinance": "Microfinance",
    "MICROFININD": "Microfinance",
    "Mutual Fund": "Mutual Fund",
    "MUTUALIND": "Mutual Fund",
    "Non Life Insurance": "Non Life Insurance",
    "NONLIFEIND": "Non Life Insurance",
    "Others": "Others",
    "OTHERSIND": "Others",
    "Tradings": "Trading",
    "Trading": "Trading",
    "TRADINGIND": "Trading",
    "NEPSE": "NEPSE",
    "FLOAT": "Float",
    "SENSITIVE": "Sensitive",
    "SENFLOAT": "Sensitive Float",
}


@dataclass(frozen=True)
class SnapshotContext:
    as_of_date: str


def _safe_float(value: object, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, (float, int, np.floating, np.integer)):
        if pd.isna(value):
            return default
        return float(value)
    try:
        if pd.isna(value):
            return default
    except Exception:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _clamp(value, minimum: float = 0.0, maximum: float = 100.0):
    return np.clip(value, minimum, maximum)


def _series_or_default(df: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column in df.columns:
        return pd.to_numeric(df[column], errors="coerce").fillna(default)
    return pd.Series(default, index=df.index, dtype=float)


def _normalize_sector(value: object) -> str:
    if value is None:
        return "Unknown"
    text = str(value).strip()
    if not text:
        return "Unknown"
    return SECTOR_NORMALIZATION.get(text, text)


def ensure_derived_tables(conn) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS broker_analytics_daily (
            date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            broker INTEGER NOT NULL,
            sector TEXT,
            buy_qty REAL DEFAULT 0,
            sell_qty REAL DEFAULT 0,
            net_qty REAL DEFAULT 0,
            gross_qty REAL DEFAULT 0,
            buy_amount REAL DEFAULT 0,
            sell_amount REAL DEFAULT 0,
            net_amount REAL DEFAULT 0,
            gross_turnover REAL DEFAULT 0,
            buy_trades INTEGER DEFAULT 0,
            sell_trades INTEGER DEFAULT 0,
            total_trades INTEGER DEFAULT 0,
            buy_vwap REAL DEFAULT 0,
            sell_vwap REAL DEFAULT 0,
            participation_pct REAL DEFAULT 0,
            concentration_rank INTEGER DEFAULT 0,
            accumulation_score REAL DEFAULT 0,
            distribution_score REAL DEFAULT 0,
            smart_money_score REAL DEFAULT 0,
            PRIMARY KEY (date, symbol, broker)
        );

        CREATE TABLE IF NOT EXISTS stock_factor_snapshot (
            date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            security_name TEXT,
            sector TEXT,
            close REAL DEFAULT 0,
            previous_close REAL DEFAULT 0,
            change_pct REAL DEFAULT 0,
            volume REAL DEFAULT 0,
            turnover REAL DEFAULT 0,
            trades INTEGER DEFAULT 0,
            rsi REAL DEFAULT 0,
            macd REAL DEFAULT 0,
            bb_pct REAL DEFAULT 0,
            atr REAL DEFAULT 0,
            volume_ratio REAL DEFAULT 0,
            pump_score REAL DEFAULT 0,
            dump_score REAL DEFAULT 0,
            broker_accumulation REAL DEFAULT 0,
            broker_distribution REAL DEFAULT 0,
            net_broker_flow REAL DEFAULT 0,
            turnover_rank REAL DEFAULT 0,
            volume_rank REAL DEFAULT 0,
            transaction_rank REAL DEFAULT 0,
            technical_score REAL DEFAULT 0,
            momentum_score REAL DEFAULT 0,
            liquidity_score REAL DEFAULT 0,
            broker_score REAL DEFAULT 0,
            volatility_score REAL DEFAULT 0,
            risk_score REAL DEFAULT 0,
            smart_money_score REAL DEFAULT 0,
            price_action_score REAL DEFAULT 0,
            support_resistance_score REAL DEFAULT 0,
            composite_score REAL DEFAULT 0,
            setup_label TEXT,
            signal_label TEXT,
            entry_price REAL DEFAULT 0,
            target_price REAL DEFAULT 0,
            stop_loss REAL DEFAULT 0,
            risk_reward REAL DEFAULT 0,
            optimizer_name TEXT,
            optimizer_score REAL DEFAULT 0,
            rule_name TEXT,
            rule_score REAL DEFAULT 0,
            backtest_return REAL DEFAULT 0,
            backtest_win_rate REAL DEFAULT 0,
            PRIMARY KEY (date, symbol)
        );

        CREATE TABLE IF NOT EXISTS sector_factor_snapshot (
            date TEXT NOT NULL,
            sector TEXT NOT NULL,
            index_value REAL DEFAULT 0,
            change_pct REAL DEFAULT 0,
            point_change REAL DEFAULT 0,
            breadth_adv INTEGER DEFAULT 0,
            breadth_dec INTEGER DEFAULT 0,
            breadth_unch INTEGER DEFAULT 0,
            avg_stock_change REAL DEFAULT 0,
            avg_signal_score REAL DEFAULT 0,
            avg_broker_flow REAL DEFAULT 0,
            avg_liquidity_score REAL DEFAULT 0,
            sector_turnover REAL DEFAULT 0,
            sector_volume REAL DEFAULT 0,
            sector_strength_score REAL DEFAULT 0,
            rotation_score REAL DEFAULT 0,
            quadrant TEXT,
            PRIMARY KEY (date, sector)
        );

        CREATE TABLE IF NOT EXISTS support_resistance_levels (
            date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            support_1 REAL DEFAULT 0,
            support_2 REAL DEFAULT 0,
            resistance_1 REAL DEFAULT 0,
            resistance_2 REAL DEFAULT 0,
            support_strength REAL DEFAULT 0,
            resistance_strength REAL DEFAULT 0,
            distance_to_support_pct REAL DEFAULT 0,
            distance_to_resistance_pct REAL DEFAULT 0,
            breakout_probability REAL DEFAULT 0,
            retest_probability REAL DEFAULT 0,
            PRIMARY KEY (date, symbol)
        );

        CREATE TABLE IF NOT EXISTS scanner_results_daily (
            date TEXT NOT NULL,
            scanner_name TEXT NOT NULL,
            symbol TEXT NOT NULL,
            rank INTEGER DEFAULT 0,
            score REAL DEFAULT 0,
            signal TEXT,
            setup_label TEXT,
            entry_price REAL DEFAULT 0,
            target_price REAL DEFAULT 0,
            stop_loss REAL DEFAULT 0,
            risk_reward REAL DEFAULT 0,
            sector TEXT,
            metadata_json TEXT,
            PRIMARY KEY (date, scanner_name, symbol)
        );

        CREATE TABLE IF NOT EXISTS watchlists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS watchlist_items (
            watchlist_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            added_at TEXT DEFAULT CURRENT_TIMESTAMP,
            notes TEXT,
            PRIMARY KEY (watchlist_id, symbol),
            FOREIGN KEY (watchlist_id) REFERENCES watchlists(id)
        );

        CREATE TABLE IF NOT EXISTS portfolios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS portfolio_positions (
            portfolio_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            qty REAL NOT NULL DEFAULT 0,
            avg_cost REAL NOT NULL DEFAULT 0,
            opened_at TEXT DEFAULT CURRENT_TIMESTAMP,
            notes TEXT,
            PRIMARY KEY (portfolio_id, symbol),
            FOREIGN KEY (portfolio_id) REFERENCES portfolios(id)
        );
        """
    )
    conn.commit()


def get_latest_common_date(conn) -> str:
    query = """
        SELECT MIN(max_date) AS latest_common_date
        FROM (
            SELECT MAX(date) AS max_date FROM daily_price
            UNION ALL
            SELECT MAX(date) AS max_date FROM signals
            UNION ALL
            SELECT MAX(date) AS max_date FROM nepse_signals
            UNION ALL
            SELECT MAX(date) AS max_date FROM broker_summary
            UNION ALL
            SELECT MAX(date) AS max_date FROM sector_index
        )
    """
    latest = pd.read_sql_query(query, conn).iloc[0, 0]
    if not latest:
        raise RuntimeError("Could not determine latest common analytics date.")
    return str(latest)


def _load_recent_price_windows(conn, as_of_date: str, lookback_days: int = 90) -> pd.DataFrame:
    query = """
        SELECT symbol, date, open, high, low, close, volume
        FROM price_history
        WHERE date <= ?
        ORDER BY symbol, date
    """
    df = pd.read_sql_query(query, conn, params=[as_of_date])
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    df["row_num"] = df.groupby("symbol").cumcount(ascending=True)
    return df.groupby("symbol", group_keys=False).tail(lookback_days).copy()


def _load_latest_symbol_frame(conn, as_of_date: str) -> pd.DataFrame:
    query = """
        WITH previous_close AS (
            SELECT ph.symbol, ph.close AS previous_close
            FROM price_history ph
            JOIN (
                SELECT symbol, MAX(date) AS prev_date
                FROM price_history
                WHERE date < :as_of_date
                GROUP BY symbol
            ) prev
              ON prev.symbol = ph.symbol AND prev.prev_date = ph.date
        ),
        latest_optimizer AS (
            SELECT ob.symbol,
                   ob.best_indicator AS optimizer_name,
                   ob.composite_score AS optimizer_score
            FROM optimizer_best ob
        ),
        latest_rules AS (
            SELECT symbol,
                   rule_name,
                   weighted_score AS rule_score
            FROM (
                SELECT tr.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY symbol
                           ORDER BY weighted_score DESC, created_at DESC
                       ) AS rn
                FROM trading_rules tr
            ) ranked
            WHERE rn = 1
        ),
        latest_backtest AS (
            SELECT symbol_filter AS symbol,
                   total_return_pct AS backtest_return,
                   win_rate_pct AS backtest_win_rate
            FROM (
                SELECT bs.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY symbol_filter
                           ORDER BY id DESC
                       ) AS rn
                FROM backtest_summary bs
            ) ranked
            WHERE rn = 1
        )
        SELECT
            dp.date,
            dp.symbol,
            COALESCE(ac.company_name, c.label, dp.symbol) AS security_name,
            COALESCE(ac.sector, 'Unknown') AS sector,
            dp.open,
            dp.high,
            dp.low,
            dp.close,
            COALESCE(pc.previous_close, dp.close) AS previous_close,
            dp.volume,
            dp.amount AS turnover,
            dp.trades,
            dp.vwap,
            s.signal AS signal_label,
            s.total_score AS signal_score,
            s.rsi14 AS rsi,
            s.macd,
            s.bb_pct,
            s.atr14 AS atr,
            s.volume_ratio,
            ns.pump_score,
            ns.dump_score,
            ns.broker_accumulation,
            ns.broker_distribution,
            ns.net_broker_flow,
            ns.liquidity_spike,
            ns.volatility_regime,
            lo.optimizer_name,
            lo.optimizer_score,
            lr.rule_name,
            lr.rule_score,
            lb.backtest_return,
            lb.backtest_win_rate
        FROM daily_price dp
        LEFT JOIN signals s
          ON s.date = dp.date AND s.symbol = dp.symbol
        LEFT JOIN nepse_signals ns
          ON ns.date = dp.date AND ns.symbol = dp.symbol
        LEFT JOIN about_company ac
          ON ac.symbol = dp.symbol
        LEFT JOIN companies c
          ON c.symbol = dp.symbol
        LEFT JOIN previous_close pc
          ON pc.symbol = dp.symbol
        LEFT JOIN latest_optimizer lo
          ON lo.symbol = dp.symbol
        LEFT JOIN latest_rules lr
          ON lr.symbol = dp.symbol
        LEFT JOIN latest_backtest lb
          ON lb.symbol = dp.symbol
        WHERE dp.date = :as_of_date
    """
    df = pd.read_sql_query(query, conn, params={"as_of_date": as_of_date})
    if df.empty:
        return df
    df["change_pct"] = np.where(
        df["previous_close"].fillna(0) > 0,
        ((df["close"] - df["previous_close"]) / df["previous_close"]) * 100.0,
        0.0,
    )
    df["sector_group"] = df["sector"].map(_normalize_sector)
    return df


def _compute_support_resistance(price_window: pd.DataFrame, latest_close_map: pd.Series) -> pd.DataFrame:
    records: list[dict[str, float | str]] = []
    for symbol, group in price_window.groupby("symbol"):
        ordered = group.sort_values("date")
        close = _safe_float(latest_close_map.get(symbol))
        if close <= 0:
            continue

        lows = ordered["low"].dropna().astype(float)
        highs = ordered["high"].dropna().astype(float)
        if lows.empty or highs.empty:
            continue

        supports = sorted(set(np.round(lows.nsmallest(min(8, len(lows))), 2)))
        resistances = sorted(set(np.round(highs.nlargest(min(8, len(highs))), 2)))

        lower_supports = [value for value in supports if value <= close]
        upper_resistances = sorted([value for value in resistances if value >= close])

        support_1 = lower_supports[-1] if lower_supports else supports[0]
        support_2 = lower_supports[-2] if len(lower_supports) > 1 else support_1
        resistance_1 = upper_resistances[0] if upper_resistances else resistances[-1]
        resistance_2 = upper_resistances[1] if len(upper_resistances) > 1 else resistance_1

        support_strength = (
            ((ordered["low"] - support_1).abs() / support_1 <= 0.01).sum() if support_1 > 0 else 0
        )
        resistance_strength = (
            ((ordered["high"] - resistance_1).abs() / resistance_1 <= 0.01).sum()
            if resistance_1 > 0
            else 0
        )

        dist_support = ((close - support_1) / close) * 100.0 if close else 0.0
        dist_resistance = ((resistance_1 - close) / close) * 100.0 if close else 0.0
        breakout_probability = _clamp(55.0 - (dist_resistance * 4.5) + (resistance_strength * 3.0))
        retest_probability = _clamp(50.0 - (dist_support * 4.0) + (support_strength * 3.5))

        records.append(
            {
                "symbol": symbol,
                "support_1": support_1,
                "support_2": support_2,
                "resistance_1": resistance_1,
                "resistance_2": resistance_2,
                "support_strength": float(support_strength),
                "resistance_strength": float(resistance_strength),
                "distance_to_support_pct": round(dist_support, 4),
                "distance_to_resistance_pct": round(dist_resistance, 4),
                "breakout_probability": round(breakout_probability, 4),
                "retest_probability": round(retest_probability, 4),
            }
        )
    return pd.DataFrame(records)


def _build_stock_factor_snapshot(conn, as_of_date: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    stock_df = _load_latest_symbol_frame(conn, as_of_date)
    if stock_df.empty:
        return stock_df, pd.DataFrame()

    stock_df["turnover_rank"] = stock_df["turnover"].rank(pct=True) * 100.0
    stock_df["volume_rank"] = stock_df["volume"].rank(pct=True) * 100.0
    stock_df["transaction_rank"] = stock_df["trades"].rank(pct=True) * 100.0

    price_window = _load_recent_price_windows(conn, as_of_date)
    levels_df = _compute_support_resistance(price_window, stock_df.set_index("symbol")["close"])
    stock_df = stock_df.merge(levels_df, on="symbol", how="left")

    signal_score = _series_or_default(stock_df, "signal_score", 50.0)
    rsi = _series_or_default(stock_df, "rsi", 50.0)
    macd = _series_or_default(stock_df, "macd", 0.0)
    volume_ratio = _series_or_default(stock_df, "volume_ratio", 1.0)
    pump_score = _series_or_default(stock_df, "pump_score", 0.0)
    dump_score = _series_or_default(stock_df, "dump_score", 0.0)
    broker_acc = _series_or_default(stock_df, "broker_accumulation", 0.0)
    broker_dist = _series_or_default(stock_df, "broker_distribution", 0.0)
    net_broker = _series_or_default(stock_df, "net_broker_flow", 0.0)
    atr = _series_or_default(stock_df, "atr", 0.0)
    close = _series_or_default(stock_df, "close", 0.0)
    change_pct = _series_or_default(stock_df, "change_pct", 0.0)
    dist_support = _series_or_default(stock_df, "distance_to_support_pct", 0.0)
    dist_resistance = _series_or_default(stock_df, "distance_to_resistance_pct", 0.0)
    breakout_probability = _series_or_default(stock_df, "breakout_probability", 50.0)
    retest_probability = _series_or_default(stock_df, "retest_probability", 50.0)
    optimizer_score = _series_or_default(stock_df, "optimizer_score", 0.0)
    rule_score = _series_or_default(stock_df, "rule_score", 0.0)
    backtest_return = _series_or_default(stock_df, "backtest_return", 0.0)
    backtest_win_rate = _series_or_default(stock_df, "backtest_win_rate", 0.0)

    atr_pct = np.where(close > 0, (atr / close) * 100.0, 0.0)

    stock_df["technical_score"] = (
        signal_score * 0.55
        + _clamp(rsi, 0, 100) * 0.20
        + _clamp((macd.rank(pct=True) * 100.0), 0, 100) * 0.25
    ).round(4)

    stock_df["momentum_score"] = (
        _clamp(change_pct * 6.0 + 50.0)
        + _clamp(pump_score, 0, 100)
        + _clamp(backtest_return * 1.2 + 50.0)
    ) / 3.0

    stock_df["liquidity_score"] = (
        stock_df["turnover_rank"] * 0.45
        + stock_df["volume_rank"] * 0.30
        + stock_df["transaction_rank"] * 0.25
    ).round(4)

    stock_df["broker_score"] = (
        _clamp((broker_acc - broker_dist) * 0.6 + 50.0)
        + _clamp(net_broker.rank(pct=True) * 100.0)
        + _clamp(backtest_win_rate)
    ) / 3.0

    stock_df["volatility_score"] = _clamp(100.0 - (atr_pct * 6.0), 0.0, 100.0).round(4)
    stock_df["risk_score"] = _clamp(
        100.0
        - (
            (atr_pct * 10.0)
            + _clamp(np.abs(change_pct) * 2.0, 0.0, 35.0)
            + _clamp(dump_score * 0.35, 0.0, 35.0)
        ),
        0.0,
        100.0,
    ).round(4)

    stock_df["smart_money_score"] = (
        _clamp(broker_acc, 0, 100) * 0.45
        + _clamp(100.0 - broker_dist, 0, 100) * 0.20
        + _clamp(net_broker.rank(pct=True) * 100.0, 0, 100) * 0.20
        + _clamp(volume_ratio * 35.0, 0, 100) * 0.15
    ).round(4)

    stock_df["price_action_score"] = (
        _clamp(100.0 - dist_support * 4.0, 0, 100) * 0.35
        + _clamp(100.0 - dist_resistance * 3.5, 0, 100) * 0.20
        + breakout_probability * 0.25
        + retest_probability * 0.20
    ).round(4)

    stock_df["support_resistance_score"] = (
        breakout_probability * 0.5 + retest_probability * 0.5
    ).round(4)

    stock_df["composite_score"] = (
        stock_df["technical_score"] * 0.22
        + stock_df["momentum_score"] * 0.16
        + stock_df["liquidity_score"] * 0.14
        + stock_df["broker_score"] * 0.18
        + stock_df["smart_money_score"] * 0.14
        + stock_df["price_action_score"] * 0.10
        + stock_df["risk_score"] * 0.06
    ).round(4)

    stock_df["setup_label"] = np.select(
        [
            stock_df["smart_money_score"] >= 70,
            stock_df["price_action_score"] >= 68,
            stock_df["liquidity_score"] >= 70,
            stock_df["change_pct"] >= 0,
        ],
        [
            "Accumulation",
            "Breakout Setup",
            "High Activity",
            "Momentum",
        ],
        default="Watch",
    )

    stock_df["entry_price"] = stock_df["close"].round(4)
    stock_df["target_price"] = np.where(
        stock_df["resistance_1"].fillna(0) > stock_df["close"],
        stock_df["resistance_1"],
        stock_df["close"] * 1.05,
    ).round(4)
    stock_df["stop_loss"] = np.where(
        stock_df["support_1"].fillna(0) > 0,
        stock_df["support_1"],
        stock_df["close"] * 0.95,
    ).round(4)
    stock_df["risk_reward"] = np.where(
        (stock_df["entry_price"] - stock_df["stop_loss"]) > 0,
        (stock_df["target_price"] - stock_df["entry_price"])
        / (stock_df["entry_price"] - stock_df["stop_loss"]),
        0.0,
    ).round(4)

    stock_df["optimizer_name"] = stock_df["optimizer_name"].fillna("Composite")
    stock_df["rule_name"] = stock_df["rule_name"].fillna("Composite")
    stock_df["signal_label"] = stock_df["signal_label"].fillna("Hold")

    stock_df["date"] = as_of_date
    levels_df["date"] = as_of_date
    return stock_df, levels_df


def _build_broker_analytics(conn, as_of_date: str) -> pd.DataFrame:
    query = """
        SELECT
            bs.date,
            bs.symbol,
            bs.broker,
            COALESCE(ac.sector, 'Unknown') AS sector,
            bs.buy_qty,
            bs.sell_qty,
            bs.net_qty,
            bs.buy_amount,
            bs.sell_amount,
            bs.net_amount,
            bs.trades_as_buyer AS buy_trades,
            bs.trades_as_seller AS sell_trades
        FROM broker_summary bs
        LEFT JOIN about_company ac
          ON ac.symbol = bs.symbol
        WHERE bs.date = ?
    """
    df = pd.read_sql_query(query, conn, params=[as_of_date])
    if df.empty:
        return df

    df["gross_qty"] = df["buy_qty"] + df["sell_qty"]
    df["gross_turnover"] = df["buy_amount"] + df["sell_amount"]
    df["total_trades"] = df["buy_trades"] + df["sell_trades"]
    df["buy_vwap"] = np.where(df["buy_qty"] > 0, df["buy_amount"] / df["buy_qty"], 0.0)
    df["sell_vwap"] = np.where(df["sell_qty"] > 0, df["sell_amount"] / df["sell_qty"], 0.0)

    symbol_totals = df.groupby("symbol")["gross_qty"].transform("sum")
    df["participation_pct"] = np.where(symbol_totals > 0, (df["gross_qty"] / symbol_totals) * 100.0, 0.0)
    df["concentration_rank"] = (
        df.groupby("symbol")["gross_qty"].rank(method="dense", ascending=False).astype(int)
    )
    df["accumulation_score"] = _clamp(
        (df["net_qty"].clip(lower=0).rank(pct=True) * 100.0) * 0.7 + df["participation_pct"] * 0.3
    )
    df["distribution_score"] = _clamp(
        ((-df["net_qty"].clip(upper=0)).rank(pct=True) * 100.0) * 0.7 + df["participation_pct"] * 0.3
    )
    df["smart_money_score"] = _clamp(
        df["accumulation_score"] * 0.55
        + (100.0 - df["distribution_score"]) * 0.20
        + df["participation_pct"] * 0.25
    )
    return df


def _build_sector_snapshot(conn, as_of_date: str, stock_df: pd.DataFrame) -> pd.DataFrame:
    sector_query = """
        WITH latest AS (
            SELECT sector, value
            FROM sector_index
            WHERE date = :as_of_date
        ),
        previous AS (
            SELECT si.sector, si.value
            FROM sector_index si
            JOIN (
                SELECT sector, MAX(date) AS prev_date
                FROM sector_index
                WHERE date < :as_of_date
                GROUP BY sector
            ) prev
              ON prev.sector = si.sector AND prev.prev_date = si.date
        )
        SELECT
            l.sector,
            l.value AS index_value,
            COALESCE(p.value, l.value) AS previous_value
        FROM latest l
        LEFT JOIN previous p
          ON p.sector = l.sector
    """
    sector_df = pd.read_sql_query(sector_query, conn, params={"as_of_date": as_of_date})
    if sector_df.empty:
        return sector_df

    sector_df["change_pct"] = np.where(
        sector_df["previous_value"] > 0,
        ((sector_df["index_value"] - sector_df["previous_value"]) / sector_df["previous_value"]) * 100.0,
        0.0,
    )
    sector_df["point_change"] = sector_df["index_value"] - sector_df["previous_value"]

    grouped = stock_df.groupby("sector_group", dropna=False)
    agg_df = grouped.agg(
        breadth_adv=("change_pct", lambda s: int((s > 0).sum())),
        breadth_dec=("change_pct", lambda s: int((s < 0).sum())),
        breadth_unch=("change_pct", lambda s: int((s == 0).sum())),
        avg_stock_change=("change_pct", "mean"),
        avg_signal_score=("technical_score", "mean"),
        avg_broker_flow=("net_broker_flow", "mean"),
        avg_liquidity_score=("liquidity_score", "mean"),
        sector_turnover=("turnover", "sum"),
        sector_volume=("volume", "sum"),
    ).reset_index().rename(columns={"sector_group": "sector"})

    sector_df["sector"] = sector_df["sector"].map(_normalize_sector)
    sector_df = sector_df.merge(agg_df, on="sector", how="left")
    sector_df = sector_df.fillna(0)
    sector_df["sector_strength_score"] = (
        _clamp(sector_df["change_pct"] * 10.0 + 50.0)
        + _clamp(sector_df["avg_signal_score"], 0, 100)
        + _clamp(sector_df["avg_liquidity_score"], 0, 100)
    ) / 3.0
    sector_df["rotation_score"] = (
        sector_df["sector_strength_score"] * 0.45
        + _clamp(sector_df["avg_stock_change"] * 12.0 + 50.0) * 0.25
        + _clamp(
            np.where(
                (sector_df["breadth_adv"] + sector_df["breadth_dec"] + sector_df["breadth_unch"]) > 0,
                sector_df["breadth_adv"]
                / (sector_df["breadth_adv"] + sector_df["breadth_dec"] + sector_df["breadth_unch"])
                * 100.0,
                0.0,
            ),
            0,
            100,
        )
        * 0.30
    )
    sector_df["quadrant"] = np.select(
        [
            (sector_df["change_pct"] >= 0) & (sector_df["rotation_score"] >= 60),
            (sector_df["change_pct"] < 0) & (sector_df["rotation_score"] >= 55),
            (sector_df["change_pct"] >= 0) & (sector_df["rotation_score"] < 60),
        ],
        ["Leading", "Improving", "Weakening"],
        default="Lagging",
    )
    sector_df["date"] = as_of_date
    return sector_df


def _metadata_json(series: pd.Series, keys: Iterable[str]) -> str:
    payload: dict[str, object] = {}
    for key in keys:
        value = series.get(key)
        if pd.isna(value):
            continue
        if isinstance(value, (np.floating, np.integer)):
            payload[key] = float(value)
        else:
            payload[key] = value
    return pd.Series([payload]).to_json(orient="records")[1:-1]


def _build_scanner_results(stock_df: pd.DataFrame, as_of_date: str) -> pd.DataFrame:
    scanner_specs = {
        "broker_activity": stock_df["broker_score"] * 0.35 + stock_df["liquidity_score"] * 0.35 + stock_df["smart_money_score"] * 0.30,
        "accumulation": stock_df["smart_money_score"] * 0.45 + stock_df["broker_score"] * 0.30 + stock_df["price_action_score"] * 0.25,
        "broker_smartflow": stock_df["smart_money_score"] * 0.55 + stock_df["broker_score"] * 0.25 + stock_df["liquidity_score"] * 0.20,
        "best_volume_transaction": stock_df["liquidity_score"] * 0.65 + stock_df["momentum_score"] * 0.20 + stock_df["technical_score"] * 0.15,
        "best_gainer_loser": _clamp(np.abs(stock_df["change_pct"]) * 9.0 + stock_df["technical_score"] * 0.35 + stock_df["momentum_score"] * 0.35),
        "smc_scan": stock_df["smart_money_score"] * 0.35 + stock_df["price_action_score"] * 0.25 + stock_df["technical_score"] * 0.20 + stock_df["risk_score"] * 0.20,
        "support_resistance": stock_df["support_resistance_score"] * 0.55 + stock_df["price_action_score"] * 0.25 + stock_df["risk_score"] * 0.20,
        "best_of_best": stock_df["composite_score"],
    }

    result_frames: list[pd.DataFrame] = []
    for scanner_name, score_series in scanner_specs.items():
        frame = stock_df.copy()
        frame["score"] = pd.Series(score_series, index=frame.index).round(4)
        frame = frame.sort_values(["score", "turnover"], ascending=[False, False]).head(200).copy()
        frame["rank"] = np.arange(1, len(frame) + 1)
        frame["scanner_name"] = scanner_name
        frame["date"] = as_of_date
        frame["metadata_json"] = frame.apply(
            lambda row: _metadata_json(
                row,
                [
                    "technical_score",
                    "momentum_score",
                    "liquidity_score",
                    "broker_score",
                    "smart_money_score",
                    "price_action_score",
                    "risk_score",
                    "change_pct",
                    "turnover",
                    "volume",
                ],
            ),
            axis=1,
        )
        result_frames.append(
            frame[
                [
                    "date",
                    "scanner_name",
                    "symbol",
                    "rank",
                    "score",
                    "signal_label",
                    "setup_label",
                    "entry_price",
                    "target_price",
                    "stop_loss",
                    "risk_reward",
                    "sector",
                    "metadata_json",
                ]
            ].rename(columns={"signal_label": "signal"})
        )
    return pd.concat(result_frames, ignore_index=True) if result_frames else pd.DataFrame()


def build_latest_snapshots(as_of_date: str | None = None) -> dict[str, int | str]:
    conn = get_db()
    ensure_derived_tables(conn)
    as_of_date = as_of_date or get_latest_common_date(conn)

    stock_df, levels_df = _build_stock_factor_snapshot(conn, as_of_date)
    broker_df = _build_broker_analytics(conn, as_of_date)
    sector_df = _build_sector_snapshot(conn, as_of_date, stock_df)
    scanner_df = _build_scanner_results(stock_df, as_of_date)

    for table in [
        "stock_factor_snapshot",
        "support_resistance_levels",
        "broker_analytics_daily",
        "sector_factor_snapshot",
        "scanner_results_daily",
    ]:
        conn.execute(f"DELETE FROM {table} WHERE date = ?", [as_of_date])

    stock_columns = [
        "date",
        "symbol",
        "security_name",
        "sector",
        "close",
        "previous_close",
        "change_pct",
        "volume",
        "turnover",
        "trades",
        "rsi",
        "macd",
        "bb_pct",
        "atr",
        "volume_ratio",
        "pump_score",
        "dump_score",
        "broker_accumulation",
        "broker_distribution",
        "net_broker_flow",
        "turnover_rank",
        "volume_rank",
        "transaction_rank",
        "technical_score",
        "momentum_score",
        "liquidity_score",
        "broker_score",
        "volatility_score",
        "risk_score",
        "smart_money_score",
        "price_action_score",
        "support_resistance_score",
        "composite_score",
        "setup_label",
        "signal_label",
        "entry_price",
        "target_price",
        "stop_loss",
        "risk_reward",
        "optimizer_name",
        "optimizer_score",
        "rule_name",
        "rule_score",
        "backtest_return",
        "backtest_win_rate",
    ]
    level_columns = [
        "date",
        "symbol",
        "support_1",
        "support_2",
        "resistance_1",
        "resistance_2",
        "support_strength",
        "resistance_strength",
        "distance_to_support_pct",
        "distance_to_resistance_pct",
        "breakout_probability",
        "retest_probability",
    ]
    broker_columns = [
        "date",
        "symbol",
        "broker",
        "sector",
        "buy_qty",
        "sell_qty",
        "net_qty",
        "gross_qty",
        "buy_amount",
        "sell_amount",
        "net_amount",
        "gross_turnover",
        "buy_trades",
        "sell_trades",
        "total_trades",
        "buy_vwap",
        "sell_vwap",
        "participation_pct",
        "concentration_rank",
        "accumulation_score",
        "distribution_score",
        "smart_money_score",
    ]
    sector_columns = [
        "date",
        "sector",
        "index_value",
        "change_pct",
        "point_change",
        "breadth_adv",
        "breadth_dec",
        "breadth_unch",
        "avg_stock_change",
        "avg_signal_score",
        "avg_broker_flow",
        "avg_liquidity_score",
        "sector_turnover",
        "sector_volume",
        "sector_strength_score",
        "rotation_score",
        "quadrant",
    ]
    scanner_columns = [
        "date",
        "scanner_name",
        "symbol",
        "rank",
        "score",
        "signal",
        "setup_label",
        "entry_price",
        "target_price",
        "stop_loss",
        "risk_reward",
        "sector",
        "metadata_json",
    ]

    if not stock_df.empty:
        stock_df[stock_columns].to_sql("stock_factor_snapshot", conn, if_exists="append", index=False)
    if not levels_df.empty:
        levels_df[level_columns].to_sql("support_resistance_levels", conn, if_exists="append", index=False)
    if not broker_df.empty:
        broker_df[broker_columns].to_sql("broker_analytics_daily", conn, if_exists="append", index=False)
    if not sector_df.empty:
        sector_df[sector_columns].to_sql("sector_factor_snapshot", conn, if_exists="append", index=False)
    if not scanner_df.empty:
        scanner_df[scanner_columns].to_sql("scanner_results_daily", conn, if_exists="append", index=False)

    conn.commit()
    conn.close()

    return {
        "date": as_of_date,
        "stock_factor_snapshot": int(len(stock_df)),
        "support_resistance_levels": int(len(levels_df)),
        "broker_analytics_daily": int(len(broker_df)),
        "sector_factor_snapshot": int(len(sector_df)),
        "scanner_results_daily": int(len(scanner_df)),
    }
