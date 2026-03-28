from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from src.db import get_db
from src.data.derived_analytics import SECTOR_NORMALIZATION


ML_FEATURE_COLUMNS = [
    "date",
    "symbol",
    "sector",
    "close",
    "volume",
    "turnover",
    "trades",
    "change_pct",
    "rsi14",
    "macd",
    "macd_hist",
    "bb_pct",
    "atr14",
    "volume_ratio",
    "total_score",
    "pump_score",
    "dump_score",
    "broker_accumulation",
    "broker_distribution",
    "liquidity_spike",
    "net_broker_flow",
    "broker_total_buy_qty",
    "broker_total_sell_qty",
    "broker_total_net_qty",
    "broker_total_buy_amount",
    "broker_total_sell_amount",
    "broker_active_count",
    "broker_buy_sell_ratio",
    "broker_top3_buy_amount_share",
    "broker_top5_buy_amount_share",
    "broker_top3_net_qty_share",
    "broker_top5_net_qty_share",
    "broker_avg_trade_size",
    "sector_change_pct",
    "sector_point_change",
    "sector_index_value",
    "ret_1d",
    "ret_3d",
    "ret_5d",
    "ret_10d",
    "ret_20d",
    "turnover_ratio_3_20",
    "volume_ratio_5_20",
    "trades_ratio_5_20",
    "volatility_5d",
    "volatility_20d",
    "close_vs_20d_high_pct",
    "close_vs_20d_low_pct",
    "distance_from_ema20_pct",
    "rsi_delta_3d",
    "score_delta_3d",
    "score_ma_5",
    "score_ma_10",
    "net_broker_flow_ma_3",
    "net_broker_flow_ma_5",
    "net_broker_flow_z20",
    "broker_accumulation_ma_3",
    "broker_distribution_ma_3",
    "liquidity_spike_ma_5",
    "pump_score_ma_3",
    "dump_score_ma_3",
    "sector_change_ma_5",
    "sector_change_ma_10",
    "sector_strength_rank_pct",
    "fwd_return_3d",
    "fwd_return_5d",
    "fwd_return_10d",
    "fwd_max_upside_5d",
    "fwd_max_drawdown_5d",
    "label_up_3d_3pct",
    "label_up_5d_4pct",
    "label_up_10d_6pct",
    "label_breakout_5d",
]


@dataclass(frozen=True)
class FeatureBuildResult:
    row_count: int
    symbol_count: int
    date_min: str | None
    date_max: str | None


def _normalize_sector(value: object) -> str:
    if value is None:
        return "Unknown"
    text = str(value).strip()
    if not text:
        return "Unknown"
    return SECTOR_NORMALIZATION.get(text, text)


def _safe_divide(a: pd.Series | np.ndarray, b: pd.Series | np.ndarray) -> np.ndarray:
    a_arr = np.asarray(a, dtype=float)
    b_arr = np.asarray(b, dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        out = np.where(np.abs(b_arr) > 1e-12, a_arr / b_arr, 0.0)
    out[~np.isfinite(out)] = 0.0
    return out


def _read_base_frame(conn) -> pd.DataFrame:
    query = """
        SELECT
            dp.date,
            dp.symbol,
            COALESCE(ac.sector, 'Unknown') AS sector,
            dp.close,
            dp.volume,
            dp.amount AS turnover,
            dp.trades,
            s.rsi14,
            s.macd,
            s.macd_hist,
            s.bb_pct,
            s.ema20,
            s.atr14,
            s.volume_ratio,
            s.total_score,
            ns.pump_score,
            ns.dump_score,
            ns.broker_accumulation,
            ns.broker_distribution,
            ns.liquidity_spike,
            ns.net_broker_flow
        FROM daily_price dp
        LEFT JOIN signals s
          ON s.date = dp.date AND s.symbol = dp.symbol
        LEFT JOIN nepse_signals ns
          ON ns.date = dp.date AND ns.symbol = dp.symbol
        LEFT JOIN about_company ac
          ON ac.symbol = dp.symbol
        ORDER BY dp.symbol, dp.date
    """
    df = pd.read_sql_query(query, conn)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    df["sector"] = df["sector"].map(_normalize_sector)
    numeric_cols = [col for col in df.columns if col not in {"date", "symbol", "sector"}]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df


def _read_broker_daily_agg(conn) -> pd.DataFrame:
    query = """
        WITH broker_ranked AS (
            SELECT
                date,
                symbol,
                broker,
                buy_qty,
                sell_qty,
                net_qty,
                buy_amount,
                sell_amount,
                trades_as_buyer,
                trades_as_seller,
                ROW_NUMBER() OVER (PARTITION BY date, symbol ORDER BY buy_amount DESC, buy_qty DESC) AS buy_rank,
                ROW_NUMBER() OVER (PARTITION BY date, symbol ORDER BY ABS(net_qty) DESC, ABS(net_amount) DESC) AS net_rank
            FROM broker_summary
        )
        SELECT
            date,
            symbol,
            SUM(buy_qty) AS broker_total_buy_qty,
            SUM(sell_qty) AS broker_total_sell_qty,
            SUM(net_qty) AS broker_total_net_qty,
            SUM(buy_amount) AS broker_total_buy_amount,
            SUM(sell_amount) AS broker_total_sell_amount,
            COUNT(*) AS broker_active_count,
            SUM(trades_as_buyer + trades_as_seller) AS broker_total_trades,
            SUM(CASE WHEN buy_rank <= 3 THEN buy_amount ELSE 0 END) AS top3_buy_amount,
            SUM(CASE WHEN buy_rank <= 5 THEN buy_amount ELSE 0 END) AS top5_buy_amount,
            SUM(CASE WHEN net_rank <= 3 THEN MAX(net_qty, 0) ELSE 0 END) AS top3_net_qty,
            SUM(CASE WHEN net_rank <= 5 THEN MAX(net_qty, 0) ELSE 0 END) AS top5_net_qty
        FROM broker_ranked
        GROUP BY date, symbol
        ORDER BY symbol, date
    """
    df = pd.read_sql_query(query, conn)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    numeric_cols = [col for col in df.columns if col not in {"date", "symbol"}]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df["broker_buy_sell_ratio"] = _safe_divide(df["broker_total_buy_qty"], df["broker_total_sell_qty"])
    df["broker_top3_buy_amount_share"] = _safe_divide(df["top3_buy_amount"], df["broker_total_buy_amount"]) * 100.0
    df["broker_top5_buy_amount_share"] = _safe_divide(df["top5_buy_amount"], df["broker_total_buy_amount"]) * 100.0
    positive_net = df["broker_total_net_qty"].clip(lower=0)
    df["broker_top3_net_qty_share"] = _safe_divide(df["top3_net_qty"], positive_net) * 100.0
    df["broker_top5_net_qty_share"] = _safe_divide(df["top5_net_qty"], positive_net) * 100.0
    df["broker_avg_trade_size"] = _safe_divide(
        df["broker_total_buy_qty"] + df["broker_total_sell_qty"],
        df["broker_total_trades"],
    )
    return df[
        [
            "date",
            "symbol",
            "broker_total_buy_qty",
            "broker_total_sell_qty",
            "broker_total_net_qty",
            "broker_total_buy_amount",
            "broker_total_sell_amount",
            "broker_active_count",
            "broker_buy_sell_ratio",
            "broker_top3_buy_amount_share",
            "broker_top5_buy_amount_share",
            "broker_top3_net_qty_share",
            "broker_top5_net_qty_share",
            "broker_avg_trade_size",
        ]
    ]


def _read_sector_history(conn) -> pd.DataFrame:
    query = """
        WITH normalized AS (
            SELECT
                date,
                sector,
                value
            FROM sector_index
        )
        SELECT date, sector, value
        FROM normalized
        ORDER BY sector, date
    """
    df = pd.read_sql_query(query, conn)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    df["sector"] = df["sector"].map(_normalize_sector)
    df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0.0)
    df = df.sort_values(["sector", "date"]).drop_duplicates(["sector", "date"], keep="last")
    df["sector_change_pct"] = df.groupby("sector")["value"].pct_change().replace([np.inf, -np.inf], np.nan).fillna(0.0) * 100.0
    df["sector_point_change"] = df.groupby("sector")["value"].diff().fillna(0.0)
    df["sector_change_ma_5"] = df.groupby("sector")["sector_change_pct"].transform(lambda s: s.rolling(5, min_periods=2).mean()).fillna(0.0)
    df["sector_change_ma_10"] = df.groupby("sector")["sector_change_pct"].transform(lambda s: s.rolling(10, min_periods=3).mean()).fillna(0.0)
    return df.rename(columns={"value": "sector_index_value"})


def _add_symbol_rollups(base: pd.DataFrame) -> pd.DataFrame:
    df = base.sort_values(["symbol", "date"]).copy()
    g = df.groupby("symbol", group_keys=False)

    df["change_pct"] = g["close"].pct_change().replace([np.inf, -np.inf], np.nan).fillna(0.0) * 100.0
    for window in (1, 3, 5, 10, 20):
        df[f"ret_{window}d"] = g["close"].pct_change(window).replace([np.inf, -np.inf], np.nan).fillna(0.0) * 100.0

    vol_ma_5 = g["volume"].transform(lambda s: s.rolling(5, min_periods=2).mean())
    vol_ma_20 = g["volume"].transform(lambda s: s.rolling(20, min_periods=5).mean())
    turnover_ma_3 = g["turnover"].transform(lambda s: s.rolling(3, min_periods=2).mean())
    turnover_ma_20 = g["turnover"].transform(lambda s: s.rolling(20, min_periods=5).mean())
    trades_ma_5 = g["trades"].transform(lambda s: s.rolling(5, min_periods=2).mean())
    trades_ma_20 = g["trades"].transform(lambda s: s.rolling(20, min_periods=5).mean())
    df["turnover_ratio_3_20"] = _safe_divide(turnover_ma_3, turnover_ma_20)
    df["volume_ratio_5_20"] = _safe_divide(vol_ma_5, vol_ma_20)
    df["trades_ratio_5_20"] = _safe_divide(trades_ma_5, trades_ma_20)

    df["volatility_5d"] = g["close"].transform(lambda s: s.pct_change().rolling(5, min_periods=3).std()).fillna(0.0)
    df["volatility_20d"] = g["close"].transform(lambda s: s.pct_change().rolling(20, min_periods=5).std()).fillna(0.0)

    rolling_high_20 = g["close"].transform(lambda s: s.rolling(20, min_periods=5).max())
    rolling_low_20 = g["close"].transform(lambda s: s.rolling(20, min_periods=5).min())
    df["close_vs_20d_high_pct"] = _safe_divide(df["close"] - rolling_high_20, rolling_high_20) * 100.0
    df["close_vs_20d_low_pct"] = _safe_divide(df["close"] - rolling_low_20, rolling_low_20) * 100.0
    df["distance_from_ema20_pct"] = _safe_divide(df["close"] - df["ema20"], df["ema20"]) * 100.0

    df["rsi_delta_3d"] = g["rsi14"].diff(3).fillna(0.0)
    df["score_delta_3d"] = g["total_score"].diff(3).fillna(0.0)
    df["score_ma_5"] = g["total_score"].transform(lambda s: s.rolling(5, min_periods=2).mean()).fillna(0.0)
    df["score_ma_10"] = g["total_score"].transform(lambda s: s.rolling(10, min_periods=3).mean()).fillna(0.0)

    df["net_broker_flow_ma_3"] = g["net_broker_flow"].transform(lambda s: s.rolling(3, min_periods=2).mean()).fillna(0.0)
    df["net_broker_flow_ma_5"] = g["net_broker_flow"].transform(lambda s: s.rolling(5, min_periods=2).mean()).fillna(0.0)
    flow_ma_20 = g["net_broker_flow"].transform(lambda s: s.rolling(20, min_periods=5).mean()).fillna(0.0)
    flow_std_20 = g["net_broker_flow"].transform(lambda s: s.rolling(20, min_periods=5).std()).replace(0, np.nan)
    df["net_broker_flow_z20"] = _safe_divide(df["net_broker_flow"] - flow_ma_20, flow_std_20)
    df["broker_accumulation_ma_3"] = g["broker_accumulation"].transform(lambda s: s.rolling(3, min_periods=2).mean()).fillna(0.0)
    df["broker_distribution_ma_3"] = g["broker_distribution"].transform(lambda s: s.rolling(3, min_periods=2).mean()).fillna(0.0)
    df["liquidity_spike_ma_5"] = g["liquidity_spike"].transform(lambda s: s.rolling(5, min_periods=2).mean()).fillna(0.0)
    df["pump_score_ma_3"] = g["pump_score"].transform(lambda s: s.rolling(3, min_periods=2).mean()).fillna(0.0)
    df["dump_score_ma_3"] = g["dump_score"].transform(lambda s: s.rolling(3, min_periods=2).mean()).fillna(0.0)

    df["fwd_return_3d"] = _safe_divide(g["close"].shift(-3) - df["close"], df["close"]) * 100.0
    df["fwd_return_5d"] = _safe_divide(g["close"].shift(-5) - df["close"], df["close"]) * 100.0
    df["fwd_return_10d"] = _safe_divide(g["close"].shift(-10) - df["close"], df["close"]) * 100.0
    fwd_max_5 = g["close"].transform(lambda s: s.shift(-1).rolling(5, min_periods=1).max())
    fwd_min_5 = g["close"].transform(lambda s: s.shift(-1).rolling(5, min_periods=1).min())
    df["fwd_max_upside_5d"] = _safe_divide(fwd_max_5 - df["close"], df["close"]) * 100.0
    df["fwd_max_drawdown_5d"] = _safe_divide(fwd_min_5 - df["close"], df["close"]) * 100.0

    df["label_up_3d_3pct"] = (df["fwd_return_3d"] >= 3.0).astype(int)
    df["label_up_5d_4pct"] = (df["fwd_return_5d"] >= 4.0).astype(int)
    df["label_up_10d_6pct"] = (df["fwd_return_10d"] >= 6.0).astype(int)
    df["label_breakout_5d"] = ((df["fwd_max_upside_5d"] >= 5.0) & (df["fwd_max_drawdown_5d"] > -3.0)).astype(int)

    return df


def _add_sector_rank(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["sector_strength_rank_pct"] = (
        out.groupby("date")["sector_change_pct"].rank(pct=True).fillna(0.0) * 100.0
    )
    return out


def build_feature_frame() -> pd.DataFrame:
    conn = get_db()
    try:
        base = _read_base_frame(conn)
        if base.empty:
            return base
        broker = _read_broker_daily_agg(conn)
        sectors = _read_sector_history(conn)
    finally:
        conn.close()

    df = base.merge(broker, on=["date", "symbol"], how="left")
    df = df.merge(sectors, on=["date", "sector"], how="left")

    fill_zero_cols = [col for col in df.columns if col not in {"date", "symbol", "sector"}]
    for col in fill_zero_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df = _add_symbol_rollups(df)
    df = _add_sector_rank(df)

    for col in ML_FEATURE_COLUMNS:
        if col not in df.columns:
            if col in {"date", "symbol", "sector"}:
                df[col] = ""
            else:
                df[col] = 0.0

    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    return df[ML_FEATURE_COLUMNS].copy()


def ensure_ml_feature_table(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ml_feature_snapshot (
            date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            sector TEXT,
            close REAL DEFAULT 0,
            volume REAL DEFAULT 0,
            turnover REAL DEFAULT 0,
            trades REAL DEFAULT 0,
            change_pct REAL DEFAULT 0,
            rsi14 REAL DEFAULT 0,
            macd REAL DEFAULT 0,
            macd_hist REAL DEFAULT 0,
            bb_pct REAL DEFAULT 0,
            atr14 REAL DEFAULT 0,
            volume_ratio REAL DEFAULT 0,
            total_score REAL DEFAULT 0,
            pump_score REAL DEFAULT 0,
            dump_score REAL DEFAULT 0,
            broker_accumulation REAL DEFAULT 0,
            broker_distribution REAL DEFAULT 0,
            liquidity_spike REAL DEFAULT 0,
            net_broker_flow REAL DEFAULT 0,
            broker_total_buy_qty REAL DEFAULT 0,
            broker_total_sell_qty REAL DEFAULT 0,
            broker_total_net_qty REAL DEFAULT 0,
            broker_total_buy_amount REAL DEFAULT 0,
            broker_total_sell_amount REAL DEFAULT 0,
            broker_active_count REAL DEFAULT 0,
            broker_buy_sell_ratio REAL DEFAULT 0,
            broker_top3_buy_amount_share REAL DEFAULT 0,
            broker_top5_buy_amount_share REAL DEFAULT 0,
            broker_top3_net_qty_share REAL DEFAULT 0,
            broker_top5_net_qty_share REAL DEFAULT 0,
            broker_avg_trade_size REAL DEFAULT 0,
            sector_change_pct REAL DEFAULT 0,
            sector_point_change REAL DEFAULT 0,
            sector_index_value REAL DEFAULT 0,
            ret_1d REAL DEFAULT 0,
            ret_3d REAL DEFAULT 0,
            ret_5d REAL DEFAULT 0,
            ret_10d REAL DEFAULT 0,
            ret_20d REAL DEFAULT 0,
            turnover_ratio_3_20 REAL DEFAULT 0,
            volume_ratio_5_20 REAL DEFAULT 0,
            trades_ratio_5_20 REAL DEFAULT 0,
            volatility_5d REAL DEFAULT 0,
            volatility_20d REAL DEFAULT 0,
            close_vs_20d_high_pct REAL DEFAULT 0,
            close_vs_20d_low_pct REAL DEFAULT 0,
            distance_from_ema20_pct REAL DEFAULT 0,
            rsi_delta_3d REAL DEFAULT 0,
            score_delta_3d REAL DEFAULT 0,
            score_ma_5 REAL DEFAULT 0,
            score_ma_10 REAL DEFAULT 0,
            net_broker_flow_ma_3 REAL DEFAULT 0,
            net_broker_flow_ma_5 REAL DEFAULT 0,
            net_broker_flow_z20 REAL DEFAULT 0,
            broker_accumulation_ma_3 REAL DEFAULT 0,
            broker_distribution_ma_3 REAL DEFAULT 0,
            liquidity_spike_ma_5 REAL DEFAULT 0,
            pump_score_ma_3 REAL DEFAULT 0,
            dump_score_ma_3 REAL DEFAULT 0,
            sector_change_ma_5 REAL DEFAULT 0,
            sector_change_ma_10 REAL DEFAULT 0,
            sector_strength_rank_pct REAL DEFAULT 0,
            fwd_return_3d REAL DEFAULT 0,
            fwd_return_5d REAL DEFAULT 0,
            fwd_return_10d REAL DEFAULT 0,
            fwd_max_upside_5d REAL DEFAULT 0,
            fwd_max_drawdown_5d REAL DEFAULT 0,
            label_up_3d_3pct INTEGER DEFAULT 0,
            label_up_5d_4pct INTEGER DEFAULT 0,
            label_up_10d_6pct INTEGER DEFAULT 0,
            label_breakout_5d INTEGER DEFAULT 0,
            PRIMARY KEY (date, symbol)
        )
        """
    )
    conn.commit()


def materialize_ml_feature_snapshot(rebuild: bool = True) -> FeatureBuildResult:
    df = build_feature_frame()
    conn = get_db()
    try:
        ensure_ml_feature_table(conn)
        if rebuild:
            conn.execute("DELETE FROM ml_feature_snapshot")
            conn.commit()
        if not df.empty:
            df.to_sql("ml_feature_snapshot", conn, if_exists="append", index=False)
            conn.commit()
        date_min = None if df.empty else str(df["date"].min())
        date_max = None if df.empty else str(df["date"].max())
        return FeatureBuildResult(
            row_count=int(len(df)),
            symbol_count=int(df["symbol"].nunique()) if not df.empty else 0,
            date_min=date_min,
            date_max=date_max,
        )
    finally:
        conn.close()


def load_ml_feature_snapshot(
    target: str = "label_up_5d_4pct",
    start_date: str | None = None,
    end_date: str | None = None,
    dropna_future: bool = True,
) -> pd.DataFrame:
    conn = get_db()
    try:
        ensure_ml_feature_table(conn)
        query = "SELECT * FROM ml_feature_snapshot WHERE 1=1"
        params: list[object] = []
        if start_date:
            query += " AND date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND date <= ?"
            params.append(end_date)
        query += " ORDER BY date, symbol"
        df = pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()

    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    numeric_cols = [col for col in df.columns if col not in {"date", "symbol", "sector"}]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    if dropna_future and target in df.columns:
        # Drop rows near the end where forward horizon was not fully available.
        label_cols = {
            "label_up_3d_3pct": "fwd_return_3d",
            "label_up_5d_4pct": "fwd_return_5d",
            "label_up_10d_6pct": "fwd_return_10d",
            "label_breakout_5d": "fwd_max_upside_5d",
        }
        future_col = label_cols.get(target)
        if future_col and future_col in df.columns:
            df = df[df[future_col].notna()].copy()
    return df


def feature_columns(target: str) -> list[str]:
    excluded = {
        "date",
        "symbol",
        "sector",
        "fwd_return_3d",
        "fwd_return_5d",
        "fwd_return_10d",
        "fwd_max_upside_5d",
        "fwd_max_drawdown_5d",
        "label_up_3d_3pct",
        "label_up_5d_4pct",
        "label_up_10d_6pct",
        "label_breakout_5d",
    }
    return [col for col in ML_FEATURE_COLUMNS if col not in excluded]
