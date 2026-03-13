# src/engine/backtester.py
# Step 7: NEPSE Backtester — full T+2-compliant signal-driven simulation
#
# T+2 SETTLEMENT (critical):
#   BUY signal on row i
#   → Entry at OPEN of row i+1  (next trading day)
#   → Position LOCKED until row i+3  (2 full trading days after entry)
#   → First possible exit: row i+3 onwards
#   Stop-loss / take-profit CANNOT fire on rows i+1 or i+2.
#
# Exit priority (checked each day from lock_until onwards):
#   1. Stop-loss  : close <= entry * (1 - sl%)
#   2. Take-profit: close >= entry * (1 + tp%)
#   3. Time-stop  : days_held >= max_hold_days
#   4. End-of-data: exit at last close

import sqlite3
import os
import traceback
from datetime import datetime

import pandas as pd
import numpy as np
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from engine.indicators import get_all_indicator_configs, generate_signals

# ── CONFIG ────────────────────────────────────────────────────────────────────
DB_PATH = "data/nepse.db"

DEFAULT_CONFIG = {
    "stop_loss_pct":     5.0,
    "take_profit_pct":  10.0,
    "max_hold_days":    15,
    "initial_capital":  100_000,
    "position_size_pct": 10.0,
}


# ── DB HELPER ─────────────────────────────────────────────────────────────────
def get_db():
    os.makedirs("data", exist_ok=True)
    return sqlite3.connect(DB_PATH)


# ── METRICS ───────────────────────────────────────────────────────────────────
def _calc_metrics(trades: list, initial_capital: float) -> dict:
    if not trades:
        return {
            "total_trades": 0, "winning_trades": 0, "losing_trades": 0,
            "winrate": 0.0, "profit_factor": 0.0,
            "avg_win_pct": 0.0, "avg_loss_pct": 0.0,
            "max_drawdown": 0.0, "total_return_pct": 0.0,
            "final_capital": initial_capital, "consistency": 0.0,
        }

    winners = [t for t in trades if t["pnl_amount"] > 0]
    losers  = [t for t in trades if t["pnl_amount"] <= 0]

    gross_profit = sum(t["pnl_amount"] for t in winners)
    gross_loss   = abs(sum(t["pnl_amount"] for t in losers))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    avg_win  = float(np.mean([t["pnl_pct"] for t in winners])) if winners else 0.0
    avg_loss = float(np.mean([t["pnl_pct"] for t in losers]))  if losers  else 0.0

    # Equity curve for drawdown
    equity = initial_capital
    equity_curve = [equity]
    for t in trades:
        equity += t["pnl_amount"]
        equity_curve.append(equity)

    arr  = np.array(equity_curve)
    peak = arr[0]
    max_dd = 0.0
    for val in arr:
        if val > peak:
            peak = val
        dd = (peak - val) / peak * 100 if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd

    final_capital    = equity_curve[-1]
    total_return_pct = (final_capital - initial_capital) / initial_capital * 100

    # Consistency: % of calendar quarters with positive PnL
    quarter_pnl: dict = {}
    for t in trades:
        try:
            d = pd.Timestamp(t["exit_date"])
            q = f"{d.year}Q{d.quarter}"
            quarter_pnl[q] = quarter_pnl.get(q, 0.0) + t["pnl_amount"]
        except Exception:
            pass
    profitable_quarters = sum(1 for v in quarter_pnl.values() if v > 0)
    consistency = (profitable_quarters / len(quarter_pnl) * 100
                   if quarter_pnl else 0.0)

    return {
        "total_trades":      len(trades),
        "winning_trades":    len(winners),
        "losing_trades":     len(losers),
        "winrate":           round(len(winners) / len(trades) * 100, 2),
        "profit_factor":     round(profit_factor, 4),
        "avg_win_pct":       round(avg_win,   4),
        "avg_loss_pct":      round(avg_loss,  4),
        "max_drawdown":      round(max_dd,    4),
        "total_return_pct":  round(total_return_pct, 4),
        "final_capital":     round(final_capital, 2),
        "consistency":       round(consistency, 2),
    }


# ── CORE BACKTEST ENGINE ──────────────────────────────────────────────────────
def run_backtest(df: pd.DataFrame, signal_series: pd.Series, config: dict) -> dict:
    """
    Simulate trades on historical OHLCV data driven by *signal_series*.

    Parameters
    ----------
    df            : clean_price_history DataFrame with columns
                    [date, open, high, low, close, volume, ...]
    signal_series : integer Series aligned with df index — 1=BUY, -1=SELL, 0=HOLD
    config        : backtest config dict (see DEFAULT_CONFIG)

    Returns
    -------
    dict with keys 'trades' (list of dicts) and 'metrics' (dict).
    """
    sl_pct    = config.get("stop_loss_pct",     DEFAULT_CONFIG["stop_loss_pct"])
    tp_pct    = config.get("take_profit_pct",   DEFAULT_CONFIG["take_profit_pct"])
    max_hold  = config.get("max_hold_days",      DEFAULT_CONFIG["max_hold_days"])
    capital   = float(config.get("initial_capital",   DEFAULT_CONFIG["initial_capital"]))
    size_pct  = config.get("position_size_pct", DEFAULT_CONFIG["position_size_pct"])

    # Reset to positional index for easy slicing
    df = df.reset_index(drop=True)
    sigs = signal_series.reset_index(drop=True)

    opens  = df["open"].values
    closes = df["close"].values
    dates  = df["date"].values
    n      = len(df)

    trades: list = []
    in_position = False   # only one open position at a time

    sl_mul = 1.0 - sl_pct  / 100.0
    tp_mul = 1.0 + tp_pct  / 100.0

    for i in range(n):
        # Skip if already holding a position
        if in_position:
            continue

        if sigs.iloc[i] != 1:   # not a BUY signal
            continue

        # ── Entry (next-day open) ─────────────────────────────────────────────
        entry_idx = i + 1
        if entry_idx >= n:
            break                    # no next day to enter

        entry_price = opens[entry_idx]
        if entry_price <= 0:
            continue

        entry_date = dates[entry_idx]
        invest     = capital * (size_pct / 100.0)
        invest     = min(invest, capital * 0.95)  # never use more than 95% of capital
        if invest < 10:
            continue

        shares = invest / entry_price
        in_position = True

        # ── T+2 lock — first exit allowed at entry_idx + 2 ───────────────────
        lock_until = entry_idx + 2   # rows i+1 (entry) and i+2 are locked
        exit_idx   = None
        exit_reason = "END_OF_DATA"

        for j in range(lock_until, n):
            days_held = j - entry_idx
            c = closes[j]

            if c <= entry_price * sl_mul:
                exit_idx    = j
                exit_reason = "STOP_LOSS"
                break
            if c >= entry_price * tp_mul:
                exit_idx    = j
                exit_reason = "TAKE_PROFIT"
                break
            if days_held >= max_hold:
                exit_idx    = j
                exit_reason = "TIME_STOP"
                break

        if exit_idx is None:
            exit_idx    = n - 1
            exit_reason = "END_OF_DATA"

        exit_price = closes[exit_idx]
        exit_date  = dates[exit_idx]
        days_held  = exit_idx - entry_idx

        gross    = (exit_price - entry_price) * shares
        # Simple commission: 0.4% each side
        comm     = invest * 0.004 + exit_price * shares * 0.004
        net      = gross - comm
        pnl_pct  = net / invest * 100

        capital += net
        in_position = False

        trades.append({
            "entry_date":   str(entry_date),
            "exit_date":    str(exit_date),
            "entry_price":  round(float(entry_price),  2),
            "exit_price":   round(float(exit_price),   2),
            "shares":       round(float(shares),        4),
            "invested":     round(float(invest),        2),
            "gross":        round(float(gross),         2),
            "commission":   round(float(comm),          2),
            "pnl_amount":   round(float(net),           2),
            "pnl_pct":      round(float(pnl_pct),       4),
            "days_held":    int(days_held),
            "exit_reason":  exit_reason,
        })

    metrics = _calc_metrics(trades, float(config.get("initial_capital",
                                                      DEFAULT_CONFIG["initial_capital"])))
    return {"trades": trades, "metrics": metrics}


# ── BACKTEST ONE INDICATOR FOR ONE SYMBOL ─────────────────────────────────────
def backtest_indicator(symbol: str, indicator_config: dict,
                       backtest_config: dict | None = None) -> dict | None:
    """
    Load clean_price_history for *symbol*, generate signals with
    *indicator_config*, run run_backtest(), and return the full result dict.

    Returns None if there is not enough data.
    """
    if backtest_config is None:
        backtest_config = DEFAULT_CONFIG.copy()

    conn = get_db()
    try:
        df = pd.read_sql_query("""
            SELECT date, open, high, low, close, volume,
                   price_change_pct, volume_ratio, atr14, market_condition
            FROM   clean_price_history
            WHERE  symbol = ?
            ORDER  BY date ASC
        """, conn, params=(symbol,))
    finally:
        conn.close()

    if df.empty or len(df) < 30:
        return None

    try:
        df_with_sig = generate_signals(df, indicator_config)
        result = run_backtest(df, df_with_sig["signal"], backtest_config)
        result["symbol"]    = symbol
        result["indicator"] = indicator_config["name"]
        result["indicator_type"] = indicator_config["type"]
        return result
    except Exception as e:
        return None


# ── BACKTEST ALL INDICATORS FOR ONE SYMBOL ────────────────────────────────────
def backtest_all_indicators(symbol: str,
                             backtest_config: dict | None = None) -> list:
    """
    Run backtest_indicator() for every config in get_all_indicator_configs().
    Returns a list of result dicts sorted by winrate descending.
    Prints progress every 50 indicators.
    """
    if backtest_config is None:
        backtest_config = DEFAULT_CONFIG.copy()

    configs = get_all_indicator_configs()
    results = []
    failed  = 0

    for i, cfg in enumerate(configs, 1):
        res = backtest_indicator(symbol, cfg, backtest_config)
        if res and res["metrics"]["total_trades"] > 0:
            results.append(res)
        else:
            failed += 1

        if i % 50 == 0 or i == len(configs):
            print(f"  [{i}/{len(configs)}] valid={len(results)} failed/no-trades={failed}")

    results.sort(key=lambda r: r["metrics"]["winrate"], reverse=True)
    return results


# ── PRETTY PRINT RESULTS TABLE ────────────────────────────────────────────────
def print_results_table(results: list, n: int = 20):
    """Print a formatted leaderboard of indicator backtest results."""
    if not results:
        print("  No results to display.")
        return

    header = f"{'#':>3}  {'Indicator':<24} {'Type':<12} {'Trades':>6} {'WinRate':>8} {'PF':>6} {'AvgW%':>7} {'AvgL%':>7} {'MaxDD%':>7} {'Return%':>8}"
    print(header)
    print("-" * len(header))

    for rank, res in enumerate(results[:n], 1):
        m = res["metrics"]
        print(
            f"{rank:>3}  {res['indicator']:<24} {res['indicator_type']:<12} "
            f"{m['total_trades']:>6} {m['winrate']:>7.1f}% {m['profit_factor']:>6.2f} "
            f"{m['avg_win_pct']:>7.2f} {m['avg_loss_pct']:>7.2f} "
            f"{m['max_drawdown']:>7.2f} {m['total_return_pct']:>8.2f}%"
        )


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 65)
    print("NEPSE Backtester - Step 7")
    print("=" * 65)
    print()

    SYMBOL = "NABIL"
    BT_CONFIG = {
        "stop_loss_pct":     5.0,
        "take_profit_pct":  10.0,
        "max_hold_days":    15,
        "initial_capital":  100_000,
        "position_size_pct": 10.0,
    }

    # 1. Load NABIL
    conn = get_db()
    nabil_df = pd.read_sql_query("""
        SELECT date, open, high, low, close, volume,
               price_change_pct, volume_ratio, atr14, market_condition
        FROM   clean_price_history
        WHERE  symbol = 'NABIL'
        ORDER  BY date ASC
    """, conn)
    conn.close()
    print(f"NABIL: {len(nabil_df)} rows loaded")
    print(f"Config: SL={BT_CONFIG['stop_loss_pct']}%  "
          f"TP={BT_CONFIG['take_profit_pct']}%  "
          f"MaxHold={BT_CONFIG['max_hold_days']}d  "
          f"PositionSize={BT_CONFIG['position_size_pct']}%")
    print()

    # 2. Test 3 specific indicators
    get_all_indicator_configs()   # suppress the print by capturing
    test_cfgs = [
        {"name": "RSI_14_30_70",  "type": "momentum",   "indicator": "rsi",      "params": {"period": 14, "oversold": 30, "overbought": 70}},
        {"name": "MACD_12_26_9",  "type": "trend",      "indicator": "macd",     "params": {"fast": 12, "slow": 26, "signal": 9}},
        {"name": "EMA_9_21",      "type": "trend",      "indicator": "ema_cross", "params": {"fast": 9, "slow": 21}},
        {"name": "BB_20_2.0",     "type": "volatility", "indicator": "bollinger", "params": {"period": 20, "std": 2.0}},
        {"name": "OBV_20",        "type": "volume",     "indicator": "obv",       "params": {"ema_period": 20}},
    ]

    spot_results = []
    for cfg in test_cfgs:
        df_sig = generate_signals(nabil_df.copy(), cfg)
        res    = run_backtest(nabil_df, df_sig["signal"], BT_CONFIG)
        res["indicator"] = cfg["name"]
        res["indicator_type"] = cfg["type"]
        spot_results.append(res)

    print(f"{'Indicator':<24} {'Type':<12} {'Trades':>6} {'WinRate':>8} {'PF':>6} {'Return%':>9}  Trades detail")
    print("-" * 90)
    for res in spot_results:
        m = res["metrics"]
        exits = {}
        for t in res["trades"]:
            exits[t["exit_reason"]] = exits.get(t["exit_reason"], 0) + 1
        detail = "  ".join(f"{k}:{v}" for k, v in exits.items())
        print(f"  {res['indicator']:<22} {res['indicator_type']:<12} "
              f"{m['total_trades']:>6} {m['winrate']:>7.1f}% "
              f"{m['profit_factor']:>6.2f} {m['total_return_pct']:>8.2f}%  {detail}")
    print()

    # 3. Full run over all 163 indicators
    answer = input("Run full backtest over all 163 indicators for NABIL? (y/n): ").strip().lower()
    if answer == "y":
        print()
        print(f"Running all indicator configs on {SYMBOL}...")
        all_results = backtest_all_indicators(SYMBOL, BT_CONFIG)
        print()
        print(f"Top 20 indicators by win rate:")
        print_results_table(all_results, n=20)
        print()
        print(f"Bottom 10 indicators by win rate:")
        print_results_table(list(reversed(all_results)), n=10)
        print()
        print(f"Total indicators with trades: {len(all_results)}")
        if all_results:
            best = all_results[0]
            print(f"Best:  {best['indicator']}  winrate={best['metrics']['winrate']:.1f}%  "
                  f"return={best['metrics']['total_return_pct']:.2f}%")
    else:
        print("Skipped full run.")

    print()
    print("[OK] Step 7 complete!")
