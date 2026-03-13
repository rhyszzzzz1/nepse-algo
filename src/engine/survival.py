# src/engine/survival.py
# Step 10: Survival Tests — filter overfitted indicator rules
#
# A rule must pass ALL 3 gates to survive:
#   Gate 1 — Minimum win rate on full history
#   Gate 2 — Walk-forward: train-period params must hold on unseen test period
#   Gate 3 — Market regime: profitable in >=2 of 3 regimes (bull/bear/sideways)
#
# Additionally: Monte Carlo simulation shuffles trade returns 1000x to estimate
# real-world worst-case drawdown and win rate.

import os
import sys
import sqlite3

import numpy as np
import pandas as pd

# Force UTF-8 on Windows
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ROOT = os.path.dirname(  # project root  (c:\nepse-algo)
    os.path.dirname(      # src
    os.path.dirname(os.path.abspath(__file__))))  # src/engine
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "src"))

from engine.backtester import run_backtest, DEFAULT_CONFIG
from engine.indicators import generate_signals

DB_PATH = os.path.join(ROOT, "data", "nepse.db")


# ── DB HELPER ─────────────────────────────────────────────────────────────────
def _get_db():
    return sqlite3.connect(DB_PATH)


def _load_symbol(symbol: str) -> pd.DataFrame | None:
    conn = _get_db()
    try:
        df = pd.read_sql_query("""
            SELECT date, open, high, low, close, volume,
                   price_change_pct, volume_ratio, atr14, market_condition
            FROM   clean_price_history
            WHERE  symbol = ?
            ORDER  BY date ASC
        """, conn, params=(symbol,))
        return df if len(df) >= 30 else None
    finally:
        conn.close()


# ── DRAWDOWN HELPER ───────────────────────────────────────────────────────────
def _equity_max_drawdown(trade_pnl_pcts: list[float],
                          initial: float = 100_000,
                          pos_size: float = 0.10) -> float:
    """Approximate max drawdown from a sequence of trade PnL %."""
    equity = initial
    peak   = initial
    max_dd = 0.0
    for pct in trade_pnl_pcts:
        invest  = equity * pos_size
        equity += invest * pct / 100.0
        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak * 100.0 if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd
    return round(max_dd, 4)


# ── GATE 1: MINIMUM WIN RATE ───────────────────────────────────────────────────
def gate1_minimum_winrate(backtest_result: dict,
                           threshold: float = 0.48) -> dict:
    """
    Gate 1: total win rate on the full history must meet *threshold*.

    Parameters
    ----------
    backtest_result : result dict from run_backtest()
    threshold       : decimal (default 0.48 = 48%)

    Returns
    -------
    {'passed': bool, 'winrate': float, 'threshold': float,
     'total_trades': int, 'winning_trades': int}
    """
    m  = backtest_result.get("metrics", {})
    wr = m.get("winrate", 0.0) / 100.0   # convert to 0-1 range

    return {
        "passed":         wr >= threshold,
        "winrate":        round(wr * 100, 2),
        "threshold":      round(threshold * 100, 2),
        "total_trades":   m.get("total_trades", 0),
        "winning_trades": m.get("winning_trades", 0),
    }


# ── GATE 2: WALK-FORWARD TEST ─────────────────────────────────────────────────
def gate2_walk_forward(symbol: str,
                        indicator_config: dict,
                        backtest_config: dict | None = None,
                        split: float = 0.70,
                        test_threshold: float = 0.45) -> dict:
    """
    Gate 2: Walk-forward validation.

    Splits the price history at *split* by date (temporal, never random).
    Backtests on the TRAIN portion → then re-runs on the TEST portion with
    the same indicator params (no re-fitting between periods).

    A rule survives if the test-period win rate >= *test_threshold*.

    Returns
    -------
    {'passed': bool, 'train_winrate': float, 'test_winrate': float,
     'train_trades': int, 'test_trades': int,
     'split_date': str, 'threshold': float}
    """
    if backtest_config is None:
        backtest_config = DEFAULT_CONFIG.copy()

    df = _load_symbol(symbol)
    if df is None or len(df) < 60:
        return {"passed": False, "error": "insufficient data",
                "train_winrate": 0, "test_winrate": 0,
                "train_trades": 0, "test_trades": 0}

    split_idx  = int(len(df) * split)
    df_train   = df.iloc[:split_idx].reset_index(drop=True)
    df_test    = df.iloc[split_idx:].reset_index(drop=True)
    split_date = df.iloc[split_idx]["date"] if split_idx < len(df) else "N/A"

    # Train
    try:
        sig_train  = generate_signals(df_train.copy(), indicator_config)["signal"]
        res_train  = run_backtest(df_train, sig_train, backtest_config)
        train_wr   = res_train["metrics"]["winrate"]
        train_tr   = res_train["metrics"]["total_trades"]
    except Exception:
        train_wr, train_tr = 0.0, 0

    # Test — same params, unseen data
    try:
        sig_test   = generate_signals(df_test.copy(), indicator_config)["signal"]
        res_test   = run_backtest(df_test, sig_test, backtest_config)
        test_wr    = res_test["metrics"]["winrate"]
        test_tr    = res_test["metrics"]["total_trades"]
    except Exception:
        test_wr, test_tr = 0.0, 0

    passed = (test_wr / 100.0) >= test_threshold and test_tr >= 2

    return {
        "passed":        passed,
        "train_winrate": round(train_wr, 2),
        "test_winrate":  round(test_wr, 2),
        "train_trades":  train_tr,
        "test_trades":   test_tr,
        "split_date":    str(split_date),
        "threshold":     round(test_threshold * 100, 2),
    }


# ── GATE 3: MARKET REGIME TEST ────────────────────────────────────────────────
def gate3_regime_test(symbol: str,
                       indicator_config: dict,
                       backtest_config: dict | None = None,
                       min_profitable_regimes: int = 2) -> dict:
    """
    Gate 3: Must be profitable in at least *min_profitable_regimes* of 3
    market regimes (bull / bear / sideways).

    Each regime is tested independently using only the rows labelled
    with that market_condition.

    Returns
    -------
    {'passed': bool, 'bull': {...}, 'bear': {...}, 'sideways': {...},
     'profitable_regimes': int}
    """
    if backtest_config is None:
        backtest_config = DEFAULT_CONFIG.copy()

    df = _load_symbol(symbol)
    if df is None:
        return {"passed": False, "error": "no data",
                "bull": {}, "bear": {}, "sideways": {}}

    regime_results = {}
    profitable     = 0

    for regime in ("bull", "bear", "sideways"):
        df_r = df[df["market_condition"] == regime].reset_index(drop=True)
        if len(df_r) < 20:
            regime_results[regime] = {
                "winrate": None, "total_trades": 0,
                "total_return_pct": None, "skipped": True
            }
            continue

        try:
            sig_r  = generate_signals(df_r.copy(), indicator_config)["signal"]
            res_r  = run_backtest(df_r, sig_r, backtest_config)
            m      = res_r["metrics"]
            wr     = m["winrate"]
            ret    = m["total_return_pct"]
            tr     = m["total_trades"]

            regime_results[regime] = {
                "winrate":          round(wr, 2),
                "total_trades":     tr,
                "total_return_pct": round(ret, 4),
                "skipped":          False,
            }
            if ret > 0 and tr >= 2:
                profitable += 1
        except Exception as e:
            regime_results[regime] = {
                "winrate": None, "total_trades": 0,
                "total_return_pct": None, "skipped": True, "error": str(e)
            }

    passed = profitable >= min_profitable_regimes
    return {
        "passed":              passed,
        "profitable_regimes":  profitable,
        "min_required":        min_profitable_regimes,
        "bull":                regime_results.get("bull", {}),
        "bear":                regime_results.get("bear", {}),
        "sideways":            regime_results.get("sideways", {}),
    }


# ── MONTE CARLO SIMULATION ────────────────────────────────────────────────────
def monte_carlo(trades: list[dict],
                n_simulations: int = 1000,
                confirmed_threshold: float = 0.40) -> dict:
    """
    Monte Carlo simulation: randomly re-order trade sequence *n_simulations*
    times and measure win rate + drawdown distribution.

    Parameters
    ----------
    trades              : list of trade dicts from run_backtest() output
    n_simulations       : number of random shuffles (default 1000)
    confirmed_threshold : 5th-percentile win rate must meet this (default 40%)

    Returns
    -------
    {'median_winrate': float, 'p5_winrate': float,
     'median_drawdown': float, 'p95_drawdown': float,
     'confirmed': bool, 'n_trades': int, 'n_simulations': int}
    """
    if not trades:
        return {
            "median_winrate": 0.0, "p5_winrate": 0.0,
            "median_drawdown": 0.0, "p95_drawdown": 0.0,
            "confirmed": False, "n_trades": 0,
            "n_simulations": n_simulations,
        }

    pnl_pcts = np.array([t["pnl_pct"] for t in trades], dtype=float)
    n        = len(pnl_pcts)

    sim_winrates  = np.empty(n_simulations)
    sim_drawdowns = np.empty(n_simulations)

    rng = np.random.default_rng(seed=42)

    for i in range(n_simulations):
        shuffled  = rng.permutation(pnl_pcts)
        wins      = np.sum(shuffled > 0)
        sim_winrates[i] = wins / n * 100.0

        # Approximate equity curve drawdown
        equity    = 100_000.0
        peak      = equity
        max_dd    = 0.0
        for pct in shuffled:
            invest  = equity * 0.10
            equity += invest * pct / 100.0
            if equity > peak:
                peak = equity
            dd = (peak - equity) / peak * 100.0 if peak > 0 else 0.0
            if dd > max_dd:
                max_dd = dd
        sim_drawdowns[i] = max_dd

    p5_wr   = float(np.percentile(sim_winrates,  5))
    confirmed = (p5_wr / 100.0) >= confirmed_threshold

    return {
        "median_winrate":  round(float(np.median(sim_winrates)),  2),
        "p5_winrate":      round(p5_wr,                           2),
        "median_drawdown": round(float(np.median(sim_drawdowns)), 4),
        "p95_drawdown":    round(float(np.percentile(sim_drawdowns, 95)), 4),
        "confirmed":       confirmed,
        "n_trades":        n,
        "n_simulations":   n_simulations,
        "threshold":       round(confirmed_threshold * 100, 2),
    }


# ── COMPOSITE: RUN ALL SURVIVAL TESTS ────────────────────────────────────────
def run_all_survival_tests(symbol: str,
                            indicator_config: dict,
                            backtest_config: dict | None = None,
                            gate1_threshold: float = 0.48,
                            gate2_threshold: float = 0.45,
                            mc_threshold:   float = 0.40,
                            n_simulations:  int   = 1000) -> dict:
    """
    Run all 3 survival gates + Monte Carlo for one symbol×indicator combo.

    A rule is marked *survived=True* only if ALL 3 gates pass.
    Monte Carlo confirmation is informational but does not block survival.

    Returns
    -------
    {
      'survived':    bool,
      'symbol':      str,
      'indicator':   str,
      'gate1':       gate1 result dict,
      'gate2':       gate2 result dict,
      'gate3':       gate3 result dict,
      'monte_carlo': mc result dict,
    }
    """
    if backtest_config is None:
        backtest_config = DEFAULT_CONFIG.copy()

    # Need a full backtest for gate1 + monte carlo
    df = _load_symbol(symbol)
    if df is None:
        return {"survived": False, "symbol": symbol,
                "indicator": indicator_config.get("name", "?"),
                "error": "insufficient data"}

    try:
        df_sig   = generate_signals(df.copy(), indicator_config)
        full_res = run_backtest(df, df_sig["signal"], backtest_config)
    except Exception as e:
        return {"survived": False, "symbol": symbol,
                "indicator": indicator_config.get("name", "?"),
                "error": str(e)}

    g1 = gate1_minimum_winrate(full_res, threshold=gate1_threshold)
    g2 = gate2_walk_forward(symbol, indicator_config, backtest_config,
                             test_threshold=gate2_threshold)
    g3 = gate3_regime_test(symbol, indicator_config, backtest_config)
    mc = monte_carlo(full_res.get("trades", []),
                     n_simulations=n_simulations,
                     confirmed_threshold=mc_threshold)

    survived = g1["passed"] and g2["passed"] and g3["passed"]

    return {
        "survived":    survived,
        "symbol":      symbol,
        "indicator":   indicator_config.get("name", "?"),
        "gate1":       g1,
        "gate2":       g2,
        "gate3":       g3,
        "monte_carlo": mc,
    }


# ── PRETTY PRINT ─────────────────────────────────────────────────────────────
def print_survival_report(result: dict):
    """Print a formatted survival report for one symbol×indicator."""
    sym  = result.get("symbol", "?")
    ind  = result.get("indicator", "?")
    surv = result.get("survived", False)

    badge = "SURVIVED" if surv else "REJECTED"
    bar   = "=" * 60
    print(f"\n{bar}")
    print(f"  {sym} x {ind}  -->  [{badge}]")
    print(bar)

    if "error" in result:
        print(f"  ERROR: {result['error']}")
        return

    g1 = result["gate1"]
    g2 = result["gate2"]
    g3 = result["gate3"]
    mc = result["monte_carlo"]

    ok1 = "[PASS]" if g1["passed"] else "[FAIL]"
    ok2 = "[PASS]" if g2["passed"] else "[FAIL]"
    ok3 = "[PASS]" if g3["passed"] else "[FAIL]"
    okm = "[CONF]" if mc["confirmed"] else "[WARN]"

    print(f"  {ok1} Gate 1 - Min Win Rate  : {g1['winrate']:.1f}%  "
          f"(need >={g1['threshold']:.0f}%, trades={g1['total_trades']})")

    print(f"  {ok2} Gate 2 - Walk-Forward  : "
          f"train={g2.get('train_winrate', 0):.1f}%  "
          f"test={g2.get('test_winrate', 0):.1f}%  "
          f"(need >={g2.get('threshold', 45):.0f}% on test,  "
          f"split={g2.get('split_date', '?')})")

    def _fmt_regime(r):
        if not r or r.get("skipped"):
            return "skipped"
        return (f"wr={r['winrate']:.0f}%  "
                f"ret={r['total_return_pct']:+.2f}%  "
                f"tr={r['total_trades']}")

    print(f"  {ok3} Gate 3 - Regime Test   : "
          f"profitable in {g3['profitable_regimes']}/{g3['min_required']} regimes")
    print(f"         bull    : {_fmt_regime(g3.get('bull', {}))}")
    print(f"         bear    : {_fmt_regime(g3.get('bear', {}))}")
    print(f"         sideways: {_fmt_regime(g3.get('sideways', {}))}")

    print(f"  {okm} Monte Carlo ({mc['n_simulations']}x): "
          f"median_wr={mc['median_winrate']:.1f}%  "
          f"p5_wr={mc['p5_winrate']:.1f}%  "
          f"p95_drawdown={mc['p95_drawdown']:.2f}%  "
          f"(need p5>={mc['threshold']:.0f}%)")

    print(f"\n  Result: {'ALL GATES PASSED -- rule is robust' if surv else 'One or more gates failed -- rule rejected'}")
    print(bar)


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("NEPSE Survival Tests - Step 10")
    print("=" * 60)
    print()

    SYMBOL = "NABIL"

    BT_CONFIG = {
        "stop_loss_pct":     5.0,
        "take_profit_pct":  10.0,
        "max_hold_days":    15,
        "initial_capital":  100_000,
        "position_size_pct": 10.0,
    }

    # Test 5 indicator configs
    test_configs = [
        {"name": "EMA_9_21",   "type": "trend",      "indicator": "ema_cross",  "params": {"fast": 9,  "slow": 21}},
        {"name": "OBV_20",     "type": "volume",     "indicator": "obv",        "params": {"ema_period": 20}},
        {"name": "RSI_21_35_65","type": "momentum",  "indicator": "rsi",        "params": {"period": 21, "oversold": 35, "overbought": 65}},
        {"name": "MACD_12_26_9","type": "trend",     "indicator": "macd",       "params": {"fast": 12, "slow": 26, "signal": 9}},
        {"name": "BB_20_2.0",  "type": "volatility", "indicator": "bollinger",  "params": {"period": 20, "std": 2.0}},
    ]

    passed_count = 0
    for cfg in test_configs:
        print(f"Testing {SYMBOL} x {cfg['name']} ...")
        result = run_all_survival_tests(SYMBOL, cfg, BT_CONFIG, n_simulations=1000)
        print_survival_report(result)
        if result.get("survived"):
            passed_count += 1

    print()
    print(f"Summary: {passed_count}/{len(test_configs)} rules survived all 3 gates.")
    print()
    print("[OK] Step 10 complete!")
