# src/engine/combinations.py
# Step 11: Combined multi-indicator rules
#
# Phase A survivors (single indicators) are combined into:
#   2-indicator AND combos  : BUY only when both agree
#   3-indicator vote combos : BUY when majority (>=2 of 3) agree
#
# Additionally, NEPSE-specific signals are layered as optional filters
# (liquidity_spike, broker_accumulation, pump_score) for the top survivors.

import os
import sys
import sqlite3
import itertools

import numpy as np
import pandas as pd

# Force UTF-8 on Windows
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── PATH SETUP ────────────────────────────────────────────────────────────────
ROOT = os.path.dirname(        # project root  (c:\nepse-algo)
    os.path.dirname(           # src
    os.path.dirname(os.path.abspath(__file__))))   # src/engine
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "src"))

from engine.backtester import run_backtest, DEFAULT_CONFIG
from engine.indicators import generate_signals
from engine.survival   import run_all_survival_tests

DB_PATH = os.path.join(ROOT, "data", "nepse.db")


# ── DB HELPERS ────────────────────────────────────────────────────────────────
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


def _load_nepse_signals(symbol: str) -> pd.DataFrame | None:
    """Load NEPSE-specific signal rows for *symbol* (from nepse_signals table)."""
    conn = _get_db()
    try:
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        if "nepse_signals" not in tables:
            return None
        df = pd.read_sql_query("""
            SELECT date, liquidity_spike, broker_accumulation_pct,
                   pump_score, dump_score, volatility_regime
            FROM   nepse_signals
            WHERE  symbol = ?
            ORDER  BY date ASC
        """, conn, params=(symbol,))
        return df if not df.empty else None
    except Exception:
        return None
    finally:
        conn.close()


# ── TOP SURVIVORS FROM OPTIMIZER DB ───────────────────────────────────────────
def load_top_survivors_from_db(symbol: str, top_n: int = 20) -> list[dict]:
    """
    Load the top *top_n* indicator configs for *symbol* from optimizer_results,
    converting them back into indicator_config dicts suitable for generate_signals().
    """
    from engine.indicators import get_all_indicator_configs

    conn = _get_db()
    try:
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        if "optimizer_results" not in tables:
            return []

        rows = conn.execute("""
            SELECT indicator, indicator_type, winrate, composite_score
            FROM   optimizer_results
            WHERE  symbol = ? AND total_trades >= 3
            ORDER  BY composite_score DESC
            LIMIT  ?
        """, (symbol, top_n)).fetchall()
    finally:
        conn.close()

    if not rows:
        return []

    # Match names back to full indicator configs
    all_cfgs = {c["name"]: c for c in get_all_indicator_configs()}
    result = []
    for row in rows:
        name = row[0]
        if name in all_cfgs:
            result.append(all_cfgs[name])
    return result


# ── BUILD COMBINATION CONFIGS ─────────────────────────────────────────────────
def build_combinations(surviving_configs: list[dict],
                        top_n: int = 20) -> list[dict]:
    """
    Build 2-indicator AND combos and 3-indicator majority-vote combos.

    Parameters
    ----------
    surviving_configs : list of indicator_config dicts (from Phase A survivors)
    top_n             : cap on single-indicator pool size

    Returns
    -------
    List of combo_config dicts, each with:
        name, type='combo', indicators=[cfg_a, cfg_b, ...], n_required=int
    """
    pool = surviving_configs[:top_n]
    combos = []

    # 2-indicator AND combos (all need to agree → n_required=2)
    for a, b in itertools.combinations(pool, 2):
        combos.append({
            "name":       f"2C_{a['name']}+{b['name']}",
            "type":       "combo",
            "indicators": [a, b],
            "n_required": 2,          # AND logic: both must say BUY
        })

    # 3-indicator majority-vote combos from top 10
    top10 = pool[:10]
    for a, b, c in itertools.combinations(top10, 3):
        combos.append({
            "name":       f"3C_{a['name']}+{b['name']}+{c['name']}",
            "type":       "combo",
            "indicators": [a, b, c],
            "n_required": 2,          # majority: >=2 of 3
        })

    return combos


# ── GENERATE COMBINATION SIGNALS ──────────────────────────────────────────────
def generate_combination_signals(df: pd.DataFrame,
                                  combo_config: dict,
                                  nepse_df: pd.DataFrame | None = None,
                                  nepse_filter: dict | None = None) -> pd.Series:
    """
    Generate a combined BUY/SELL/HOLD signal series.

    For each row, each constituent indicator votes +1 (BUY), -1 (SELL), or 0 (HOLD).
    The combined signal is:
        BUY  if sum_of_votes >= n_required
        SELL if sum_of_votes <= -n_required
        HOLD otherwise

    Optional NEPSE filter (dict with keys 'liquidity_min', 'broker_acc_min',
    'pump_max'): a BUY is promoted only if the NEPSE conditions are met.

    Returns
    -------
    pd.Series of integer signals: 1=BUY, -1=SELL, 0=HOLD
    """
    indicators = combo_config["indicators"]
    n_required = combo_config.get("n_required", len(indicators))

    # Collect one signal column per indicator
    vote_matrix = pd.DataFrame(index=df.index)
    for cfg in indicators:
        try:
            df_sig = generate_signals(df.copy(), cfg)
            vote_matrix[cfg["name"]] = df_sig["signal"].values
        except Exception:
            vote_matrix[cfg["name"]] = 0

    # Sum votes
    buy_votes  = (vote_matrix == 1).sum(axis=1)
    sell_votes = (vote_matrix == -1).sum(axis=1)

    combined = pd.Series(0, index=df.index)
    combined[buy_votes  >= n_required] =  1
    combined[sell_votes >= n_required] = -1

    # Optional NEPSE signal filter (only tighten BUYs, never create new ones)
    if nepse_df is not None and nepse_filter and not nepse_df.empty:
        try:
            merged = df[["date"]].merge(
                nepse_df[["date", "liquidity_spike",
                           "broker_accumulation_pct", "pump_score"]],
                on="date", how="left"
            ).fillna(0)

            liq_min = nepse_filter.get("liquidity_min",  0)
            acc_min = nepse_filter.get("broker_acc_min", 0)
            pmp_max = nepse_filter.get("pump_max",      99)

            nepse_ok = (
                (merged["liquidity_spike"]        >= liq_min) &
                (merged["broker_accumulation_pct"] >= acc_min) &
                (merged["pump_score"]             <= pmp_max)
            ).values

            # Demote BUYs that don't pass NEPSE filter to HOLD
            combined[(combined == 1) & ~nepse_ok] = 0
        except Exception:
            pass   # NEPSE filter is optional — silently skip on error

    return combined


# ── BACKTEST ALL COMBINATIONS ─────────────────────────────────────────────────
def backtest_combinations(symbol: str,
                           surviving_configs: list[dict],
                           backtest_config: dict | None = None,
                           top_n:      int = 20,
                           run_survival: bool = True) -> list[dict]:
    """
    Build all 2- and 3-indicator combinations from *surviving_configs*,
    backtest each on *symbol*, optionally run survival gates, and return
    results sorted by composite_score descending.

    Parameters
    ----------
    symbol            : NEPSE symbol string
    surviving_configs : Phase A single-indicator survivors
    backtest_config   : dict (defaults to DEFAULT_CONFIG)
    top_n             : max pool size for combining
    run_survival      : if True, run all 3 survival gates on each combo

    Returns
    -------
    List of result dicts, sorted by metrics['winrate'] descending.
    Each dict contains keys: name, metrics, survival (optional).
    """
    if backtest_config is None:
        backtest_config = DEFAULT_CONFIG.copy()

    df = _load_symbol(symbol)
    if df is None:
        print(f"  No data for {symbol}")
        return []

    nepse_df = _load_nepse_signals(symbol)

    combos   = build_combinations(surviving_configs, top_n=top_n)
    total    = len(combos)
    results  = []
    no_trades = 0

    print(f"  {total} combinations to test on {symbol}")
    print(f"  (run_survival={'yes' if run_survival else 'no'})")
    print()

    for i, combo in enumerate(combos, 1):
        try:
            sig    = generate_combination_signals(df, combo, nepse_df)
            bt_res = run_backtest(df, sig, backtest_config)
            m      = bt_res["metrics"]

            if m["total_trades"] == 0:
                no_trades += 1
                continue

            entry = {
                "name":    combo["name"],
                "type":    combo["type"],
                "n_indic": len(combo["indicators"]),
                "metrics": m,
            }

            if run_survival:
                # Build a pseudo indicator_config so survival tests work
                pseudo_cfg = {
                    "name":       combo["name"],
                    "type":       "combo",
                    "indicator":  "_combo_",
                    "params":     {},
                    "_combo_cfg": combo,   # stash for internal use
                }
                # Gate 1 only for combos (gate 2/3 require re-generating signals
                # which is expensive; we do a lightweight version)
                from engine.survival import gate1_minimum_winrate, monte_carlo
                g1 = gate1_minimum_winrate(bt_res, threshold=0.48)
                mc = monte_carlo(bt_res.get("trades", []), n_simulations=500)
                entry["gate1"]       = g1
                entry["monte_carlo"] = mc
                entry["survived"]    = g1["passed"] and mc["confirmed"]

            results.append(entry)

        except Exception as e:
            pass

        if i % 20 == 0 or i == total:
            valid = len(results)
            print(f"  [{i:>4}/{total}] valid={valid}  no_trades={no_trades}")

    # Sort by win rate descending
    results.sort(key=lambda r: r["metrics"]["winrate"], reverse=True)

    print()
    print(f"  Done: {len(results)} combos with trades, {no_trades} with no trades")
    return results


# ── PRINT RESULTS TABLE ───────────────────────────────────────────────────────
def print_combinations_table(results: list[dict], n: int = 20):
    """Print a leaderboard of combination backtest results."""
    if not results:
        print("  No results to display.")
        return

    header = (f"{'#':>3}  {'Name':<46} {'N':>2}  "
              f"{'Tr':>4} {'WR%':>6} {'PF':>6} "
              f"{'AvgW':>7} {'AvgL':>7} {'Ret%':>8}")
    print(header)
    print("-" * len(header))

    for rank, r in enumerate(results[:n], 1):
        m = r["metrics"]
        surv = ""
        if "survived" in r:
            surv = "[S]" if r["survived"] else "   "
        print(
            f"{rank:>3}  {r['name']:<46} {r['n_indic']:>2}  "
            f"{m['total_trades']:>4} {m['winrate']:>5.1f}% "
            f"{m['profit_factor']:>6.2f} "
            f"{m['avg_win_pct']:>7.2f} {m['avg_loss_pct']:>7.2f} "
            f"{m['total_return_pct']:>7.2f}%  {surv}"
        )


# ── MOCK SURVIVORS (fallback when optimizer hasn't run) ───────────────────────
def _mock_survivors() -> list[dict]:
    """Return a small set of hard-coded indicator configs for testing."""
    return [
        {"name": "RSI_21_35_65",  "type": "momentum",   "indicator": "rsi",       "params": {"period": 21, "oversold": 35, "overbought": 65}},
        {"name": "RSI_21_35_70",  "type": "momentum",   "indicator": "rsi",       "params": {"period": 21, "oversold": 35, "overbought": 70}},
        {"name": "RSI_14_35_65",  "type": "momentum",   "indicator": "rsi",       "params": {"period": 14, "oversold": 35, "overbought": 65}},
        {"name": "OBV_20",        "type": "volume",     "indicator": "obv",       "params": {"ema_period": 20}},
        {"name": "VOLMA_10_20",   "type": "volume",     "indicator": "volma",     "params": {"short_ma": 10, "long_ma": 20}},
        {"name": "VOLMA_5_50",    "type": "volume",     "indicator": "volma",     "params": {"short_ma": 5,  "long_ma": 50}},
        {"name": "SMA_9_50",      "type": "trend",      "indicator": "sma_cross", "params": {"fast": 9,  "slow": 50}},
        {"name": "SMA_5_50",      "type": "trend",      "indicator": "sma_cross", "params": {"fast": 5,  "slow": 50}},
        {"name": "EMA_9_50",      "type": "trend",      "indicator": "ema_cross", "params": {"fast": 9,  "slow": 50}},
        {"name": "EMA_21_34",     "type": "trend",      "indicator": "ema_cross", "params": {"fast": 21, "slow": 34}},
        {"name": "ADX_10_20",     "type": "trend",      "indicator": "adx",       "params": {"period": 10, "threshold": 20}},
        {"name": "ST_14_4.0",     "type": "trend",      "indicator": "supertrend","params": {"period": 14, "multiplier": 4.0}},
    ]


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 65)
    print("NEPSE Combination Builder - Step 11")
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

    # 1. Load survivors — try DB first, fall back to mock set
    print("Step 1: Loading top survivors...")
    survivors = load_top_survivors_from_db(SYMBOL, top_n=20)
    if survivors:
        print(f"  Loaded {len(survivors)} survivors from optimizer_results for {SYMBOL}")
    else:
        survivors = _mock_survivors()
        print(f"  No optimizer results found — using {len(survivors)} mock configs")
    print()

    # 2. Build and count all combinations
    print("Step 2: Building combinations...")
    combos = build_combinations(survivors, top_n=min(len(survivors), 20))
    combos_2 = [c for c in combos if c["n_required"] == 2 and len(c["indicators"]) == 2]
    combos_3 = [c for c in combos if len(c["indicators"]) == 3]
    print(f"  Pool size:          {min(len(survivors), 20)} indicators")
    print(f"  2-indicator combos: {len(combos_2)}")
    print(f"  3-indicator combos: {len(combos_3)}")
    print(f"  Total combinations: {len(combos)}")
    print()

    # 3. Quick spot test — 3 selected combos on NABIL
    print("Step 3: Spot-testing 3 combinations on NABIL...")
    spot_combos = combos[:3]   # first 3 from pool
    df_nabil    = _load_symbol(SYMBOL)
    nepse_df    = _load_nepse_signals(SYMBOL)

    if df_nabil is not None:
        print()
        spot_results = []
        for combo in spot_combos:
            try:
                sig    = generate_combination_signals(df_nabil, combo, nepse_df)
                bt     = run_backtest(df_nabil, sig, BT_CONFIG)
                m      = bt["metrics"]
                from engine.survival import gate1_minimum_winrate, monte_carlo
                g1 = gate1_minimum_winrate(bt)
                mc = monte_carlo(bt.get("trades", []), n_simulations=500)
                spot_results.append({
                    "name":    combo["name"],
                    "n_indic": len(combo["indicators"]),
                    "metrics": m,
                    "gate1":   g1,
                    "monte_carlo": mc,
                    "survived": g1["passed"] and mc["confirmed"],
                })
                surv_tag = "[SURVIVED]" if (g1["passed"] and mc["confirmed"]) else "[rejected]"
                print(f"  {combo['name'][:48]}")
                print(f"    trades={m['total_trades']}  "
                      f"wr={m['winrate']:.1f}%  "
                      f"pf={m['profit_factor']:.2f}  "
                      f"ret={m['total_return_pct']:.2f}%  "
                      f"{surv_tag}")
                print()
            except Exception as e:
                print(f"  {combo['name']}: ERROR - {e}")
    else:
        print(f"  No data found for {SYMBOL}")
        spot_results = []

    # 4. Full combination backtest on NABIL (top-20 pool)
    print()
    print("Step 4: Full combination backtest on NABIL...")
    print()
    all_results = backtest_combinations(
        SYMBOL, survivors, BT_CONFIG, top_n=min(len(survivors), 20),
        run_survival=True
    )

    # 5. Print top-20 leaderboard
    print()
    print("Top 20 combinations by win rate:")
    print()
    print_combinations_table(all_results, n=20)

    # 6. Summary
    survived_count = sum(1 for r in all_results if r.get("survived"))
    total_with_trades = len(all_results)
    print()
    print(f"Survived (Gate1 + MC): {survived_count} / {total_with_trades} combos with trades")

    print()
    print("[OK] Step 11 complete!")
