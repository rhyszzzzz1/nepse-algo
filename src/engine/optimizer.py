# src/engine/optimizer.py
# Step 8: NEPSE Indicator Optimizer
# Runs backtest_all_indicators() across every symbol in clean_price_history,
# identifies the best-performing indicator config per symbol, and persists
# results to SQLite for the API and dashboard to consume.
#
# Tables written:
#   optimizer_results  — one row per (symbol, indicator) combo
#   optimizer_best     — one row per symbol — the single best indicator config
#
# Ranking metric: composite_score = winrate * 0.4 + profit_factor_capped * 0.3
#                                   + total_return_pct * 0.3
# (winrate alone is misleading when n_trades is tiny)

import sqlite3
from db import get_db
import os
import sys
import traceback
from datetime import datetime

# Force UTF-8 output on Windows (avoids cp932 UnicodeEncodeError)
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import pandas as pd
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from engine.backtester  import backtest_all_indicators, DEFAULT_CONFIG
from engine.indicators  import get_all_indicator_configs

# ── CONFIG ────────────────────────────────────────────────────────────────────
try:
    from config import DEFAULT_BACKTEST_CONFIG as BACKTEST_CONFIG
except ImportError:
    _ROOT3 = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    DB_PATH = os.path.join(_ROOT3, "data", "nepse.db")
    BACKTEST_CONFIG = {
        "stop_loss_pct": 5.0, "take_profit_pct": 10.0,
        "max_hold_days": 15,  "initial_capital": 100_000,
        "position_size_pct": 10.0,
    }


MIN_TRADES = 3   # discard configs with fewer trades (statistically meaningless)



# ── DB HELPERS ────────────────────────────────────────────────────────────────


def create_tables():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS optimizer_results (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol           TEXT NOT NULL,
            indicator        TEXT NOT NULL,
            indicator_type   TEXT,
            total_trades     INTEGER,
            winning_trades   INTEGER,
            losing_trades    INTEGER,
            winrate          REAL,
            profit_factor    REAL,
            avg_win_pct      REAL,
            avg_loss_pct     REAL,
            max_drawdown     REAL,
            total_return_pct REAL,
            consistency      REAL,
            composite_score  REAL,
            run_at           TEXT,
            UNIQUE(symbol, indicator)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS optimizer_best (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol           TEXT NOT NULL UNIQUE,
            best_indicator   TEXT,
            indicator_type   TEXT,
            total_trades     INTEGER,
            winrate          REAL,
            profit_factor    REAL,
            total_return_pct REAL,
            composite_score  REAL,
            run_at           TEXT
        )
    """)
    conn.commit()
    conn.close()
    print("[OK] optimizer tables ready")


# ── COMPOSITE SCORE ───────────────────────────────────────────────────────────
def composite_score(m: dict) -> float:
    """
    Single ranking metric that balances win rate, profit factor, and return.
    Caps profit_factor at 5 to prevent division-heavy configs from dominating.
    """
    pf_capped  = min(m.get("profit_factor", 0), 5.0)
    wr         = m.get("winrate", 0)
    ret        = m.get("total_return_pct", 0)
    trades     = m.get("total_trades", 0)

    # Down-weight configs with very few trades
    trade_conf = min(trades / MIN_TRADES, 1.0)

    score = (wr * 0.40 + pf_capped * 10 * 0.30 + ret * 0.30) * trade_conf
    return round(score, 4)


# ── OPTIMIZE ONE SYMBOL ────────────────────────────────────────────────────────
def optimize_symbol(symbol: str, verbose: bool = False) -> dict | None:
    """
    Run all indicator backtests for *symbol*, persist every result,
    and return the best config dict or None if no valid results.
    """
    results = backtest_all_indicators(symbol, BACKTEST_CONFIG)

    if not results:
        return None

    run_at = datetime.now().isoformat()
    conn   = get_db()
    best_row = None
    best_score = -1e9

    for res in results:
        m = res["metrics"]
        if m["total_trades"] < MIN_TRADES:
            continue

        score = composite_score(m)

        try:
            conn.execute("""
                INSERT OR REPLACE INTO optimizer_results
                (symbol, indicator, indicator_type, total_trades, winning_trades,
                 losing_trades, winrate, profit_factor, avg_win_pct, avg_loss_pct,
                 max_drawdown, total_return_pct, consistency, composite_score, run_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                symbol, res["indicator"], res["indicator_type"],
                m["total_trades"], m["winning_trades"], m["losing_trades"],
                m["winrate"], m["profit_factor"],
                m["avg_win_pct"], m["avg_loss_pct"],
                m["max_drawdown"], m["total_return_pct"],
                m["consistency"], score, run_at,
            ))
        except Exception:
            pass

        if score > best_score:
            best_score = score
            best_row   = res

    conn.commit()

    if best_row:
        m = best_row["metrics"]
        try:
            conn.execute("""
                INSERT OR REPLACE INTO optimizer_best
                (symbol, best_indicator, indicator_type, total_trades,
                 winrate, profit_factor, total_return_pct, composite_score, run_at)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (
                symbol, best_row["indicator"], best_row["indicator_type"],
                m["total_trades"], m["winrate"], m["profit_factor"],
                m["total_return_pct"], best_score, run_at,
            ))
            conn.commit()
        except Exception:
            pass

    conn.close()

    if verbose and best_row:
        m = best_row["metrics"]
        print(f"  {symbol:<8} best={best_row['indicator']:<24} "
              f"score={best_score:.2f}  wr={m['winrate']:.1f}%  "
              f"ret={m['total_return_pct']:.2f}%  trades={m['total_trades']}")

    return best_row


# ── OPTIMIZE ALL SYMBOLS ───────────────────────────────────────────────────────
def optimize_all(limit: int | None = None):
    """
    Run optimize_symbol() for every symbol in clean_price_history.
    *limit* caps the number of symbols (useful for testing).
    """
    conn = get_db()
    symbols = [r[0] for r in conn.execute(
        "SELECT DISTINCT symbol FROM clean_price_history ORDER BY symbol"
    ).fetchall()]
    conn.close()

    if limit:
        symbols = symbols[:limit]

    total   = len(symbols)
    success = 0
    failed  = 0

    print(f"Optimizing {total} symbols x 163 indicators each...")
    print("(this may take a while -- ~10-20 min for all symbols)")
    print()

    for i, symbol in enumerate(symbols, 1):
        try:
            result = optimize_symbol(symbol, verbose=False)
            if result:
                success += 1
                m = result["metrics"]
                if i % 10 == 0 or i == total:
                    print(f"  [{i:>3}/{total}] {symbol:<8}  "
                          f"best={result['indicator']:<22}  "
                          f"wr={m['winrate']:.1f}%  "
                          f"ret={m['total_return_pct']:.2f}%")
            else:
                failed += 1
        except Exception as e:
            print(f"  [{i}/{total}] {symbol}: ERROR - {e}")
            failed += 1

    print()
    print(f"[DONE]  {success} symbols optimized  |  {failed} failed/skipped")
    return success


# ── LEADERBOARD ───────────────────────────────────────────────────────────────
def print_leaderboard(n: int = 30):
    """Print the top-N best symbol×indicator combos from optimizer_best."""
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT symbol, best_indicator, indicator_type,
                   total_trades, winrate, profit_factor,
                   total_return_pct, composite_score
            FROM   optimizer_best
            ORDER  BY composite_score DESC
            LIMIT  ?
        """, (n,)).fetchall()

        if not rows:
            print("  No optimizer results yet.")
            return

        print(f"\n{'#':>3}  {'Symbol':<8} {'Best Indicator':<24} {'Type':<12} "
              f"{'Tr':>4} {'WR%':>6} {'PF':>6} {'Ret%':>7}  {'Score':>7}")
        print("-" * 85)
        for rank, r in enumerate(rows, 1):
            print(f"{rank:>3}  {r[0]:<8} {r[1]:<24} {r[2]:<12} "
                  f"{r[3]:>4} {r[4]:>5.1f}% {r[5]:>6.2f} "
                  f"{r[6]:>6.2f}%  {r[7]:>7.3f}")

        # Indicator popularity
        ind_pop = conn.execute("""
            SELECT best_indicator, COUNT(*) AS n
            FROM   optimizer_best
            GROUP  BY best_indicator
            ORDER  BY n DESC
            LIMIT  10
        """).fetchall()

        print(f"\nMost popular best indicators (across all symbols):")
        for ind, cnt in ind_pop:
            bar = "#" * min(cnt, 40)
            print(f"  {ind:<26} {cnt:>3}  {bar}")

    finally:
        conn.close()


# ── INDICATOR TYPE SUMMARY ────────────────────────────────────────────────────
def print_type_summary():
    """Aggregate win rate and return by indicator type."""
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT indicator_type,
                   COUNT(*)           AS symbols,
                   AVG(winrate)       AS avg_wr,
                   AVG(total_return_pct) AS avg_ret,
                   AVG(composite_score)  AS avg_score
            FROM   optimizer_best
            GROUP  BY indicator_type
            ORDER  BY avg_score DESC
        """).fetchall()

        print(f"\n{'Type':<14} {'Symbols':>7} {'Avg WR%':>8} {'Avg Ret%':>9} {'Avg Score':>10}")
        print("-" * 55)
        for r in rows:
            print(f"  {r[0]:<12} {r[1]:>7} {r[2]:>7.1f}% {r[3]:>8.2f}%  {r[4]:>9.3f}")
    finally:
        conn.close()


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 65)
    print("NEPSE Indicator Optimizer - Step 8")
    print("=" * 65)
    print()

    # 1. Create tables
    create_tables()
    print()

    # 2. Quick test — optimize just NABIL, NICA, UPPER
    test_symbols = ["NABIL", "NICA", "UPPER"]
    print(f"Quick test: optimizing {test_symbols}...")
    for sym in test_symbols:
        optimize_symbol(sym, verbose=True)
    print()

    # 3. Leaderboard so far
    print_leaderboard(n=10)
    print_type_summary()
    print()

    # 4. Optional full run — pass --full flag to skip the prompt
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true", help="Run full optimization on all symbols")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of symbols (for testing)")
    args, _ = parser.parse_known_args()

    conn = get_db()
    n_syms = conn.execute(
        "SELECT COUNT(DISTINCT symbol) FROM clean_price_history"
    ).fetchone()[0]
    conn.close()

    if args.full or args.limit:
        n = args.limit if args.limit else n_syms
        print(f"Running full optimization on {n} symbols...")
        optimize_all(limit=args.limit)
        print()
        print_leaderboard(n=30)
        print_type_summary()
    else:
        print(f"\n{n_syms} symbols available. Run with --full to optimize all, or --limit N to test N symbols.")
        print("Example: py -3.11 src/engine/optimizer.py --full")

    # 5. Final DB summary
    conn = get_db()
    nr = conn.execute("SELECT COUNT(*) FROM optimizer_results").fetchone()[0]
    nb = conn.execute("SELECT COUNT(*) FROM optimizer_best").fetchone()[0]
    conn.close()
    print(f"\noptimizer_results rows: {nr}")
    print(f"optimizer_best rows:    {nb}")
    print("\n[OK] Step 8 complete!")
