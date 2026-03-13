# src/pipeline/run_pipeline.py
# Step 9: NEPSE Full Pipeline Runner
#
# Runs every step of the data pipeline in order:
#   1. Fetch    — pull latest prices, floor sheet, market summary from NEPSE API
#   2. Clean    — remove bad data, compute ATR/volume_ma, tag market condition
#   3. Signals  — compute RSI/MACD/BB/EMA signals for all symbols
#   4. NEPSE    — compute NEPSE-specific signals (pump/dump/liquidity)
#   5. Backtest — re-run backtests for all symbols (optional, slow)
#   6. Optimize — update best indicator per symbol (optional, very slow)
#
# Usage:
#   py -3.11 src/pipeline/run_pipeline.py          # steps 1-4 (fast daily run)
#   py -3.11 src/pipeline/run_pipeline.py --full   # steps 1-6 (weekly, ~30 min)
#   py -3.11 src/pipeline/run_pipeline.py --step 3 # run only step 3

import sys
import os
import time
import argparse
import traceback
from datetime import datetime

# Force UTF-8 on Windows
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

# ── HELPERS ───────────────────────────────────────────────────────────────────
def header(title: str):
    bar = "=" * 60
    print(f"\n{bar}")
    print(f"  {title}")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(bar)

def ok(msg: str):
    print(f"  [OK]  {msg}")

def err(msg: str):
    print(f"  [ERR] {msg}")

def step_timer(fn, label: str) -> bool:
    """Run fn(), print timing. Return True on success."""
    start = time.time()
    try:
        fn()
        elapsed = time.time() - start
        ok(f"{label} — {elapsed:.1f}s")
        return True
    except Exception as e:
        elapsed = time.time() - start
        err(f"{label} FAILED after {elapsed:.1f}s: {e}")
        traceback.print_exc()
        return False


# ── STEP FUNCTIONS ────────────────────────────────────────────────────────────

def step1_fetch():
    """Fetch fresh price/floor-sheet/market-summary data from NEPSE API."""
    header("Step 1: Fetch NEPSE data")
    from data.fetcher import fetch_all_price_history, fetch_market_summary
    from data.broker_fetcher import fetch_sector_indices

    ok("Fetching all price history...")
    fetch_all_price_history()

    ok("Fetching market summary...")
    fetch_market_summary()

    ok("Fetching sector indices...")
    fetch_sector_indices()


def step2_clean():
    """Clean raw price data and tag market conditions."""
    header("Step 2: Clean price data")
    from data.cleaner import clean_all_symbols

    result = clean_all_symbols()
    if isinstance(result, tuple):
        cleaned, skipped = result
        ok(f"Cleaned {cleaned} symbols, skipped {skipped}")
    else:
        ok(f"Clean complete")


def step3_signals():
    """Generate RSI/MACD/BB/EMA signals for all symbols."""
    header("Step 3: Generate technical signals")
    from signals.generator import generate_all_signals

    result = generate_all_signals()
    ok(f"Generated signals: {result}")


def step4_nepse():
    """Compute NEPSE-specific signals (pump/dump/liquidity/volatility)."""
    header("Step 4: NEPSE-specific signals")
    from signals.nepse_signals import calculate_all_signals

    result = calculate_all_signals()
    ok(f"NEPSE signals: {result}")


def step5_backtest(symbols=None):
    """Run backtests for all (or specified) symbols."""
    header("Step 5: Backtest all symbols")
    import sqlite3
    from engine.backtester import backtest_indicator, DEFAULT_CONFIG
    from engine.indicators import get_all_indicator_configs

    db = os.path.join(ROOT, "data", "nepse.db")
    conn = sqlite3.connect(db)

    if symbols:
        syms = symbols
    else:
        syms = [r[0] for r in conn.execute(
            "SELECT DISTINCT symbol FROM clean_price_history ORDER BY symbol"
        ).fetchall()]
    conn.close()

    configs = get_all_indicator_configs()
    # Use best 5 indicators by default to keep this fast
    selected = configs[:5]
    success = 0

    for sym in syms:
        for cfg in selected:
            res = backtest_indicator(sym, cfg, DEFAULT_CONFIG)
            if res and res["metrics"]["total_trades"] > 0:
                success += 1

    ok(f"Backtest complete — {success} successful runs across {len(syms)} symbols")


def step6_optimize(limit=None):
    """Re-optimise best indicator per symbol."""
    header("Step 6: Optimize indicators")
    from engine.optimizer import optimize_all

    n = optimize_all(limit=limit)
    ok(f"Optimized {n} symbols")


# ── SIGNAL REPORT (always shown at the end of daily run) ─────────────────────

def print_signal_report():
    """Print today's top BUY signals as a quick summary."""
    import sqlite3
    db = os.path.join(ROOT, "data", "nepse.db")
    if not os.path.exists(db):
        print("  DB not found:", db)
        return
    conn = sqlite3.connect(db)

    try:
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        if "signals" not in tables:
            print("  signals table not yet created -- run step 3 first.")
            conn.close()
            return

        row = conn.execute("SELECT MAX(date) FROM signals").fetchone()
        sig_date = row[0] if row else None
        if not sig_date:
            print("  No signals in DB yet.")
            conn.close()
            return
    except Exception as e:
        print(f"  Could not query signals table: {e}")
        conn.close()
        return

    rows = conn.execute("""
        SELECT symbol, close, rsi14, volume_ratio, total_score, market_condition, signal
        FROM   signals
        WHERE  date = ? AND signal = 'BUY'
        ORDER  BY total_score DESC
        LIMIT  15
    """, (sig_date,)).fetchall()

    breakdown = conn.execute("""
        SELECT signal, COUNT(*) FROM signals WHERE date = ? GROUP BY signal
    """, (sig_date,)).fetchall()

    conn.close()

    print(f"\n  Signal date: {sig_date}")
    print(f"  Breakdown:   {dict(breakdown)}")
    print()
    print(f"  {'Symbol':<8} {'Close':>8} {'RSI':>6} {'VolR':>6} {'Score':>6} {'Cond':<10}")
    print(f"  {'-'*55}")
    for r in rows:
        print(f"  {r[0]:<8} {r[1]:>8,.0f} {r[2]:>6.1f} {r[3]:>6.2f} {r[4]:>+6} {r[5]:<10}")


# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NEPSE Pipeline Runner")
    parser.add_argument("--full",   action="store_true",
                        help="Run all 6 steps including backtest + optimize (slow)")
    parser.add_argument("--step",   type=int, default=None,
                        help="Run only a specific step number (1-6)")
    parser.add_argument("--skip-fetch", action="store_true",
                        help="Skip step 1 (fetch) — useful when API is unavailable")
    parser.add_argument("--limit",  type=int, default=None,
                        help="Limit symbols processed in step 5/6 (for testing)")
    args = parser.parse_args()

    pipeline_start = time.time()
    results = {}

    print("=" * 60)
    print("  NEPSE Algorithmic Trading Pipeline - Step 9")
    print(f"  Mode: {'FULL (all 6 steps)' if args.full else 'DAILY (steps 1-4)' if not args.step else f'SINGLE step {args.step}'}")
    print("=" * 60)

    if args.step:
        # Single step mode
        step_map = {
            1: ("Fetch",     lambda: step1_fetch()),
            2: ("Clean",     lambda: step2_clean()),
            3: ("Signals",   lambda: step3_signals()),
            4: ("NEPSE sig", lambda: step4_nepse()),
            5: ("Backtest",  lambda: step5_backtest()),
            6: ("Optimize",  lambda: step6_optimize(args.limit)),
        }
        if args.step in step_map:
            label, fn = step_map[args.step]
            results[args.step] = step_timer(fn, label)
        else:
            print(f"Unknown step {args.step}. Valid steps: 1-6")
            sys.exit(1)

    else:
        # Default daily run: steps 1-4 (skip fetch if --skip-fetch)
        if not args.skip_fetch:
            results[1] = step_timer(step1_fetch, "Fetch NEPSE data")
        else:
            print("  [--] Step 1 skipped (--skip-fetch)")
            results[1] = None

        results[2] = step_timer(step2_clean,   "Clean price data")
        results[3] = step_timer(step3_signals, "Generate signals")
        results[4] = step_timer(step4_nepse,   "NEPSE signals")

        if args.full:
            results[5] = step_timer(lambda: step5_backtest(), "Backtest")
            results[6] = step_timer(lambda: step6_optimize(args.limit), "Optimize")

    # Summary
    total_elapsed = time.time() - pipeline_start
    print()
    print("=" * 60)
    print("  PIPELINE SUMMARY")
    print("=" * 60)
    for step_num, success in results.items():
        status = "[OK]  " if success else ("[SKIP]" if success is None else "[FAIL]")
        names  = {1:"Fetch", 2:"Clean", 3:"Signals",
                  4:"NEPSE signals", 5:"Backtest", 6:"Optimize"}
        print(f"  {status} Step {step_num}: {names.get(step_num,'')}")

    print(f"\n  Total time: {total_elapsed:.1f}s")

    # Show today's top signals
    print()
    print("=" * 60)
    print("  TOP BUY SIGNALS")
    print("=" * 60)
    print_signal_report()

    print()
    print("[OK] Pipeline complete!")
