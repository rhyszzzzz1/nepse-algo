# src/engine/scorer.py
# Step 12: Final Rule Scorer & Storage
#
# Weighted scoring formula (0-100):
#   score = winrate_pct        * 0.35
#         + pf_normalized      * 0.30   (profit_factor capped at 3.0)
#         + consistency        * 0.20   (% quarters profitable)
#         + low_drawdown_score * 0.15   (inverted drawdown)
#
# Surviving rules (single + combination) are stored in trading_rules table.

import os
import sys
import sqlite3
import json
from datetime import datetime

import pandas as pd

# Force UTF-8 on Windows
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── PATH SETUP ────────────────────────────────────────────────────────────────
ROOT = os.path.dirname(        # project root  (c:\nepse-algo)
    os.path.dirname(           # src
    os.path.dirname(os.path.abspath(__file__))))  # src/engine
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "src"))

from engine.backtester   import backtest_all_indicators, run_backtest, DEFAULT_CONFIG
from engine.indicators   import generate_signals
from engine.survival     import run_all_survival_tests, gate1_minimum_winrate, monte_carlo
from engine.combinations import (build_combinations, generate_combination_signals,
                                  load_top_survivors_from_db, _mock_survivors,
                                  _load_symbol, _load_nepse_signals)

try:
    from config import (DB_PATH, DEFAULT_BACKTEST_CONFIG,
                        WEIGHT_WINRATE, WEIGHT_PROFIT_FACTOR,
                        WEIGHT_CONSISTENCY, WEIGHT_DRAWDOWN,
                        PROFIT_FACTOR_CAP,
                        MIN_WINRATE, WALK_FORWARD_MIN_WR, MONTE_CARLO_MIN_P5)
except ImportError:
    DB_PATH = os.path.join(ROOT, "data", "nepse.db")
    DEFAULT_BACKTEST_CONFIG = {"stop_loss_pct": 5.0, "take_profit_pct": 10.0,
                               "max_hold_days": 15, "initial_capital": 100_000,
                               "position_size_pct": 10.0}
    WEIGHT_WINRATE = 0.35; WEIGHT_PROFIT_FACTOR = 0.30
    WEIGHT_CONSISTENCY = 0.20; WEIGHT_DRAWDOWN = 0.15
    PROFIT_FACTOR_CAP = 3.0
    MIN_WINRATE = 0.48; WALK_FORWARD_MIN_WR = 0.45; MONTE_CARLO_MIN_P5 = 0.40


# ── DB HELPERS ────────────────────────────────────────────────────────────────
def _get_db():
    return sqlite3.connect(DB_PATH)


def create_trading_rules_table():
    """Create the trading_rules table if it does not exist."""
    conn = _get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trading_rules (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol                TEXT    NOT NULL,
            rule_name             TEXT    NOT NULL,
            rule_type             TEXT    NOT NULL CHECK(rule_type IN ('single','combination')),
            indicator_config      TEXT,
            winrate               REAL,
            profit_factor         REAL,
            consistency           REAL,
            max_drawdown          REAL,
            weighted_score        REAL,
            total_trades          INTEGER,
            avg_hold_days         REAL,
            walk_forward_winrate  REAL,
            monte_carlo_p5        REAL,
            backtest_config       TEXT,
            created_at            TEXT,
            UNIQUE(symbol, rule_name)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_trading_rules_symbol "
        "ON trading_rules(symbol)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_trading_rules_score "
        "ON trading_rules(weighted_score DESC)"
    )
    conn.commit()
    conn.close()
    print("[OK] trading_rules table ready")


# ── SCORING FORMULA ───────────────────────────────────────────────────────────
def calculate_score(metrics: dict) -> float:
    """
    Compute the weighted score (0-100) from a backtester metrics dict.

    Components
    ----------
    winrate_pct        (0-100)   weight 0.35
    pf_normalized      (0-100)   weight 0.30  — profit_factor capped at 3×
    consistency        (0-100)   weight 0.20  — % quarters profitable
    low_drawdown_score (0-100)   weight 0.15  — (1 - max_drawdown%) * 100
    """
    wr  = float(metrics.get("winrate",           0))
    pf  = float(metrics.get("profit_factor",     0))
    con = float(metrics.get("consistency",       0))
    dd  = float(metrics.get("max_drawdown",      0))

    pf_norm      = min(pf / PROFIT_FACTOR_CAP, 1.0) * 100.0
    dd_score     = max(0.0, (1.0 - dd / 100.0)) * 100

    score = (wr  * WEIGHT_WINRATE
           + pf_norm   * WEIGHT_PROFIT_FACTOR
           + con        * WEIGHT_CONSISTENCY
           + dd_score   * WEIGHT_DRAWDOWN)

    return round(score, 4)


# ── SAVE A SINGLE RULE ────────────────────────────────────────────────────────
def save_rule(symbol:           str,
              indicator_config: dict,
              backtest_config:  dict,
              backtest_result:  dict,
              survival_result:  dict | None = None,
              rule_type:        str = "single") -> int | None:
    """
    Score and persist one rule to trading_rules.

    Returns the row id, or None on failure.
    """
    m     = backtest_result.get("metrics", {})
    score = calculate_score(m)

    # avg hold days
    trades = backtest_result.get("trades", [])
    avg_hold = (sum(t.get("days_held", 0) for t in trades) / len(trades)
                if trades else 0.0)

    # survival data
    wf_wr = None
    mc_p5 = None
    if survival_result:
        g2    = survival_result.get("gate2", {})
        mc    = survival_result.get("monte_carlo", {})
        wf_wr = g2.get("test_winrate")
        mc_p5 = mc.get("p5_winrate")

    rule_name = indicator_config.get("name", "unknown")

    conn = _get_db()
    try:
        cursor = conn.execute("""
            INSERT OR REPLACE INTO trading_rules
            (symbol, rule_name, rule_type, indicator_config,
             winrate, profit_factor, consistency, max_drawdown,
             weighted_score, total_trades, avg_hold_days,
             walk_forward_winrate, monte_carlo_p5,
             backtest_config, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            symbol, rule_name, rule_type,
            json.dumps(indicator_config),
            m.get("winrate",         0),
            m.get("profit_factor",   0),
            m.get("consistency",     0),
            m.get("max_drawdown",    0),
            score,
            m.get("total_trades",    0),
            round(avg_hold, 2),
            wf_wr, mc_p5,
            json.dumps(backtest_config),
            datetime.now().isoformat(),
        ))
        conn.commit()
        return cursor.lastrowid
    except Exception as e:
        print(f"  [WARN] save_rule({symbol}, {rule_name}): {e}")
        return None
    finally:
        conn.close()


# ── FULL PIPELINE FOR ONE SYMBOL ──────────────────────────────────────────────
def run_full_pipeline(symbol: str,
                       backtest_config: dict | None = None,
                       gate1_threshold: float = MIN_WINRATE,
                       gate2_threshold: float = WALK_FORWARD_MIN_WR,
                       mc_threshold:   float = MONTE_CARLO_MIN_P5,
                       combo_top_n:    int   = 20,
                       verbose:        bool  = True) -> list[dict]:
    """
    End-to-end pipeline for *symbol*:

      1. backtest_all_indicators — 163 configs
      2. survival tests on each (3 gates + MC)
      3. collect single survivors
      4. build combination rules from survivors
      5. backtest + score combinations
      6. save all survivors to trading_rules
      7. return sorted list of saved rules

    Returns list of dicts with keys:
        symbol, rule_name, rule_type, weighted_score, metrics
    """
    if backtest_config is None:
        backtest_config = DEFAULT_CONFIG.copy()

    def vprint(*a, **kw):
        if verbose:
            print(*a, **kw)

    vprint(f"\n{'='*60}")
    vprint(f"  Full pipeline: {symbol}")
    vprint(f"{'='*60}")

    # ── Step A: Backtest all 163 single indicators ────────────────────────────
    vprint(f"\n[A] Backtesting all indicators on {symbol}...")
    all_bt = backtest_all_indicators(symbol, backtest_config)
    vprint(f"    {len(all_bt)} configs produced trades")

    # ── Step B: Survival tests on each ────────────────────────────────────────
    vprint(f"\n[B] Running survival tests ({len(all_bt)} configs)...")
    single_survivors = []
    saved_rules      = []

    for bt_res in all_bt:
        m = bt_res.get("metrics", {})
        if m.get("total_trades", 0) < 3:
            continue

        # Rebuild the indicator config from the result
        from engine.indicators import get_all_indicator_configs
        all_cfgs = {c["name"]: c for c in get_all_indicator_configs(silent=True)}
        ind_name = bt_res.get("indicator", "")
        ind_cfg  = all_cfgs.get(ind_name)
        if not ind_cfg:
            continue

        surv = run_all_survival_tests(
            symbol, ind_cfg, backtest_config,
            gate1_threshold=gate1_threshold,
            gate2_threshold=gate2_threshold,
            mc_threshold=mc_threshold,
            n_simulations=500,
        )

        if surv.get("survived"):
            single_survivors.append(ind_cfg)

            rule_id = save_rule(
                symbol, ind_cfg, backtest_config, bt_res,
                survival_result=surv, rule_type="single"
            )
            score = calculate_score(m)
            saved_rules.append({
                "symbol":         symbol,
                "rule_name":      ind_name,
                "rule_type":      "single",
                "weighted_score": score,
                "metrics":        m,
                "db_id":          rule_id,
            })

    vprint(f"    {len(single_survivors)} single rules survived all 3 gates")

    # ── Step C: Combination rules from survivors ───────────────────────────────
    if len(single_survivors) >= 2:
        vprint(f"\n[C] Building combinations from {len(single_survivors)} survivors...")

        df_sym   = _load_symbol(symbol)
        nepse_df = _load_nepse_signals(symbol)

        combos   = build_combinations(single_survivors, top_n=combo_top_n)
        vprint(f"    {len(combos)} combinations to test")

        combo_passed = 0
        for combo in combos:
            try:
                sig    = generate_combination_signals(df_sym, combo, nepse_df)
                bt_res = run_backtest(df_sym, sig, backtest_config)
                m      = bt_res.get("metrics", {})

                if m.get("total_trades", 0) < 3:
                    continue

                g1 = gate1_minimum_winrate(bt_res, threshold=gate1_threshold)
                mc = monte_carlo(bt_res.get("trades", []),
                                 n_simulations=500,
                                 confirmed_threshold=mc_threshold)

                if g1["passed"] and mc["confirmed"]:
                    rule_id = save_rule(
                        symbol, combo, backtest_config, bt_res,
                        survival_result={"gate2": {}, "monte_carlo": mc},
                        rule_type="combination"
                    )
                    score = calculate_score(m)
                    saved_rules.append({
                        "symbol":         symbol,
                        "rule_name":      combo["name"],
                        "rule_type":      "combination",
                        "weighted_score": score,
                        "metrics":        m,
                        "db_id":          rule_id,
                    })
                    combo_passed += 1

            except Exception:
                pass

        vprint(f"    {combo_passed} combination rules survived Gate1 + MC")
    else:
        vprint(f"\n[C] Skipped combinations (need >=2 single survivors, "
               f"have {len(single_survivors)})")

    # Sort by weighted_score descending
    saved_rules.sort(key=lambda r: r["weighted_score"], reverse=True)

    vprint(f"\n  Total rules saved for {symbol}: {len(saved_rules)}")
    return saved_rules


# ── QUERY HELPERS ─────────────────────────────────────────────────────────────
def get_top_rules(symbol: str, limit: int = 10) -> list[dict]:
    """
    Return the top *limit* rules for *symbol* from trading_rules,
    sorted by weighted_score DESC.
    """
    conn = _get_db()
    try:
        rows = conn.execute("""
            SELECT id, symbol, rule_name, rule_type,
                   winrate, profit_factor, consistency,
                   max_drawdown, weighted_score,
                   total_trades, avg_hold_days,
                   walk_forward_winrate, monte_carlo_p5
            FROM   trading_rules
            WHERE  symbol = ?
            ORDER  BY weighted_score DESC
            LIMIT  ?
        """, (symbol, limit)).fetchall()
        cols = [d[0] for d in conn.execute(
            "SELECT id, symbol, rule_name, rule_type, winrate, profit_factor, "
            "consistency, max_drawdown, weighted_score, total_trades, avg_hold_days, "
            "walk_forward_winrate, monte_carlo_p5 FROM trading_rules LIMIT 0"
        ).description or []]
        # Use simpler approach
        result = []
        for row in rows:
            result.append({
                "id":                   row[0],
                "symbol":               row[1],
                "rule_name":            row[2],
                "rule_type":            row[3],
                "winrate":              row[4],
                "profit_factor":        row[5],
                "consistency":          row[6],
                "max_drawdown":         row[7],
                "weighted_score":       row[8],
                "total_trades":         row[9],
                "avg_hold_days":        row[10],
                "walk_forward_winrate": row[11],
                "monte_carlo_p5":       row[12],
            })
        return result
    finally:
        conn.close()


def get_rules_summary():
    """Print and return a summary of trading_rules across all symbols."""
    conn = _get_db()
    try:
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        if "trading_rules" not in tables:
            print("  trading_rules table does not exist yet.")
            return {}

        total = conn.execute("SELECT COUNT(*) FROM trading_rules").fetchone()[0]
        singles = conn.execute(
            "SELECT COUNT(*) FROM trading_rules WHERE rule_type='single'"
        ).fetchone()[0]
        combos = conn.execute(
            "SELECT COUNT(*) FROM trading_rules WHERE rule_type='combination'"
        ).fetchone()[0]

        top_symbols = conn.execute("""
            SELECT symbol, COUNT(*) AS n, MAX(weighted_score) AS top_score
            FROM   trading_rules
            GROUP  BY symbol
            ORDER  BY n DESC
            LIMIT  10
        """).fetchall()

        print(f"\n  trading_rules summary:")
        print(f"  Total: {total}  |  Single: {singles}  |  Combo: {combos}")
        print()
        print(f"  {'Symbol':<8} {'Rules':>6} {'Top Score':>10}")
        print(f"  {'-'*28}")
        for r in top_symbols:
            print(f"  {r[0]:<8} {r[1]:>6} {r[2]:>10.2f}")

        return {"total": total, "singles": singles, "combos": combos}
    finally:
        conn.close()


# ── PRETTY PRINT RULES TABLE ──────────────────────────────────────────────────
def print_rules_table(rules: list[dict], title: str = "Top Trading Rules"):
    """Print a formatted leaderboard of trading rules."""
    if not rules:
        print("  No rules to display.")
        return

    print(f"\n  {title}")
    hdr = (f"  {'#':>3}  {'Rule':<40} {'Type':<6} "
           f"{'Tr':>4} {'WR%':>6} {'PF':>5} {'Con%':>6} "
           f"{'DD%':>6} {'Hold':>5} {'Score':>7}")
    print(hdr)
    print(f"  {'-' * (len(hdr)-2)}")
    for rank, r in enumerate(rules, 1):
        t = r.get("rule_type", "?")[0].upper()
        wf = f" wf={r['walk_forward_winrate']:.0f}%" if r.get("walk_forward_winrate") else ""
        mc = f" mc={r['monte_carlo_p5']:.0f}%" if r.get("monte_carlo_p5") else ""
        print(
            f"  {rank:>3}  {r['rule_name']:<40} {t:<6}"
            f"{r['total_trades']:>4} {r['winrate']:>5.1f}% "
            f"{r['profit_factor']:>5.2f} {r['consistency']:>5.1f}% "
            f"{r['max_drawdown']:>5.2f}% {r['avg_hold_days']:>5.1f}  "
            f"{r['weighted_score']:>6.2f}{wf}{mc}"
        )


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 65)
    print("NEPSE Rule Scorer - Step 12")
    print("=" * 65)
    print()

    SYMBOL = "NABIL"
    BT_CONFIG = {**DEFAULT_BACKTEST_CONFIG}

    # 1. Create table
    create_trading_rules_table()
    print()

    # 2. Demo the scoring formula with sample metrics
    print("Score formula demo:")
    sample_metrics = [
        {"name": "Perfect",   "winrate": 80, "profit_factor": 3.0,  "consistency": 100, "max_drawdown": 2},
        {"name": "Good",      "winrate": 60, "profit_factor": 1.8,  "consistency":  75, "max_drawdown": 8},
        {"name": "Average",   "winrate": 50, "profit_factor": 1.1,  "consistency":  50, "max_drawdown": 15},
        {"name": "Poor",      "winrate": 35, "profit_factor": 0.6,  "consistency":  25, "max_drawdown": 30},
    ]
    print(f"  {'Label':<12} {'WR%':>5} {'PF':>5} {'Con%':>5} {'DD%':>5}  {'Score':>7}")
    print(f"  {'-'*48}")
    for s in sample_metrics:
        sc = calculate_score(s)
        print(f"  {s['name']:<12} {s['winrate']:>5} {s['profit_factor']:>5.1f} "
              f"{s['consistency']:>5} {s['max_drawdown']:>5}  {sc:>7.2f}")
    print()

    # 3. Run full pipeline for NABIL
    print(f"Running full pipeline for {SYMBOL}...")
    rules = run_full_pipeline(
        SYMBOL, BT_CONFIG,
        gate1_threshold=MIN_WINRATE,
        gate2_threshold=WALK_FORWARD_MIN_WR,
        mc_threshold=MONTE_CARLO_MIN_P5,
        combo_top_n=15,
        verbose=True,
    )
    print()

    # 4. Top 5 rules from DB
    top = get_top_rules(SYMBOL, limit=10)
    print_rules_table(top, title=f"Top 10 Trading Rules for {SYMBOL}")

    # 5. Summary
    print()
    get_rules_summary()

    # 6. DB count
    conn = _get_db()
    n = conn.execute("SELECT COUNT(*) FROM trading_rules WHERE symbol=?",
                     (SYMBOL,)).fetchone()[0]
    conn.close()
    print(f"\n  trading_rules rows for {SYMBOL}: {n}")

    print()
    print("[OK] Step 12 complete!")
