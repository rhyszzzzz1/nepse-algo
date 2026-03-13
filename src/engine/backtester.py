# src/engine/backtester.py
# Step 6: NEPSE Backtester
# Simulates a rule-based trading strategy driven by signals from the signals
# table.  Applies NEPSE-specific T+2 settlement (cannot sell within 2 trading
# days of purchase), realistic commission (0.4% buy + 0.4% sell), and
# generates detailed trade logs and performance metrics.
#
# Strategy:
#   - BUY  when signal == 'BUY'  and no open position exists for the symbol
#   - SELL when signal == 'SELL' and an open position has cleared T+2
#   - Close remaining positions at the last available price at end of backtest
#
# Position sizing: equal-weight, configurable capital per trade (default 10k NPR)
# Results stored in:
#   backtest_trades  — one row per completed trade
#   backtest_summary — one row per backtest run (performance metrics)

import sqlite3
import os
import traceback
import uuid
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

# ── CONFIG ────────────────────────────────────────────────────────────────────
DB_PATH          = "data/nepse.db"
DEFAULT_CAPITAL  = 100_000   # NPR — total starting capital
CAPITAL_PER_TRADE = 10_000   # NPR — amount deployed per trade
BUY_COMMISSION   = 0.004     # 0.4%
SELL_COMMISSION  = 0.004     # 0.4%
T2_DAYS          = 2         # minimum holding days before sell allowed


# ── DB HELPER ─────────────────────────────────────────────────────────────────
def get_db():
    os.makedirs("data", exist_ok=True)
    return sqlite3.connect(DB_PATH)


# ── CREATE TABLES ─────────────────────────────────────────────────────────────
def create_backtest_tables():
    conn = get_db()

    conn.execute("""
        CREATE TABLE IF NOT EXISTS backtest_trades (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id         TEXT NOT NULL,
            symbol         TEXT NOT NULL,
            buy_date       TEXT NOT NULL,
            sell_date      TEXT,
            buy_price      REAL,
            sell_price     REAL,
            shares         REAL,
            gross_profit   REAL,
            commission     REAL,
            net_profit     REAL,
            return_pct     REAL,
            holding_days   INTEGER,
            exit_reason    TEXT,
            created_at     TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS backtest_summary (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          TEXT NOT NULL UNIQUE,
            symbol_filter   TEXT,
            start_date      TEXT,
            end_date        TEXT,
            starting_capital REAL,
            ending_capital   REAL,
            total_return_pct REAL,
            total_trades    INTEGER,
            winning_trades  INTEGER,
            losing_trades   INTEGER,
            win_rate_pct    REAL,
            avg_return_pct  REAL,
            best_trade_pct  REAL,
            worst_trade_pct REAL,
            max_drawdown_pct REAL,
            sharpe_ratio    REAL,
            created_at      TEXT
        )
    """)

    conn.commit()
    conn.close()
    print("[OK] backtest tables ready")


# ── TRADING DAY HELPERS ───────────────────────────────────────────────────────
def _get_trading_dates(conn, symbol=None):
    """Return a sorted list of all trading dates in clean_price_history."""
    if symbol:
        rows = conn.execute(
            "SELECT DISTINCT date FROM clean_price_history WHERE symbol=? ORDER BY date",
            (symbol,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT DISTINCT date FROM clean_price_history ORDER BY date"
        ).fetchall()
    return [r[0] for r in rows]


def _earliest_sell_date(buy_date, all_dates):
    """
    Return the earliest date on which a position bought on *buy_date* can be
    sold, respecting T+2 settlement (2 trading days must pass).
    """
    try:
        idx = all_dates.index(buy_date)
    except ValueError:
        return None
    sell_idx = idx + T2_DAYS
    if sell_idx >= len(all_dates):
        return None
    return all_dates[sell_idx]


# ── METRICS HELPERS ───────────────────────────────────────────────────────────
def _sharpe(returns, risk_free=0.0):
    """Annualised Sharpe ratio from a list of per-trade return %."""
    if len(returns) < 2:
        return 0.0
    arr = np.array(returns, dtype=float)
    mu  = arr.mean() - risk_free
    sd  = arr.std(ddof=1)
    if sd == 0:
        return 0.0
    # approx annual: assume ~252 trading days, trades are ~daily observations
    return round(float(mu / sd * np.sqrt(252)), 4)


def _max_drawdown(equity_curve):
    """Max drawdown % from a list of equity values."""
    arr = np.array(equity_curve, dtype=float)
    if len(arr) < 2:
        return 0.0
    peak = arr[0]
    max_dd = 0.0
    for val in arr:
        if val > peak:
            peak = val
        dd = (peak - val) / peak * 100 if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd
    return round(max_dd, 4)


# ── BACKTEST ONE SYMBOL ───────────────────────────────────────────────────────
def backtest_symbol(symbol, start_date=None, end_date=None,
                    capital=DEFAULT_CAPITAL,
                    capital_per_trade=CAPITAL_PER_TRADE):
    """
    Backtest the signal strategy for a single symbol.
    Returns a dict of performance metrics, or None if insufficient data.
    """
    conn = get_db()
    try:
        # Load signals joined with price data
        query = """
            SELECT s.date, s.close, s.signal, s.total_score,
                   s.market_condition, c.high, c.low
            FROM   signals s
            JOIN   clean_price_history c
              ON   s.symbol = c.symbol AND s.date = c.date
            WHERE  s.symbol = ?
        """
        params = [symbol]
        if start_date:
            query += " AND s.date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND s.date <= ?"
            params.append(end_date)
        query += " ORDER BY s.date ASC"

        df = pd.read_sql_query(query, conn, params=params)
        if df.empty or len(df) < 10:
            return None

        all_dates = df["date"].tolist()

        cash        = float(capital)
        position    = None    # {buy_date, buy_price, shares, cost_basis}
        trades      = []
        equity_curve = [cash]

        for _, row in df.iterrows():
            date     = row["date"]
            price    = float(row["close"])
            signal   = row["signal"]

            # -- SELL logic ---------------------------------------------------
            if position is not None:
                earliest = _earliest_sell_date(position["buy_date"], all_dates)
                can_sell = (earliest is not None and date >= earliest)

                if can_sell and (signal == "SELL" or row["total_score"] <= -2):
                    sell_price = price
                    gross      = (sell_price - position["buy_price"]) * position["shares"]
                    commission = (position["cost_basis"] * BUY_COMMISSION +
                                  sell_price * position["shares"] * SELL_COMMISSION)
                    net        = gross - commission
                    ret_pct    = net / position["cost_basis"] * 100
                    holding    = all_dates.index(date) - all_dates.index(position["buy_date"])

                    cash += position["cost_basis"] + net
                    trades.append({
                        "symbol":       symbol,
                        "buy_date":     position["buy_date"],
                        "sell_date":    date,
                        "buy_price":    position["buy_price"],
                        "sell_price":   sell_price,
                        "shares":       position["shares"],
                        "gross_profit": round(gross, 2),
                        "commission":   round(commission, 2),
                        "net_profit":   round(net, 2),
                        "return_pct":   round(ret_pct, 4),
                        "holding_days": holding,
                        "exit_reason":  "SELL_SIGNAL",
                    })
                    position = None

            # -- BUY logic ----------------------------------------------------
            if position is None and signal == "BUY":
                invest = min(capital_per_trade, cash * 0.9)
                if invest >= 100 and price > 0:
                    shares    = invest / price
                    cost      = shares * price
                    cash     -= cost
                    position  = {
                        "buy_date":  date,
                        "buy_price": price,
                        "shares":    shares,
                        "cost_basis": cost,
                    }

            # Track equity (cash + open position value)
            open_val = position["shares"] * price if position else 0.0
            equity_curve.append(cash + open_val)

        # -- Close any open position at end of period -------------------------
        if position is not None:
            last_price = float(df.iloc[-1]["close"])
            gross      = (last_price - position["buy_price"]) * position["shares"]
            commission = (position["cost_basis"] * BUY_COMMISSION +
                          last_price * position["shares"] * SELL_COMMISSION)
            net        = gross - commission
            ret_pct    = net / position["cost_basis"] * 100
            holding    = (len(all_dates) - 1 -
                          all_dates.index(position["buy_date"]))
            cash      += position["cost_basis"] + net
            trades.append({
                "symbol":       symbol,
                "buy_date":     position["buy_date"],
                "sell_date":    df.iloc[-1]["date"],
                "buy_price":    position["buy_price"],
                "sell_price":   last_price,
                "shares":       position["shares"],
                "gross_profit": round(gross, 2),
                "commission":   round(commission, 2),
                "net_profit":   round(net, 2),
                "return_pct":   round(ret_pct, 4),
                "holding_days": holding,
                "exit_reason":  "END_OF_PERIOD",
            })

        if not trades:
            return None

        # -- Persist trades ---------------------------------------------------
        run_id     = str(uuid.uuid4())[:8]
        created_at = datetime.now().isoformat()
        for t in trades:
            conn.execute("""
                INSERT INTO backtest_trades
                (run_id, symbol, buy_date, sell_date, buy_price, sell_price,
                 shares, gross_profit, commission, net_profit, return_pct,
                 holding_days, exit_reason, created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (run_id, t["symbol"], t["buy_date"], t["sell_date"],
                  t["buy_price"], t["sell_price"], t["shares"],
                  t["gross_profit"], t["commission"], t["net_profit"],
                  t["return_pct"], t["holding_days"], t["exit_reason"],
                  created_at))
        conn.commit()

        # -- Compute metrics --------------------------------------------------
        returns       = [t["return_pct"] for t in trades]
        winners       = [r for r in returns if r > 0]
        losers        = [r for r in returns if r <= 0]
        total_return  = (cash - capital) / capital * 100
        win_rate      = len(winners) / len(trades) * 100 if trades else 0.0
        sharpe        = _sharpe(returns)
        max_dd        = _max_drawdown(equity_curve)

        summary = {
            "run_id":            run_id,
            "symbol":            symbol,
            "start_date":        df.iloc[0]["date"],
            "end_date":          df.iloc[-1]["date"],
            "starting_capital":  capital,
            "ending_capital":    round(cash, 2),
            "total_return_pct":  round(total_return, 4),
            "total_trades":      len(trades),
            "winning_trades":    len(winners),
            "losing_trades":     len(losers),
            "win_rate_pct":      round(win_rate, 2),
            "avg_return_pct":    round(float(np.mean(returns)), 4) if returns else 0.0,
            "best_trade_pct":    round(max(returns), 4) if returns else 0.0,
            "worst_trade_pct":   round(min(returns), 4) if returns else 0.0,
            "max_drawdown_pct":  max_dd,
            "sharpe_ratio":      sharpe,
        }

        conn.execute("""
            INSERT OR REPLACE INTO backtest_summary
            (run_id, symbol_filter, start_date, end_date, starting_capital,
             ending_capital, total_return_pct, total_trades, winning_trades,
             losing_trades, win_rate_pct, avg_return_pct, best_trade_pct,
             worst_trade_pct, max_drawdown_pct, sharpe_ratio, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (run_id, symbol, summary["start_date"], summary["end_date"],
              capital, summary["ending_capital"], summary["total_return_pct"],
              len(trades), len(winners), len(losers), win_rate,
              summary["avg_return_pct"], summary["best_trade_pct"],
              summary["worst_trade_pct"], max_dd, sharpe, created_at))
        conn.commit()

        return summary

    except Exception as e:
        print(f"  [ERR] Backtest failed for {symbol}: {e}")
        traceback.print_exc()
        return None
    finally:
        conn.close()


# ── BACKTEST ALL SYMBOLS ──────────────────────────────────────────────────────
def backtest_all(start_date=None, end_date=None,
                 capital=DEFAULT_CAPITAL,
                 capital_per_trade=CAPITAL_PER_TRADE):
    """
    Run backtest_symbol() for every symbol that has signal data.
    Returns a DataFrame ranking all symbols by total_return_pct.
    """
    conn = get_db()
    symbols = [r[0] for r in conn.execute(
        "SELECT DISTINCT symbol FROM signals"
    ).fetchall()]
    conn.close()

    total = len(symbols)
    print(f"Backtesting {total} symbols...")
    results = []
    done, failed = 0, 0

    for i, symbol in enumerate(symbols, 1):
        res = backtest_symbol(symbol, start_date=start_date,
                              end_date=end_date, capital=capital,
                              capital_per_trade=capital_per_trade)
        if res:
            results.append(res)
            done += 1
        else:
            failed += 1

        if i % 50 == 0 or i == total:
            print(f"  [{i}/{total}] done={done} failed={failed}")

    if not results:
        print("  No results.")
        return pd.DataFrame()

    df = pd.DataFrame(results).sort_values("total_return_pct", ascending=False)
    df.reset_index(drop=True, inplace=True)
    print(f"\n[DONE] Backtested {done} symbols | {failed} skipped")
    return df


# ── PERFORMANCE REPORT ────────────────────────────────────────────────────────
def print_performance_report(df, n=20):
    """Print top/bottom performers and aggregate market statistics."""
    if df is None or df.empty:
        print("No backtest data to report.")
        return

    cols = ["symbol", "total_trades", "win_rate_pct",
            "avg_return_pct", "total_return_pct",
            "max_drawdown_pct", "sharpe_ratio"]

    print(f"\n{'='*60}")
    print(f"  TOP {n} PERFORMERS")
    print(f"{'='*60}")
    print(df[cols].head(n).to_string(index=False))

    print(f"\n{'='*60}")
    print(f"  BOTTOM {n} PERFORMERS")
    print(f"{'='*60}")
    print(df[cols].tail(n).to_string(index=False))

    print(f"\n{'='*60}")
    print(f"  AGGREGATE STATS  ({len(df)} symbols)")
    print(f"{'='*60}")
    print(f"  Avg return:      {df['total_return_pct'].mean():.2f}%")
    print(f"  Median return:   {df['total_return_pct'].median():.2f}%")
    print(f"  % profitable:    {(df['total_return_pct'] > 0).mean()*100:.1f}%")
    print(f"  Avg win rate:    {df['win_rate_pct'].mean():.1f}%")
    print(f"  Avg Sharpe:      {df['sharpe_ratio'].mean():.4f}")
    print(f"  Avg max DD:      {df['max_drawdown_pct'].mean():.2f}%")
    print(f"  Best symbol:     {df.iloc[0]['symbol']}  "
          f"({df.iloc[0]['total_return_pct']:.2f}%)")
    print(f"  Worst symbol:    {df.iloc[-1]['symbol']}  "
          f"({df.iloc[-1]['total_return_pct']:.2f}%)")


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 55)
    print("NEPSE Backtester - Step 6")
    print("=" * 55)

    # 1. Create tables
    create_backtest_tables()
    print()

    # 2. Test single symbol — NABIL
    print("Testing backtest on NABIL...")
    result = backtest_symbol("NABIL")
    if result:
        print("\nNABIL backtest result:")
        for k, v in result.items():
            if k not in ("run_id",):
                print(f"  {k:<22} {v}")
    else:
        print("  Not enough signal data for NABIL.")
    print()

    # 3. Prompt for full backtest
    answer = input("Run full backtest on all symbols? (y/n): ").strip().lower()
    if answer == "y":
        all_results = backtest_all()
        print_performance_report(all_results)
    else:
        print("Skipped full backtest.")
    print()

    # 4. DB summary
    conn = get_db()
    trade_count   = conn.execute("SELECT COUNT(*) FROM backtest_trades").fetchone()[0]
    summary_count = conn.execute("SELECT COUNT(*) FROM backtest_summary").fetchone()[0]
    conn.close()

    print(f"backtest_trades rows:   {trade_count}")
    print(f"backtest_summary rows:  {summary_count}")
    print("\n[OK] Step 6 complete!")
