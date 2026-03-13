# src/signals/generator.py
# Step 5: NEPSE Signal Generator
# Reads clean_price_history and broker_summary data from SQLite, computes
# technical indicators (RSI, MACD, Bollinger Bands, volume surge), combines
# them into a composite signal score, and saves buy/sell/hold signals to the
# signals table.
#
# Indicators used (all via 'ta' library):
#   RSI-14, MACD (12/26/9), Bollinger Bands (20, 2sd), EMA-20 trend filter
# Broker data overlay (when available):
#   buyer_concentration > 60% = bullish accumulation bonus
#   net_broker_flow > 0       = bullish flow bonus
#
# Signal score: -4 to +4  (positive = bullish, negative = bearish)
# Signal label: BUY (score >= 2), SELL (score <= -2), HOLD otherwise

import sqlite3
from db import get_db
import os
import traceback
from datetime import datetime

import pandas as pd
import ta

# ── CONFIG ────────────────────────────────────────────────────────────────────
DB_PATH   = "data/nepse.db"
MIN_ROWS  = 30   # minimum clean rows needed to generate a signal


# ── DB HELPER ─────────────────────────────────────────────────────────────────


# ── CREATE TABLE ──────────────────────────────────────────────────────────────
def create_signals_table():
    """Create the signals table if it does not already exist."""
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol              TEXT NOT NULL,
            date                TEXT NOT NULL,
            close               REAL,
            rsi14               REAL,
            macd                REAL,
            macd_signal         REAL,
            macd_hist           REAL,
            bb_upper            REAL,
            bb_lower            REAL,
            bb_pct              REAL,
            ema20               REAL,
            volume_ratio        REAL,
            atr14               REAL,
            market_condition    TEXT,
            rsi_score           INTEGER,
            macd_score          INTEGER,
            bb_score            INTEGER,
            trend_score         INTEGER,
            volume_score        INTEGER,
            broker_score        INTEGER,
            total_score         INTEGER,
            signal              TEXT,
            generated_at        TEXT,
            UNIQUE(symbol, date)
        )
    """)
    conn.commit()
    conn.close()
    print("[OK] signals table ready")


# ── SCORE HELPERS ─────────────────────────────────────────────────────────────
def _rsi_score(rsi):
    """RSI scoring: oversold = +1, overbought = -1, neutral = 0."""
    if pd.isna(rsi):
        return 0
    if rsi < 35:
        return 1
    if rsi > 65:
        return -1
    return 0


def _macd_score(hist, prev_hist):
    """MACD histogram crossing zero line: +1 bullish cross, -1 bearish cross."""
    if pd.isna(hist) or pd.isna(prev_hist):
        return 0
    if prev_hist < 0 and hist >= 0:
        return 1    # bullish crossover
    if prev_hist > 0 and hist <= 0:
        return -1   # bearish crossover
    if hist > 0:
        return 1    # already above zero
    if hist < 0:
        return -1   # already below zero
    return 0


def _bb_score(close, bb_upper, bb_lower, bb_pct):
    """Bollinger Band position score."""
    if pd.isna(bb_pct) or pd.isna(bb_upper) or pd.isna(bb_lower):
        return 0
    if close <= bb_lower or bb_pct <= 0.05:
        return 1    # near/below lower band — oversold
    if close >= bb_upper or bb_pct >= 0.95:
        return -1   # near/above upper band — overbought
    return 0


def _trend_score(close, ema20):
    """Simple EMA-20 trend filter."""
    if pd.isna(ema20):
        return 0
    return 1 if close > ema20 else -1


def _volume_score(volume_ratio):
    """High volume on up-moves is bullish confirmation."""
    if pd.isna(volume_ratio):
        return 0
    if volume_ratio >= 2.0:
        return 1    # unusually high volume
    if volume_ratio <= 0.4:
        return -1   # drying up volume
    return 0


# ── GENERATE SIGNALS FOR ONE SYMBOL ──────────────────────────────────────────
def generate_signals_for(symbol):
    """
    Compute technical indicators and score the latest trading session for
    *symbol*. Saves a row to the signals table for every date in clean history.

    Returns the latest signal dict, or None on failure.
    """
    conn = get_db()
    try:
        df = pd.read_sql_query("""
            SELECT symbol, date, open, high, low, close, volume,
                   volume_ratio, atr14, market_condition
            FROM   clean_price_history
            WHERE  symbol = ?
            ORDER  BY date ASC
        """, conn, params=(symbol,))

        if df.empty or len(df) < MIN_ROWS:
            return None

        # ── INDICATORS ────────────────────────────────────────────────────────
        close  = df["close"]
        high   = df["high"]
        low    = df["low"]
        volume = df["volume"]

        # RSI-14
        rsi = ta.momentum.RSIIndicator(close=close, window=14, fillna=False).rsi()

        # MACD (12, 26, 9)
        macd_obj   = ta.trend.MACD(close=close, window_slow=26,
                                    window_fast=12, window_sign=9, fillna=False)
        macd       = macd_obj.macd()
        macd_sig   = macd_obj.macd_signal()
        macd_hist  = macd_obj.macd_diff()

        # Bollinger Bands (20, 2)
        bb         = ta.volatility.BollingerBands(close=close, window=20,
                                                   window_dev=2, fillna=False)
        bb_upper   = bb.bollinger_hband()
        bb_lower   = bb.bollinger_lband()
        bb_pct     = bb.bollinger_pband()

        # EMA-20
        ema20      = ta.trend.EMAIndicator(close=close, window=20,
                                            fillna=False).ema_indicator()

        # ── BROKER OVERLAY ────────────────────────────────────────────────────
        try:
            broker_df = pd.read_sql_query("""
                SELECT date, buyer_concentration, net_broker_flow
                FROM   broker_summary
                WHERE  symbol = ?
                ORDER  BY date ASC
            """, conn, params=(symbol,))
            broker_map = broker_df.set_index("date").to_dict("index") if not broker_df.empty else {}
        except Exception:
            broker_map = {}

        # ── SCORE EVERY ROW AND SAVE ──────────────────────────────────────────
        generated_at = datetime.now().isoformat()
        latest = None

        for i in range(len(df)):
            row   = df.iloc[i]
            pr_hi = macd_hist.iloc[i - 1] if i > 0 else float("nan")

            r_score = _rsi_score(rsi.iloc[i])
            m_score = _macd_score(macd_hist.iloc[i], pr_hi)
            b_score = _bb_score(row["close"], bb_upper.iloc[i],
                                 bb_lower.iloc[i], bb_pct.iloc[i])
            t_score = _trend_score(row["close"], ema20.iloc[i])
            v_score = _volume_score(row["volume_ratio"])

            # Broker overlay (only if data exists for this date)
            bk_score = 0
            bk = broker_map.get(row["date"], {})
            if bk:
                if bk.get("buyer_concentration", 0) > 60:
                    bk_score += 1
                if bk.get("net_broker_flow", 0) > 0:
                    bk_score += 1

            total = r_score + m_score + b_score + t_score + v_score + bk_score

            if total >= 2:
                signal = "BUY"
            elif total <= -2:
                signal = "SELL"
            else:
                signal = "HOLD"

            try:
                conn.execute("""
                    INSERT OR REPLACE INTO signals
                    (symbol, date, close, rsi14, macd, macd_signal, macd_hist,
                     bb_upper, bb_lower, bb_pct, ema20, volume_ratio, atr14,
                     market_condition, rsi_score, macd_score, bb_score,
                     trend_score, volume_score, broker_score, total_score,
                     signal, generated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    row["symbol"], row["date"],
                    float(row["close"]  or 0),
                    float(rsi.iloc[i]        if not pd.isna(rsi.iloc[i])        else 0),
                    float(macd.iloc[i]       if not pd.isna(macd.iloc[i])       else 0),
                    float(macd_sig.iloc[i]   if not pd.isna(macd_sig.iloc[i])   else 0),
                    float(macd_hist.iloc[i]  if not pd.isna(macd_hist.iloc[i])  else 0),
                    float(bb_upper.iloc[i]   if not pd.isna(bb_upper.iloc[i])   else 0),
                    float(bb_lower.iloc[i]   if not pd.isna(bb_lower.iloc[i])   else 0),
                    float(bb_pct.iloc[i]     if not pd.isna(bb_pct.iloc[i])     else 0),
                    float(ema20.iloc[i]      if not pd.isna(ema20.iloc[i])      else 0),
                    float(row["volume_ratio"]if not pd.isna(row["volume_ratio"])else 0),
                    float(row["atr14"]       if not pd.isna(row["atr14"])       else 0),
                    row["market_condition"],
                    r_score, m_score, b_score, t_score, v_score, bk_score, total,
                    signal, generated_at,
                ))
            except Exception as e:
                pass   # duplicate or bad row — skip silently

        conn.commit()

        # Return a summary dict for the last (most recent) row
        last = df.iloc[-1]
        last_i = len(df) - 1
        pr_hi = macd_hist.iloc[last_i - 1] if last_i > 0 else float("nan")
        r_s = _rsi_score(rsi.iloc[last_i])
        m_s = _macd_score(macd_hist.iloc[last_i], pr_hi)
        b_s = _bb_score(last["close"], bb_upper.iloc[last_i],
                         bb_lower.iloc[last_i], bb_pct.iloc[last_i])
        t_s = _trend_score(last["close"], ema20.iloc[last_i])
        v_s = _volume_score(last["volume_ratio"])
        bk  = broker_map.get(last["date"], {})
        bk_s = 0
        if bk.get("buyer_concentration", 0) > 60:
            bk_s += 1
        if bk.get("net_broker_flow", 0) > 0:
            bk_s += 1
        tot = r_s + m_s + b_s + t_s + v_s + bk_s

        latest = {
            "symbol":            symbol,
            "date":              last["date"],
            "close":             last["close"],
            "rsi14":             round(float(rsi.iloc[last_i])       if not pd.isna(rsi.iloc[last_i])       else 0, 2),
            "macd_hist":         round(float(macd_hist.iloc[last_i]) if not pd.isna(macd_hist.iloc[last_i]) else 0, 4),
            "bb_pct":            round(float(bb_pct.iloc[last_i])    if not pd.isna(bb_pct.iloc[last_i])    else 0, 4),
            "ema20":             round(float(ema20.iloc[last_i])      if not pd.isna(ema20.iloc[last_i])      else 0, 2),
            "market_condition":  last["market_condition"],
            "total_score":       tot,
            "signal":            "BUY" if tot >= 2 else "SELL" if tot <= -2 else "HOLD",
        }
        return latest

    except Exception as e:
        print(f"  [ERR] {symbol}: {e}")
        traceback.print_exc()
        return None
    finally:
        conn.close()


# ── GENERATE SIGNALS FOR ALL SYMBOLS ─────────────────────────────────────────
def generate_all_signals():
    """
    Run generate_signals_for() over every symbol in clean_price_history.
    Prints a progress counter and a final breakdown of BUY/SELL/HOLD counts.
    """
    conn = get_db()
    symbols = [r[0] for r in conn.execute(
        "SELECT DISTINCT symbol FROM clean_price_history"
    ).fetchall()]
    conn.close()

    total = len(symbols)
    print(f"Generating signals for {total} symbols...")
    counts = {"BUY": 0, "SELL": 0, "HOLD": 0, "FAILED": 0}

    for i, symbol in enumerate(symbols, 1):
        result = generate_signals_for(symbol)
        if result:
            counts[result["signal"]] += 1
        else:
            counts["FAILED"] += 1

        if i % 50 == 0 or i == total:
            print(f"  [{i}/{total}] BUY={counts['BUY']} "
                  f"SELL={counts['SELL']} HOLD={counts['HOLD']} "
                  f"FAILED={counts['FAILED']}")

    print(f"\n[DONE] Signals generated:")
    print(f"  BUY:    {counts['BUY']}")
    print(f"  SELL:   {counts['SELL']}")
    print(f"  HOLD:   {counts['HOLD']}")
    print(f"  FAILED: {counts['FAILED']}")
    return counts


# ── TOP SIGNALS REPORT ────────────────────────────────────────────────────────
def get_top_signals(n=20):
    """
    Print the top-N BUY and top-N SELL signals from the most recent date
    that has signal data, ranked by total_score.
    Returns a tuple of (buy_df, sell_df).
    """
    conn = get_db()
    try:
        # Most recent date with signals
        latest_date = conn.execute(
            "SELECT MAX(date) FROM signals"
        ).fetchone()[0]

        if not latest_date:
            print("No signal data found.")
            return None, None

        df = pd.read_sql_query("""
            SELECT symbol, date, close, rsi14, macd_hist, bb_pct,
                   market_condition, total_score, signal
            FROM   signals
            WHERE  date = ?
            ORDER  BY total_score DESC
        """, conn, params=(latest_date,))

        buys  = df[df["signal"] == "BUY"].head(n)
        sells = df[df["signal"] == "SELL"].sort_values(
            "total_score").head(n)

        print(f"\n=== Top {n} BUY signals for {latest_date} ===")
        if not buys.empty:
            print(buys[["symbol", "close", "rsi14", "macd_hist",
                         "market_condition", "total_score"]].to_string(index=False))
        else:
            print("  (none)")

        print(f"\n=== Top {n} SELL signals for {latest_date} ===")
        if not sells.empty:
            print(sells[["symbol", "close", "rsi14", "macd_hist",
                          "market_condition", "total_score"]].to_string(index=False))
        else:
            print("  (none)")

        return buys, sells

    except Exception as e:
        print(f"[ERR] get_top_signals failed: {e}")
        traceback.print_exc()
        return None, None
    finally:
        conn.close()


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 55)
    print("NEPSE Signal Generator - Step 5")
    print("=" * 55)

    # 1. Create table
    create_signals_table()
    print()

    # 2. Test with NABIL
    print("Testing with NABIL...")
    result = generate_signals_for("NABIL")
    if result:
        print(f"\nLatest signal for NABIL:")
        for k, v in result.items():
            print(f"  {k:<20} {v}")
    print()

    # 3. Generate for all symbols
    answer = input("Generate signals for all symbols? (y/n): ").strip().lower()
    if answer == "y":
        generate_all_signals()
    else:
        print("Skipped full run.")
    print()

    # 4. Top signals report
    get_top_signals(n=20)

    # 5. DB summary
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
    buys  = conn.execute("SELECT COUNT(*) FROM signals WHERE signal='BUY'").fetchone()[0]
    sells = conn.execute("SELECT COUNT(*) FROM signals WHERE signal='SELL'").fetchone()[0]
    conn.close()
    print(f"\nTotal signal rows: {total}  (BUY={buys}, SELL={sells})")
    print("[OK] Step 5 complete!")
