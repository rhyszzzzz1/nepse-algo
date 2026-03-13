# src/signals/nepse_signals.py
# Step 5b: NEPSE-Specific Signal Calculator
# Calculates signals unique to Nepal's market structure:
# pump/dump scoring, broker accumulation/distribution,
# liquidity spikes, and volatility regime classification.

import sqlite3
from db import get_db
import os
import traceback
from datetime import datetime

import pandas as pd
import numpy as np

# ── CONFIG ────────────────────────────────────────────────────────────────────
DB_PATH  = "data/nepse.db"
MIN_ROWS = 20   # minimum clean rows required to calculate signals


# ── DB HELPER ─────────────────────────────────────────────────────────────────


# ── CREATE TABLE ──────────────────────────────────────────────────────────────
def create_signals_table():
    """Create the nepse_signals table if it does not exist."""
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nepse_signals (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol               TEXT NOT NULL,
            date                 TEXT NOT NULL,
            pump_score           REAL DEFAULT 0,
            dump_score           REAL DEFAULT 0,
            broker_accumulation  REAL DEFAULT 0,
            broker_distribution  REAL DEFAULT 0,
            liquidity_spike      REAL DEFAULT 0,
            volatility_regime    TEXT DEFAULT 'normal',
            net_broker_flow      REAL DEFAULT 0,
            signal_date          TEXT,
            calculated_at        TEXT,
            UNIQUE(symbol, date)
        )
    """)
    conn.commit()
    conn.close()
    print("[OK] nepse_signals table ready")


# ── VOLATILITY REGIME ─────────────────────────────────────────────────────────
def _volatility_regime(atr_series):
    """
    Classify each row's ATR relative to its own 90-day rolling average.
    Returns a Series of 'low' / 'normal' / 'high' / 'extreme' strings.
    """
    rolling_avg = atr_series.rolling(window=90, min_periods=10).mean()
    ratio = atr_series / rolling_avg

    def _label(r):
        if pd.isna(r):
            return "normal"
        if r < 0.5:
            return "low"
        if r < 1.5:
            return "normal"
        if r < 2.5:
            return "high"
        return "extreme"

    return ratio.apply(_label)


# ── CALCULATE SIGNALS FOR ONE SYMBOL ─────────────────────────────────────────
def calculate_signals_for(symbol):
    """
    Load clean_price_history (and broker_summary if available) for *symbol*,
    compute all NEPSE-specific signals for every row, persist to nepse_signals.

    Returns the full signals DataFrame, or None on failure.
    """
    conn = get_db()
    try:
        # ── Load price data ───────────────────────────────────────────────────
        price_df = pd.read_sql_query("""
            SELECT date, close, open, high, low, volume,
                   price_change_pct, volume_ratio, atr14, market_condition
            FROM   clean_price_history
            WHERE  symbol = ?
            ORDER  BY date ASC
        """, conn, params=(symbol,))

        if price_df.empty or len(price_df) < MIN_ROWS:
            return None

        # Ensure numerics
        for col in ["close", "open", "high", "low", "volume",
                    "price_change_pct", "volume_ratio", "atr14"]:
            price_df[col] = pd.to_numeric(price_df[col], errors="coerce")

        # ── Load broker summary (optional) ────────────────────────────────────
        try:
            broker_df = pd.read_sql_query("""
                SELECT date,
                       buyer_concentration  AS broker_accumulation,
                       seller_concentration AS broker_distribution,
                       net_broker_flow
                FROM   broker_summary
                WHERE  symbol = ?
                ORDER  BY date ASC
            """, conn, params=(symbol,))
            broker_map = (broker_df.set_index("date").to_dict("index")
                          if not broker_df.empty else {})
        except Exception:
            broker_map = {}

        # ── Derived series ────────────────────────────────────────────────────
        close  = price_df["close"]
        pct    = price_df["price_change_pct"].fillna(0)
        volr   = price_df["volume_ratio"].fillna(1.0)
        atr    = price_df["atr14"].fillna(0)

        # 52-week high / low (rolling 252 days)
        high52 = close.rolling(252, min_periods=20).max()
        low52  = close.rolling(252, min_periods=20).min()
        at_52h = close >= high52
        at_52l = close <= low52

        # 3 consecutive up / down days
        up_day   = (pct > 0).astype(int)
        dn_day   = (pct < 0).astype(int)
        consec_up = (up_day.rolling(3, min_periods=3).sum() == 3)
        consec_dn = (dn_day.rolling(3, min_periods=3).sum() == 3)

        # Volatility regime
        vol_regime = _volatility_regime(atr)

        # ── Build row-by-row signal scores ────────────────────────────────────
        calculated_at = datetime.now().isoformat()
        records = []

        for i, row in price_df.iterrows():
            date = row["date"]
            vr   = row["volume_ratio"] if not pd.isna(row["volume_ratio"]) else 1.0
            pc   = row["price_change_pct"] if not pd.isna(row["price_change_pct"]) else 0.0

            # Broker data for this date (may be missing — defaults to 0)
            bk = broker_map.get(date, {})
            b_accum = float(bk.get("broker_accumulation", 0) or 0)
            b_dist  = float(bk.get("broker_distribution",  0) or 0)
            b_flow  = float(bk.get("net_broker_flow",      0) or 0)

            # ── pump_score ────────────────────────────────────────────────────
            pump = 0
            if vr > 3.0:
                pump += 30
            if pc > 5.0:
                pump += 25
            if b_accum > 70:
                pump += 20
            if consec_up.iloc[i]:
                pump += 15
            if at_52h.iloc[i]:
                pump += 10
            pump = min(pump, 100)

            # ── dump_score ────────────────────────────────────────────────────
            dump = 0
            if vr > 3.0:
                dump += 30
            if pc < -5.0:
                dump += 25
            if b_dist > 70:
                dump += 20
            if consec_dn.iloc[i]:
                dump += 15
            if at_52l.iloc[i]:
                dump += 10
            dump = min(dump, 100)

            records.append({
                "symbol":              symbol,
                "date":                date,
                "pump_score":          float(pump),
                "dump_score":          float(dump),
                "broker_accumulation": b_accum,
                "broker_distribution": b_dist,
                "liquidity_spike":     round(vr, 4),
                "volatility_regime":   vol_regime.iloc[i],
                "net_broker_flow":     b_flow,
                "signal_date":         date,
                "calculated_at":       calculated_at,
            })

        # ── Persist ───────────────────────────────────────────────────────────
        saved = 0
        for r in records:
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO nepse_signals
                    (symbol, date, pump_score, dump_score,
                     broker_accumulation, broker_distribution,
                     liquidity_spike, volatility_regime,
                     net_broker_flow, signal_date, calculated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    r["symbol"], r["date"],
                    r["pump_score"], r["dump_score"],
                    r["broker_accumulation"], r["broker_distribution"],
                    r["liquidity_spike"], r["volatility_regime"],
                    r["net_broker_flow"], r["signal_date"], r["calculated_at"],
                ))
                saved += 1
            except Exception as e:
                pass  # skip duplicate or malformed rows

        conn.commit()

        result_df = pd.DataFrame(records)
        print(f"  {symbol}: {len(price_df)} rows -> {saved} signals saved")
        return result_df

    except Exception as e:
        print(f"  [ERR] {symbol}: {e}")
        traceback.print_exc()
        return None
    finally:
        conn.close()


# ── CALCULATE ALL SIGNALS ─────────────────────────────────────────────────────
def calculate_all_signals():
    """
    Run calculate_signals_for() for every symbol in clean_price_history.
    Prints progress every 50 symbols and a final summary.
    """
    conn = get_db()
    symbols = [r[0] for r in conn.execute(
        "SELECT DISTINCT symbol FROM clean_price_history"
    ).fetchall()]
    conn.close()

    total = len(symbols)
    print(f"Calculating NEPSE signals for {total} symbols...")
    success, failed = 0, 0

    for i, symbol in enumerate(symbols, 1):
        result = calculate_signals_for(symbol)
        if result is not None:
            success += 1
        else:
            failed += 1

        if i % 50 == 0 or i == total:
            print(f"  [{i}/{total}]  done={success}  failed={failed}")

    print(f"\n[DONE] {success} symbols processed | {failed} skipped")


# ── GET CURRENT SIGNALS FOR A SYMBOL ─────────────────────────────────────────
def get_current_signals(symbol):
    """
    Return the most recent nepse_signals row for *symbol* as a dict.
    Used by the dashboard to display the live signal status.
    Returns None if no data exists.
    """
    conn = get_db()
    try:
        row = conn.execute("""
            SELECT * FROM nepse_signals
            WHERE  symbol = ?
            ORDER  BY date DESC
            LIMIT  1
        """, (symbol.upper(),)).fetchone()

        if row is None:
            return None

        cols = [d[0] for d in conn.execute(
            "SELECT * FROM nepse_signals LIMIT 0"
        ).description]
        return dict(zip(cols, row))

    except Exception as e:
        print(f"[ERR] get_current_signals({symbol}): {e}")
        return None
    finally:
        conn.close()


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 55)
    print("NEPSE Signal Calculator - Step 5b")
    print("=" * 55)

    # 1. Create table
    create_signals_table()
    print()

    # 2. Test with NABIL
    print("Calculating signals for NABIL...")
    df = calculate_signals_for("NABIL")

    if df is not None and not df.empty:
        latest = df.iloc[-1]
        print("\nLatest NABIL NEPSE signals:")
        print(f"  date               {latest['date']}")
        print(f"  pump_score         {latest['pump_score']:.1f} / 100")
        print(f"  dump_score         {latest['dump_score']:.1f} / 100")
        print(f"  liquidity_spike    {latest['liquidity_spike']:.2f}x avg volume")
        print(f"  volatility_regime  {latest['volatility_regime']}")
        print(f"  broker_accum       {latest['broker_accumulation']:.1f}%")
        print(f"  broker_dist        {latest['broker_distribution']:.1f}%")
        print(f"  net_broker_flow    {latest['net_broker_flow']:,.0f}")

        print("\nPump score distribution for NABIL:")
        bins = [0, 10, 30, 60, 100]
        labels = ["0-10 (none)", "11-30 (low)", "31-60 (medium)", "61-100 (high)"]
        cats = pd.cut(df["pump_score"], bins=bins, labels=labels, include_lowest=True)
        print(cats.value_counts().sort_index().to_string())
    print()

    # 4. Calculate all symbols
    answer = input("Calculate signals for all symbols? (y/n): ").strip().lower()
    if answer == "y":
        calculate_all_signals()
    else:
        print("Skipped.")
    print()

    # 5. DB totals
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM nepse_signals").fetchone()[0]
    symbols = conn.execute("SELECT COUNT(DISTINCT symbol) FROM nepse_signals").fetchone()[0]
    top_pump = conn.execute("""
        SELECT symbol, pump_score, date
        FROM   nepse_signals
        ORDER  BY pump_score DESC, date DESC
        LIMIT  5
    """).fetchall()
    conn.close()

    print(f"Total rows in nepse_signals: {total} ({symbols} symbols)")
    print("\nTop 5 highest pump scores (all time):")
    for r in top_pump:
        print(f"  {r[0]:<12} pump={r[1]:.0f}  date={r[2]}")

    print("\n[OK] Step 5b complete!")
