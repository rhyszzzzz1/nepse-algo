# src/data/cleaner.py
# Step 4: NEPSE Price Data Cleaner
# Cleans raw price_history rows, computes derived indicators, and tags each row
# with a market condition (bull / bear / sideways).
# Uses the 'ta' library (NOT pandas_ta).

import sqlite3
from db import get_db
import os
import traceback
from datetime import datetime

import pandas as pd
import ta

# ── CONFIG ────────────────────────────────────────────────────────────────────
DB_PATH = "data/nepse.db"


# ── DB HELPER ─────────────────────────────────────────────────────────────────

# ── CREATE TABLE ──────────────────────────────────────────────────────────────
def create_clean_table():
    """Create the clean_price_history table if it does not already exist."""
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS clean_price_history (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol           TEXT    NOT NULL,
            date             TEXT    NOT NULL,
            open             REAL,
            high             REAL,
            low              REAL,
            close            REAL,
            volume           REAL,
            price_change_pct REAL,
            volume_ma20      REAL,
            volume_ratio     REAL,
            atr14            REAL,
            market_condition TEXT,
            is_clean         INTEGER DEFAULT 1,
            cleaned_at       TEXT,
            UNIQUE(symbol, date)
        )
    """)
    conn.commit()
    conn.close()
    print("[OK] clean_price_history table ready")


# ── CLEAN ONE SYMBOL ──────────────────────────────────────────────────────────
def clean_symbol(symbol):
    """
    Load raw price data for *symbol*, apply cleaning rules, calculate derived
    fields (ATR-14, volume ratio, EMA-50 market condition), and persist the
    result into clean_price_history.

    Returns the cleaned DataFrame, or None on failure.
    """
    conn = get_db()
    try:
        df = pd.read_sql_query(
            """
            SELECT symbol, date, open, high, low, close, volume
            FROM   price_history
            WHERE  symbol = ?
            ORDER  BY date ASC
            """,
            conn, params=(symbol,)
        )
    except Exception as e:
        print(f"  [ERR] DB read failed for {symbol}: {e}")
        conn.close()
        return None

    if df.empty:
        conn.close()
        return None

    try:
        # ── CLEANING RULES ────────────────────────────────────────────────────
        original_len = len(df)

        # Ensure numeric types
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Drop rows with NaN in critical columns
        df.dropna(subset=["close", "volume"], inplace=True)

        # Rule 1: close and volume must be positive
        df = df[(df["close"] > 0) & (df["volume"] > 0)]

        # Rule 2: high must be >= low
        df = df[df["high"] >= df["low"]]

        # Rule 3: fill zero open with previous close
        df["open"] = df["open"].replace(0, float("nan"))
        df["open"] = df["open"].fillna(df["close"].shift(1))
        df["open"] = df["open"].fillna(df["close"])   # first row fallback

        # Sort by date (should already be sorted, but enforce it)
        df.sort_values("date", inplace=True)
        df.reset_index(drop=True, inplace=True)

        # Rule 4: remove circuit-breaker anomalies (> 30% daily move)
        df["_prev_close"] = df["close"].shift(1)
        df["_pct"] = (df["close"] - df["_prev_close"]).abs() / df["_prev_close"] * 100
        # Keep first row (no previous close) and rows within ±30 %
        df = df[(df["_pct"].isna()) | (df["_pct"] <= 30)]
        df.drop(columns=["_prev_close", "_pct"], inplace=True)
        df.reset_index(drop=True, inplace=True)

        removed = original_len - len(df)

        # ── DERIVED FIELDS ────────────────────────────────────────────────────
        # price_change_pct
        df["price_change_pct"] = (
            (df["close"] - df["close"].shift(1)) / df["close"].shift(1) * 100
        ).round(4)

        # volume_ma20
        df["volume_ma20"] = df["volume"].rolling(window=20, min_periods=1).mean()

        # volume_ratio
        df["volume_ratio"] = (df["volume"] / df["volume_ma20"]).round(4)

        # ATR-14 via 'ta' library — requires at least 14 rows
        if len(df) >= 14:
            atr = ta.volatility.AverageTrueRange(
                high=df["high"],
                low=df["low"],
                close=df["close"],
                window=14,
                fillna=False,
            )
            df["atr14"] = atr.average_true_range().round(4)
        else:
            df["atr14"] = float("nan")

        # EMA-50 market condition — requires at least 50 rows for meaningful signal
        if len(df) >= 50:
            ema_ind = ta.trend.EMAIndicator(close=df["close"], window=50, fillna=False)
            ema50 = ema_ind.ema_indicator()

            ema50_prev = ema50.shift(1)
            ema_rising = ema50 > ema50_prev
            above_ema  = df["close"] > ema50

            conditions = []
            for i in range(len(df)):
                if pd.isna(ema50.iloc[i]):
                    conditions.append("sideways")
                elif ema_rising.iloc[i] and above_ema.iloc[i]:
                    conditions.append("bull")
                elif (not ema_rising.iloc[i]) and (not above_ema.iloc[i]):
                    conditions.append("bear")
                else:
                    conditions.append("sideways")
        else:
            # Too few rows to compute a meaningful EMA-50
            conditions = ["sideways"] * len(df)

        df["market_condition"] = conditions

        # ── PERSIST ───────────────────────────────────────────────────────────
        cleaned_at = datetime.now().isoformat()
        saved = 0
        for _, row in df.iterrows():
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO clean_price_history
                    (symbol, date, open, high, low, close, volume,
                     price_change_pct, volume_ma20, volume_ratio,
                     atr14, market_condition, is_clean, cleaned_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                """, (
                    row["symbol"],
                    row["date"],
                    float(row["open"]  if not pd.isna(row["open"])  else 0),
                    float(row["high"]  if not pd.isna(row["high"])  else 0),
                    float(row["low"]   if not pd.isna(row["low"])   else 0),
                    float(row["close"] if not pd.isna(row["close"]) else 0),
                    float(row["volume"]if not pd.isna(row["volume"])else 0),
                    float(row["price_change_pct"] if not pd.isna(row["price_change_pct"]) else 0),
                    float(row["volume_ma20"]       if not pd.isna(row["volume_ma20"])      else 0),
                    float(row["volume_ratio"]      if not pd.isna(row["volume_ratio"])     else 0),
                    float(row["atr14"]             if not pd.isna(row["atr14"])            else 0),
                    row["market_condition"],
                    cleaned_at,
                ))
                saved += 1
            except Exception as e:
                print(f"  [WARN] Row skipped for {symbol}: {e}")

        conn.commit()
        print(f"  {symbol}: {original_len} raw -> {len(df)} clean "
              f"({removed} removed) | {saved} saved")
        return df

    except Exception as e:
        print(f"  [ERR] Failed cleaning {symbol}: {e}")
        traceback.print_exc()
        return None
    finally:
        conn.close()


# ── CLEAN ALL SYMBOLS ─────────────────────────────────────────────────────────
def clean_all_symbols(max_workers=10):
    """
    Fetch every symbol from the companies table and run clean_symbol() on each
    using parallel threads. Prints a progress counter and a final summary.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    conn = get_db()
    symbols = [r[0] for r in conn.execute("SELECT symbol FROM companies").fetchall()]
    conn.close()

    total = len(symbols)
    print(f"\nCleaning {total} symbols using {max_workers} parallel workers...")
    success, failed, total_rows = 0, 0, 0
    completed = 0

    def process_one(symbol):
        try:
            result = clean_symbol(symbol)
            return symbol, result
        except:
            return symbol, None

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_one, symbol): symbol for symbol in symbols}
        for future in as_completed(futures):
            symbol, result = future.result()
            completed += 1
            if result is not None:
                success += 1
                total_rows += len(result)
            else:
                failed += 1
            
            if completed % 20 == 0:
                print(f"Progress: {completed}/{total} | Success: {success} | Failed: {failed}")

    print(f"\n[DONE] Cleaned {success} symbols | {failed} failed | {total_rows} total rows")


# ── MARKET CONDITION SUMMARY ──────────────────────────────────────────────────
def get_market_condition_summary():
    """
    Print a breakdown of bull/bear/sideways row counts per symbol, and
    highlight the top-10 most bullish and top-10 most bearish symbols.
    """
    conn = get_db()
    try:
        df = pd.read_sql_query("""
            SELECT   symbol, market_condition, COUNT(*) AS cnt
            FROM     clean_price_history
            GROUP BY symbol, market_condition
        """, conn)

        pivot = df.pivot_table(
            index="symbol", columns="market_condition",
            values="cnt", aggfunc="sum", fill_value=0
        )

        # Ensure all three columns exist even if data is sparse
        for col in ["bull", "bear", "sideways"]:
            if col not in pivot.columns:
                pivot[col] = 0

        pivot["total"] = pivot["bull"] + pivot["bear"] + pivot["sideways"]
        pivot["bull_pct"] = (pivot["bull"] / pivot["total"] * 100).round(1)
        pivot["bear_pct"] = (pivot["bear"] / pivot["total"] * 100).round(1)
        pivot.reset_index(inplace=True)

        print("\n--- Top 10 BULL symbols (most bull-condition days) ---")
        top_bull = pivot.nlargest(10, "bull")[["symbol", "bull", "bull_pct"]]
        print(top_bull.to_string(index=False))

        print("\n--- Top 10 BEAR symbols (most bear-condition days) ---")
        top_bear = pivot.nlargest(10, "bear")[["symbol", "bear", "bear_pct"]]
        print(top_bear.to_string(index=False))

        return pivot

    except Exception as e:
        print(f"[ERR] Market condition summary failed: {e}")
        traceback.print_exc()
        return None
    finally:
        conn.close()


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 55)
    print("NEPSE Data Cleaner - Step 4")
    print("=" * 55)

    # 1. Create table
    create_clean_table()
    print()

    # 2. Test with NABIL
    print("Testing with NABIL...")
    nabil_df = clean_symbol("NABIL")
    if nabil_df is not None and not nabil_df.empty:
        print("\nFirst 5 cleaned rows for NABIL:")
        print(nabil_df[["date", "close", "price_change_pct",
                         "volume_ratio", "atr14", "market_condition"]].head().to_string(index=False))
        print(f"\nMarket condition breakdown for NABIL:")
        print(nabil_df["market_condition"].value_counts().to_string())
    print()

    # 3. Prompt to clean all symbols
    answer = input("Clean all symbols? (y/n): ").strip().lower()
    if answer == "y":
        clean_all_symbols()
    else:
        print("Skipped full clean.")

    # 4. Market condition summary
    print()
    get_market_condition_summary()

    # 5. Total row count
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM clean_price_history").fetchone()[0]
    conn.close()
    print(f"\nTotal rows in clean_price_history: {total}")
    print("\n[OK] Step 4 complete!")
