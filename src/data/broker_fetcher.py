# src/data/broker_fetcher.py
# Step 3: NEPSE Broker & Sector Data Fetcher
# Fetches broker-level trading data and sector index data from the NEPSE API
# and stores everything in the local SQLite database for accumulation signal detection.

import sqlite3
import os
import traceback
from datetime import datetime

import pandas as pd
from nepse import Nepse

# ── CONFIG ────────────────────────────────────────────────────────────────────
DB_PATH = "data/nepse.db"

# ── SETUP ─────────────────────────────────────────────────────────────────────
def get_nepse():
    """Initialize and return Nepse client with TLS verification disabled."""
    nepse = Nepse()
    nepse.setTLSVerification(False)
    return nepse


def get_db():
    """Return SQLite connection, creating the data folder if needed."""
    os.makedirs("data", exist_ok=True)
    return sqlite3.connect(DB_PATH)


# ── CREATE TABLES ─────────────────────────────────────────────────────────────
def create_broker_tables():
    """Create all 4 broker/sector tables if they don't already exist."""
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS floor_sheet (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            date            TEXT NOT NULL,
            symbol          TEXT NOT NULL,
            buyer_broker    INTEGER,
            seller_broker   INTEGER,
            quantity        REAL,
            rate            REAL,
            amount          REAL,
            fetched_at      TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS broker_summary (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            date                  TEXT NOT NULL,
            symbol                TEXT NOT NULL,
            total_trades          INTEGER,
            total_volume          REAL,
            top3_buyer_volume     REAL,
            top3_seller_volume    REAL,
            buyer_concentration   REAL,
            seller_concentration  REAL,
            net_broker_flow       REAL,
            fetched_at            TEXT,
            UNIQUE(date, symbol)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sector_index (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            date        TEXT NOT NULL,
            sector      TEXT NOT NULL,
            value       REAL,
            fetched_at  TEXT,
            UNIQUE(date, sector)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS supply_demand (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            date           TEXT NOT NULL,
            symbol         TEXT NOT NULL,
            total_buy_qty  REAL,
            total_sell_qty REAL,
            buy_sell_ratio REAL,
            fetched_at     TEXT,
            UNIQUE(date, symbol)
        )
    """)

    conn.commit()
    conn.close()
    print("[OK] Broker tables created successfully")


# ── FETCH FLOOR SHEET ─────────────────────────────────────────────────────────
def fetch_floor_sheet_for(symbol):
    """
    Fetch today's floor sheet (all broker trades) for a given symbol.
    Handles both list and dict API return types.
    Returns a DataFrame of the trades.
    """
    print(f"Fetching floor sheet for {symbol}...")
    nepse = get_nepse()
    conn = get_db()

    try:
        data = nepse.getFloorSheetOf(symbol)

        # Normalise return type to a DataFrame
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            df = pd.DataFrame()
            for key, val in data.items():
                if isinstance(val, list):
                    df = pd.DataFrame(val)
                    break
        else:
            df = pd.DataFrame()

        print(f"   Got {len(df)} trades")

        if df.empty:
            print("   No trades today (market may be closed or no activity)")
            return df

        print(f"   Columns: {list(df.columns)}")
        fetched_at = datetime.now().isoformat()
        today = datetime.now().strftime("%Y-%m-%d")

        for _, row in df.iterrows():
            try:
                conn.execute("""
                    INSERT INTO floor_sheet
                    (date, symbol, buyer_broker, seller_broker, quantity, rate, amount, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    today,
                    symbol,
                    int(row.get('buyerBrokerNo',  row.get('buyMemberId',  0)) or 0),
                    int(row.get('sellerBrokerNo', row.get('sellMemberId', 0)) or 0),
                    float(row.get('contractQuantity', row.get('quantity', 0)) or 0),
                    float(row.get('contractRate',     row.get('rate',     0)) or 0),
                    float(row.get('contractAmount',   row.get('amount',   0)) or 0),
                    fetched_at,
                ))
            except Exception as e:
                print(f"   Skipped trade row: {e}")

        conn.commit()
        print(f"[OK] Saved {len(df)} floor-sheet trades for {symbol}")
        return df

    except Exception as e:
        print(f"[ERR] Error fetching floor sheet for {symbol}: {e}")
        traceback.print_exc()
        return pd.DataFrame()
    finally:
        conn.close()


# ── BROKER SUMMARY ────────────────────────────────────────────────────────────
def calculate_broker_summary(symbol, date=None):
    """
    Read today's floor_sheet rows for *symbol* from SQLite, calculate broker
    concentration metrics, and persist the result to broker_summary.

    Returns a dict with all computed metrics, or None on failure.
    """
    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")

    print(f"Calculating broker summary for {symbol} on {date}...")
    conn = get_db()

    try:
        df = pd.read_sql_query(
            "SELECT * FROM floor_sheet WHERE symbol = ? AND date = ?",
            conn, params=(symbol, date)
        )

        if df.empty:
            print(f"   No floor-sheet data for {symbol} on {date} - skipping summary")
            return None

        total_trades  = len(df)
        total_volume  = float(df['quantity'].sum())

        # Top-3 buyer brokers by volume
        buyer_vol = df.groupby('buyer_broker')['quantity'].sum().nlargest(3)
        top3_buyer_volume = float(buyer_vol.sum())

        # Top-3 seller brokers by volume
        seller_vol = df.groupby('seller_broker')['quantity'].sum().nlargest(3)
        top3_seller_volume = float(seller_vol.sum())

        buyer_concentration  = (top3_buyer_volume  / total_volume * 100) if total_volume else 0.0
        seller_concentration = (top3_seller_volume / total_volume * 100) if total_volume else 0.0
        net_broker_flow      = top3_buyer_volume - top3_seller_volume

        fetched_at = datetime.now().isoformat()

        conn.execute("""
            INSERT OR REPLACE INTO broker_summary
            (date, symbol, total_trades, total_volume,
             top3_buyer_volume, top3_seller_volume,
             buyer_concentration, seller_concentration,
             net_broker_flow, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            date, symbol, total_trades, total_volume,
            top3_buyer_volume, top3_seller_volume,
            round(buyer_concentration, 2), round(seller_concentration, 2),
            net_broker_flow, fetched_at,
        ))
        conn.commit()

        result = {
            "date":                 date,
            "symbol":               symbol,
            "total_trades":         total_trades,
            "total_volume":         total_volume,
            "top3_buyer_volume":    top3_buyer_volume,
            "top3_seller_volume":   top3_seller_volume,
            "buyer_concentration":  round(buyer_concentration, 2),
            "seller_concentration": round(seller_concentration, 2),
            "net_broker_flow":      net_broker_flow,
        }

        print(f"[OK] Broker summary saved for {symbol}")
        print(f"   Total trades:          {total_trades}")
        print(f"   Total volume:          {total_volume:,.0f}")
        print(f"   Buyer concentration:   {buyer_concentration:.1f}%")
        print(f"   Seller concentration:  {seller_concentration:.1f}%")
        print(f"   Net broker flow:       {net_broker_flow:,.0f}")
        return result

    except Exception as e:
        print(f"[ERR] Error calculating broker summary for {symbol}: {e}")
        traceback.print_exc()
        return None
    finally:
        conn.close()


# ── FETCH SECTOR INDICES ──────────────────────────────────────────────────────
def fetch_sector_indices():
    """
    Fetch all NEPSE sub-index (sector) values via getNepseSubIndices().
    Handles list and dict return types.
    Returns a DataFrame of the sector data.
    """
    print("Fetching sector indices...")
    nepse = get_nepse()
    conn = get_db()

    try:
        data = nepse.getNepseSubIndices()

        # Normalise to DataFrame
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            df = pd.DataFrame()
            for key, val in data.items():
                if isinstance(val, list) and len(val) > 0:
                    df = pd.DataFrame(val)
                    break
            if df.empty:
                # dict itself may be the single record
                df = pd.DataFrame([data])
        else:
            print(f"   Unexpected data type: {type(data)}")
            return pd.DataFrame()

        if df.empty:
            print("   No sector index data returned")
            return df

        print(f"   Got {len(df)} sector entries")
        print(f"   Columns: {list(df.columns)}")

        fetched_at = datetime.now().isoformat()
        today      = datetime.now().strftime("%Y-%m-%d")
        saved      = 0

        for _, row in df.iterrows():
            try:
                # Try several possible column names for sector label
                sector = (
                    row.get('index') or
                    row.get('name') or
                    row.get('sectorName') or
                    row.get('indexName') or
                    'Unknown'
                )
                # Try several possible column names for numeric value
                value = (
                    row.get('currentValue') or
                    row.get('close') or
                    row.get('value') or
                    0
                )
                conn.execute("""
                    INSERT OR REPLACE INTO sector_index (date, sector, value, fetched_at)
                    VALUES (?, ?, ?, ?)
                """, (today, str(sector), float(value or 0), fetched_at))
                saved += 1
            except Exception as e:
                print(f"   Skipped sector row: {e}")

        conn.commit()
        print(f"[OK] Saved {saved} sector index entries")
        return df

    except Exception as e:
        print(f"[ERR] Error fetching sector indices: {e}")
        traceback.print_exc()
        return pd.DataFrame()
    finally:
        conn.close()


# ── FETCH SUPPLY & DEMAND ─────────────────────────────────────────────────────
def fetch_supply_demand(symbol):
    """
    Fetch the buy/sell order-book summary for a symbol via getSupplyDemand().
    Not all symbols support this endpoint — errors are caught gracefully.
    Returns a dict with quantities and ratio, or None on failure.
    """
    print(f"Fetching supply/demand for {symbol}...")
    nepse = get_nepse()
    conn  = get_db()

    try:
        # getSupplyDemand() takes no arguments; returns data for all symbols
        data = nepse.getSupplyDemand()

        # Normalise to a list of dicts
        if isinstance(data, dict):
            rows = []
            for val in data.values():
                if isinstance(val, list):
                    rows = val
                    break
        elif isinstance(data, list):
            rows = data
        else:
            rows = []

        # Filter to our target symbol if possible
        symbol_rows = [r for r in rows if isinstance(r, dict) and
                       str(r.get('symbol', r.get('stockSymbol', ''))).upper() == symbol.upper()]
        target_rows = symbol_rows if symbol_rows else rows

        if not target_rows:
            print(f"   No supply/demand data for {symbol}")
            return None

        df = pd.DataFrame(target_rows)
        buy_col  = next((c for c in df.columns if 'buy'  in c.lower()), None)
        sell_col = next((c for c in df.columns if 'sell' in c.lower()), None)
        total_buy_qty  = float(df[buy_col].sum())  if buy_col  else 0.0
        total_sell_qty = float(df[sell_col].sum()) if sell_col else 0.0

        buy_sell_ratio = (total_buy_qty / total_sell_qty) if total_sell_qty else 0.0

        fetched_at = datetime.now().isoformat()
        today      = datetime.now().strftime("%Y-%m-%d")

        conn.execute("""
            INSERT OR REPLACE INTO supply_demand
            (date, symbol, total_buy_qty, total_sell_qty, buy_sell_ratio, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (today, symbol, total_buy_qty, total_sell_qty, round(buy_sell_ratio, 4), fetched_at))
        conn.commit()

        result = {
            "symbol":         symbol,
            "date":           today,
            "total_buy_qty":  total_buy_qty,
            "total_sell_qty": total_sell_qty,
            "buy_sell_ratio": round(buy_sell_ratio, 4),
        }

        print(f"[OK] Supply/demand saved for {symbol}")
        print(f"   Buy qty:   {total_buy_qty:,.0f}")
        print(f"   Sell qty:  {total_sell_qty:,.0f}")
        print(f"   Ratio:     {buy_sell_ratio:.4f}")
        return result

    except Exception as e:
        print(f"[ERR] Error fetching supply/demand for {symbol}: {e}")
        traceback.print_exc()
        return None
    finally:
        conn.close()


# ── MAIN TEST RUN ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 55)
    print("NEPSE Broker & Sector Fetcher - Step 3")
    print("=" * 55)

    # 1. Create tables
    create_broker_tables()
    print()

    # 2. Floor sheet for NABIL
    fs_df = fetch_floor_sheet_for("NABIL")
    print(f"   Floor sheet rows returned: {len(fs_df)}")
    print()

    # 3. Broker summary for NABIL (uses floor_sheet rows just saved)
    summary = calculate_broker_summary("NABIL")
    if summary:
        print(f"   Summary: {summary}")
    print()

    # 4. Sector indices
    sector_df = fetch_sector_indices()
    print(f"   Sector rows returned: {len(sector_df)}")
    print()

    # 5. Supply/demand for NABIL
    sd = fetch_supply_demand("NABIL")
    if sd:
        print(f"   Supply/demand: {sd}")
    print()

    # 6. Database row counts
    conn = get_db()
    print("[DB] Database row counts:")
    print(f"   floor_sheet:    {conn.execute('SELECT COUNT(*) FROM floor_sheet').fetchone()[0]}")
    print(f"   broker_summary: {conn.execute('SELECT COUNT(*) FROM broker_summary').fetchone()[0]}")
    print(f"   sector_index:   {conn.execute('SELECT COUNT(*) FROM sector_index').fetchone()[0]}")
    print(f"   supply_demand:  {conn.execute('SELECT COUNT(*) FROM supply_demand').fetchone()[0]}")
    conn.close()

    print("\n[OK] Step 3 complete!")
