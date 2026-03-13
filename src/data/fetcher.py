# src/data/fetcher.py
# Step 2: NEPSE Data Fetcher
# Fetches price, volume, floor sheet and market data from nepalstock.com
# and stores everything in a local SQLite database.
import sqlite3
import os

# Force SQLite — ignore any PostgreSQL environment variables
os.environ.pop("DATABASE_URL", None)
os.environ.pop("POSTGRES_URL", None)
from nepse import Nepse
import pandas as pd
import sqlite3
from db import get_db
import os
from datetime import datetime

# ── CONFIG ────────────────────────────────────────────────────────────────────
DB_PATH = "data/nepse.db"

# ── SETUP ─────────────────────────────────────────────────────────────────────
def get_nepse():
    """Initialize and return Nepse client."""
    nepse = Nepse()
    nepse.setTLSVerification(False)
    return nepse


# ── CREATE TABLES ─────────────────────────────────────────────────────────────
def create_tables():
    """Create all database tables if they don't exist."""
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS price_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol      TEXT NOT NULL,
            date        TEXT NOT NULL,
            open        REAL,
            high        REAL,
            low         REAL,
            close       REAL,
            volume      REAL,
            fetched_at  TEXT,
            UNIQUE(symbol, date)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            id          INTEGER PRIMARY KEY,
            symbol      TEXT NOT NULL,
            name        TEXT,
            sector      TEXT,
            fetched_at  TEXT,
            UNIQUE(symbol)
        )
    """)

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
        CREATE TABLE IF NOT EXISTS market_summary (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            date           TEXT NOT NULL,
            nepse_index    REAL,
            total_turnover REAL,
            total_volume   REAL,
            fetched_at     TEXT,
            UNIQUE(date)
        )
    """)

    conn.commit()
    conn.close()
    print("✅ Tables created successfully")

# ── FETCH COMPANY LIST ────────────────────────────────────────────────────────
def fetch_company_list():
    """Fetch all listed companies and save to database."""
    print("Fetching company list...")
    nepse = get_nepse()
    conn = get_db()

    try:
        companies = nepse.getCompanyList()
        df = pd.DataFrame(companies)
        print(f"   Found {len(df)} companies")
        print(f"   Columns: {list(df.columns)}")

        fetched_at = datetime.now().isoformat()

        for _, row in df.iterrows():
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO companies (symbol, name, sector, fetched_at)
                    VALUES (?, ?, ?, ?)
                """, (
                    str(row.get('symbol', row.get('stockSymbol', ''))),
                    str(row.get('companyName', row.get('name', ''))),
                    str(row.get('sectorName', '')),
                    fetched_at
                ))
            except Exception as e:
                print(f"   Skipped row: {e}")

        conn.commit()
        print(f"✅ Saved {len(df)} companies to database")
        return df

    except Exception as e:
        print(f"❌ Error fetching companies: {e}")
        return None
    finally:
        conn.close()

# ── FETCH PRICE HISTORY ───────────────────────────────────────────────────────
def fetch_price_history(symbol):
    """Fetch full price/volume history for a stock symbol."""
    print(f"Fetching price history for {symbol}...")
    nepse = get_nepse()
    conn = get_db()

    try:
        data = nepse.getCompanyPriceVolumeHistory(symbol)

        # Handle different return formats
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            for key, val in data.items():
                if isinstance(val, list) and len(val) > 0:
                    df = pd.DataFrame(val)
                    break
            else:
                print(f"   Unexpected dict format: {list(data.keys())}")
                return None
        else:
            print(f"   Unexpected data type: {type(data)}")
            return None

        print(f"   Got {len(df)} rows")
        print(f"   Columns: {list(df.columns)}")

        fetched_at = datetime.now().isoformat()
        saved = 0

        for _, row in df.iterrows():
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO price_history
                    (symbol, date, open, high, low, close, volume, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    symbol,
                    str(row.get('date', row.get('businessDate', ''))),
                    float(row.get('openPrice',  row.get('open',  0)) or 0),
                    float(row.get('highPrice',  row.get('high',  0)) or 0),
                    float(row.get('lowPrice',   row.get('low',   0)) or 0),
                    float(row.get('closePrice', row.get('close', 0)) or 0),
                    float(row.get('totalTradedQuantity', row.get('volume', 0)) or 0),
                    fetched_at
                ))
                saved += 1
            except Exception as e:
                print(f"   Skipped row: {e}")

        conn.commit()
        print(f"✅ Saved {saved} rows for {symbol}")
        return df

    except Exception as e:
        print(f"❌ Error fetching {symbol}: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        conn.close()

# ── FETCH PRICE HISTORY FOR ALL COMPANIES ────────────────────────────────────
# ── FETCH PRICE HISTORY FOR ALL COMPANIES (PARALLEL) ─────────────────────────
def fetch_all_price_histories(max_workers=10):
    """Fetch price history for every company using parallel threads."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    conn = get_db()
    symbols = [row[0] for row in conn.execute("SELECT symbol FROM companies").fetchall()]
    conn.close()

    total = len(symbols)
    print(f"Fetching price history for {total} companies using {max_workers} parallel workers...")

    success, failed = 0, 0
    completed = 0

    def process_one(symbol):
        try:
            result = fetch_price_history(symbol)
            return symbol, result is not None
        except:
            return symbol, False

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_one, symbol): symbol for symbol in symbols}
        for future in as_completed(futures):
            symbol, ok = future.result()
            completed += 1
            if ok:
                success += 1
            else:
                failed += 1
            if completed % 20 == 0:
                print(f"Progress: {completed}/{total} | Success: {success} | Failed: {failed}")

    print(f"\n✅ Done. Success: {success} | Failed: {failed}")
# ── FETCH FLOOR SHEET ─────────────────────────────────────────────────────────
def fetch_floor_sheet_for(symbol):
    """Fetch today's floor sheet (broker trades) for a symbol."""
    print(f"Fetching floor sheet for {symbol}...")
    nepse = get_nepse()
    conn = get_db()

    try:
        data = nepse.getFloorSheetOf(symbol)

        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            for key, val in data.items():
                if isinstance(val, list):
                    df = pd.DataFrame(val)
                    break
            else:
                df = pd.DataFrame()
        else:
            df = pd.DataFrame()

        print(f"   Got {len(df)} trades")

        if len(df) == 0:
            print("   No trades today (market may be closed)")
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
                    fetched_at
                ))
            except Exception as e:
                print(f"   Skipped row: {e}")

        conn.commit()
        print(f"✅ Saved floor sheet for {symbol}")
        return df

    except Exception as e:
        print(f"❌ Error fetching floor sheet: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        conn.close()

# ── FETCH MARKET SUMMARY ──────────────────────────────────────────────────────
def fetch_market_summary():
    """Fetch today's overall market summary."""
    print("Fetching market summary...")
    nepse = get_nepse()
    conn = get_db()

    try:
        summary = nepse.getSummary()
        fetched_at = datetime.now().isoformat()
        today = datetime.now().strftime("%Y-%m-%d")

        if isinstance(summary, list) and len(summary) > 0:
            print(f"   Got {len(summary)} summary items")
            print(f"   First item keys: {list(summary[0].keys()) if isinstance(summary[0], dict) else summary[0]}")

            # Find NEPSE main index entry
            nepse_data = None
            for item in summary:
                if isinstance(item, dict):
                    name = str(item.get('index', item.get('name', ''))).upper()
                    if 'NEPSE' in name:
                        nepse_data = item
                        break

            if not nepse_data:
                nepse_data = summary[0]

            conn.execute("""
                INSERT OR REPLACE INTO market_summary
                (date, nepse_index, total_turnover, total_volume, fetched_at)
                VALUES (?, ?, ?, ?, ?)
            """, (
                today,
                float(nepse_data.get('currentValue', nepse_data.get('close', 0)) or 0),
                0.0,
                0.0,
                fetched_at
            ))
            conn.commit()
            print("✅ Market summary saved")

        elif isinstance(summary, dict):
            conn.execute("""
                INSERT OR REPLACE INTO market_summary
                (date, nepse_index, total_turnover, total_volume, fetched_at)
                VALUES (?, ?, ?, ?, ?)
            """, (
                today,
                float(summary.get('nepseIndex', summary.get('currentValue', 0)) or 0),
                float(summary.get('totalTurnover', 0) or 0),
                float(summary.get('totalTradedShares', 0) or 0),
                fetched_at
            ))
            conn.commit()
            print("✅ Market summary saved")

        return summary

    except Exception as e:
        print(f"❌ Error fetching market summary: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        conn.close()

# ── MAIN TEST RUN ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 50)
    print("NEPSE Data Fetcher — Step 2")
    print("=" * 50)

    # 1. Create tables
    create_tables()

    # 2. Fetch company list
    companies = fetch_company_list()

    # 3. Test price history with one stock
    fetch_price_history("NABIL")

    # 4. Fetch market summary
    fetch_market_summary()

    # 5. Database summary
    conn = get_db()
    print("\n📊 Database summary:")
    print(f"   Companies:     {conn.execute('SELECT COUNT(*) FROM companies').fetchone()[0]}")
    print(f"   Price rows:    {conn.execute('SELECT COUNT(*) FROM price_history').fetchone()[0]}")
    print(f"   Floor trades:  {conn.execute('SELECT COUNT(*) FROM floor_sheet').fetchone()[0]}")
    conn.close()

    print("\n✅ Step 2 complete! Your database is ready.")