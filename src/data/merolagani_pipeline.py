# src/data/merolagani_pipeline.py
# Scrapes Merolagani for company metadata, news, dividends, OHLCV, and floorsheet.

import os
import re
import io
import json
import time
import math
import sqlite3
import logging
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup

from floorsheet_pipeline import get_db, ensure_schema, compute_broker_summary, compute_daily_price

# =========================================================
# USER SETTINGS
# =========================================================

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if "RAILWAY_VOLUME_MOUNT_PATH" in os.environ:
    DB_PATH = os.path.join(os.environ["RAILWAY_VOLUME_MOUNT_PATH"], "nepse.db")
else:
    DB_PATH = os.path.join(ROOT, "data", "nepse.db")

OUTPUT_DIR = os.path.join(ROOT, "data", "merolagani_output")

# ---------- TESTING ----------
LIMIT_SYMBOLS = None               # None = process all symbols
MAX_FLOORSHEET_PAGES_OVERRIDE = 10 # None = auto
MAX_NEWS_PAGES_OVERRIDE = 5        # None = auto
MAX_DIVIDEND_PAGES_OVERRIDE = 5    # None = auto

# ---------- DATES ----------
OHLCV_START_DATE = "2025-01-01"
OHLCV_END_DATE = datetime.now().strftime("%Y-%m-%d")

# Floorsheet backfill ranges
FLOOR_START_DATE = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
FLOOR_END_DATE = datetime.now().strftime("%Y-%m-%d")

# ---------- NETWORK ----------
REQUEST_TIMEOUT = 40
SLEEP_BETWEEN_REQUESTS = 0.5
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

BASE_MEROLAGANI = "https://www.merolagani.com"
AUTOSUGGEST_URL = f"{BASE_MEROLAGANI}/handlers/AutoSuggestHandler.ashx?type=Company"
TV_SYMBOL_RESOLVE_URL = None
TV_HISTORY_URL = None

# =========================================================
# ACTIVE SYMBOLS (Fallback if DB is empty)
# =========================================================
ACTIVE_SYMBOLS_TEXT = r"""
ADBL NABIL NICA
"""

# =========================================================
# LOGGING
# =========================================================
logger = logging.getLogger("merolagani_pipeline")
logger.setLevel(logging.INFO)

if not logger.handlers:
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(
        logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
    )
    logger.addHandler(stream_handler)


# =========================================================
# HELPERS
# =========================================================
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def daterange(start_date: datetime, end_date: datetime):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)

def to_mmddyyyy(date_obj: datetime) -> str:
    return date_obj.strftime("%m/%d/%Y")

def to_yyyy_mm_dd(date_obj: datetime) -> str:
    return date_obj.strftime("%Y-%m-%d")

def clean_text(x) -> str:
    if x is None:
        return ""
    return re.sub(r"\s+", " ", str(x)).strip()

def safe_sleep():
    time.sleep(SLEEP_BETWEEN_REQUESTS)

def normalize_sql_column(name: str) -> str:
    name = clean_text(name).lower()
    name = name.replace("%", "percent")
    name = re.sub(r"[^a-z0-9_]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        return "col"
    if re.match(r"^\d", name):
        name = f"c_{name}"
    return name[:120]

def dedupe_columns(cols: List[str]) -> List[str]:
    seen = {}
    out = []
    for c in cols:
        base = normalize_sql_column(c)
        if base not in seen:
            seen[base] = 0
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base}_{seen[base]}")
    return out

def extract_symbols_from_text(raw_text: str) -> List[str]:
    junk = {"SCRIPT", "VERSION", "INFO", "OUTPUT", "LIMIT", "TRUE", "FALSE",
            "NONE", "DATE", "PAGE", "PAGING", "BUTTON", "NEWS", "ABOUT",
            "DIVIDEND", "FLOORSHEET", "CONTENTPLACEHOLDER", "EVENTTARGET",
            "EVENTARGUMENT", "VIEWSTATE", "ASYNCPOST", "UPDATEPANEL"}
    candidates = re.findall(r"\b[A-Z][A-Z0-9]{1,11}\b", raw_text.upper())
    cleaned = []
    for token in candidates:
        if token in junk or token.startswith("CTL00") or token.isdigit() or (token.startswith("PC") and token[2:].isdigit()):
            continue
        cleaned.append(token)
    seen = set()
    return [sym for sym in cleaned if not (sym in seen or seen.add(sym))]

def parse_aspx_delta_tables(text: str) -> List[pd.DataFrame]:
    dfs = []
    possible_html_chunks = re.findall(r"(<table.*?</table>)", text, flags=re.I | re.S)
    for html in possible_html_chunks:
        try:
            dfs.extend(pd.read_html(io.StringIO(html)))
        except Exception:
            pass
    return dfs

def try_read_html_tables(text: str) -> List[pd.DataFrame]:
    dfs = []
    try:
        dfs.extend(pd.read_html(io.StringIO(text)))
    except Exception:
        pass
    dfs.extend(parse_aspx_delta_tables(text))
    return dfs


# =========================================================
# DATABASE STORAGE (Native to nepse-algo)
# =========================================================
class DataStorage:
    def __init__(self, db_path: str, output_dir: str):
        self.db_path = db_path
        self.output_dir = output_dir
        ensure_dir(output_dir)
        self.conn = get_db()
        ensure_schema()
        self._ensure_custom_schemas()

    def _ensure_custom_schemas(self):
        # Extend default schema with Merolagani specific tables
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS instruments (
                symbol TEXT PRIMARY KEY,
                label TEXT, 
                source TEXT
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS about_company (
                symbol TEXT PRIMARY KEY,
                fetched_at TEXT
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS dividend (
                symbol TEXT,
                fetched_at TEXT
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS news (
                symbol TEXT,
                fetched_at TEXT
            )
        """)
        self.conn.commit()

    def close(self):
        try:
            self.conn.commit()
            self.conn.close()
        except:
            pass

    def save_df_to_csv(self, df: pd.DataFrame, path: str):
        ensure_dir(os.path.dirname(path))
        df.to_csv(path, index=False, encoding="utf-8-sig")

    def save_df_to_sqlite(self, df: pd.DataFrame, table_name: str, if_exists: str = "append", add_fetched_at=True):
        if df is None or df.empty:
            return

        df2 = df.copy()
        if add_fetched_at and 'fetched_at' not in df2.columns:
            df2['fetched_at'] = datetime.now().isoformat()

        for col in df2.columns:
            df2[col] = df2[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x)
        
        df2.columns = dedupe_columns([str(c) for c in df2.columns])
        
        # Automatically alter table if new columns are found
        if if_exists == "append":
            try:
                existing_cols = {row[1] for row in self.conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
                if existing_cols:
                    for col in df2.columns:
                        if col not in existing_cols:
                            try:
                                self.conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} TEXT")
                                self.conn.commit()
                                logger.info(f"Added new column '{col}' to table '{table_name}'")
                            except Exception as e:
                                logger.warning(f"Failed to add column {col} to {table_name}: {e}")
            except Exception as e:
                logger.warning(f"Error checking table schema for {table_name}: {e}")

        df2.to_sql(table_name, self.conn, if_exists=if_exists, index=False)

    def replace_table(self, df: pd.DataFrame, table_name: str):
        self.save_df_to_sqlite(df, table_name, if_exists="replace")

    def create_indexes(self):
        cursor = self.conn.cursor()
        for sql in [
            "CREATE INDEX IF NOT EXISTS idx_instruments_symbol ON instruments(symbol)",
            "CREATE INDEX IF NOT EXISTS idx_news_symbol ON news(symbol)",
            "CREATE INDEX IF NOT EXISTS idx_dividend_symbol ON dividend(symbol)",
            "CREATE INDEX IF NOT EXISTS idx_about_symbol ON about_company(symbol)"
        ]:
            try: cursor.execute(sql)
            except: pass
        self.conn.commit()


# =========================================================
# HTTP CLIENT
# =========================================================
class MerolaganiClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Referer": BASE_MEROLAGANI,
            "Origin": BASE_MEROLAGANI,
        })

    def get(self, url: str, **kwargs) -> requests.Response:
        resp = self.session.get(url, timeout=REQUEST_TIMEOUT, **kwargs)
        resp.raise_for_status()
        safe_sleep()
        return resp

    def post(self, url: str, data=None, headers=None, **kwargs) -> requests.Response:
        hdrs = headers or {}
        resp = self.session.post(url, data=data, headers=hdrs, timeout=REQUEST_TIMEOUT, **kwargs)
        resp.raise_for_status()
        safe_sleep()
        return resp

# =========================================================
# SCRAPER
# =========================================================
class MerolaganiScraper:
    def __init__(self, storage: DataStorage):
        self.client = MerolaganiClient()
        self.storage = storage
        self.output_dir = storage.output_dir

    def get_company_page(self, symbol: str) -> Tuple[str, BeautifulSoup]:
        url = f"{BASE_MEROLAGANI}/CompanyDetail.aspx?symbol={symbol}"
        resp = self.client.get(url)
        return resp.text, BeautifulSoup(resp.text, "html.parser")

    def make_base_post_data(self, symbol: str, soup: BeautifulSoup) -> Dict[str, str]:
        fields = {}
        for inp in soup.select("input[type='hidden']"):
            if inp.get("name"):
                fields[inp.get("name")] = inp.get("value", "")

        data = {
            "symbol": symbol.lower(),
            "ctl00$ASCompany$hdnAutoSuggest": "0",
            "ctl00$ASCompany$txtAutoSuggest": "",
            "ctl00$txtNews": "",
            "ctl00$ContentPlaceHolder1$CompanyDetail1$hdnStockSymbol": symbol.upper(),
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__ASYNCPOST": "true",
        }
        data.update(fields)
        return data

    def postback_company_detail(self, symbol: str, trigger_control: str, active_tab_id: str, extra_data: Optional[Dict[str, str]] = None) -> str:
        url = f"{BASE_MEROLAGANI}/CompanyDetail.aspx?symbol={symbol}"
        html, soup = self.get_company_page(symbol)
        data = self.make_base_post_data(symbol, soup)

        data["ctl00$ScriptManager1"] = f"ctl00$ContentPlaceHolder1$CompanyDetail1$tabPanel|{trigger_control}"
        data["ctl00$ContentPlaceHolder1$CompanyDetail1$hdnActiveTabID"] = active_tab_id
        if extra_data:
            data.update(extra_data)

        headers = {
            "X-MicrosoftAjax": "Delta=true",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Referer": url,
        }
        resp = self.client.post(url, data=data, headers=headers)
        return resp.text

    def fetch_about_company(self, symbol: str) -> pd.DataFrame:
        txt = self.postback_company_detail(symbol, "ctl00$ContentPlaceHolder1$CompanyDetail1$btnAboutTab", "#divAbout", 
            {"__EVENTTARGET": "ctl00$ContentPlaceHolder1$CompanyDetail1$btnAboutTab"})
        tables = try_read_html_tables(txt)
        if not tables: return pd.DataFrame()
        
        df = max(tables, key=len).copy()
        if df.shape[1] >= 2:
            df = df.iloc[:, :2].copy()
            df.columns = ["field_name", "field_value"]
        else: return pd.DataFrame()
        
        df["field_name"] = df["field_name"].astype(str).map(clean_text)
        df["field_value"] = df["field_value"].astype(str).map(clean_text)
        df = df[(df["field_name"] != "") & (~df["field_name"].str.contains("updatepanel|viewstate|eventvalidation|scriptmanager", case=False, na=False))].copy()

        if df.empty: return pd.DataFrame()

        row = {"symbol": symbol}
        for _, r in df.iterrows():
            key = normalize_sql_column(r["field_name"])
            if key and key != "symbol":
                row[key] = r["field_value"]

        return pd.DataFrame([row])

    def fetch_all_dividend(self, symbol: str) -> pd.DataFrame:
        frames = []
        for page in range(1, (MAX_DIVIDEND_PAGES_OVERRIDE or 50) + 1):
            trigger = "ctl00$ContentPlaceHolder1$CompanyDetail1$btnDividendTab" if page == 1 else "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControl5$btnPaging"
            extra = {
                "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControl5$hdnPCID": "PC5",
                "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControl5$hdnCurrentPage": str(page if page > 1 else 0),
                "__EVENTTARGET": "" if page > 1 else "ctl00$ContentPlaceHolder1$CompanyDetail1$btnDividendTab",
            }
            try:
                txt = self.postback_company_detail(symbol, trigger, "#divDividend", extra)
                tables = try_read_html_tables(txt)
                if not tables: break
                df = max(tables, key=len).copy()
                df.columns = dedupe_columns([str(c) for c in df.columns])
                df["symbol"] = symbol
                df["page_number"] = page
                frames.append(df)
                if len(df) < 10: break
            except Exception as e:
                logger.warning(f"Dividend page {page} failed for {symbol}: {e}")
                break
        return pd.concat(frames, ignore_index=True).drop_duplicates() if frames else pd.DataFrame()

    def fetch_all_news(self, symbol: str) -> pd.DataFrame:
        frames = []
        for page in range(1, (MAX_NEWS_PAGES_OVERRIDE or 50) + 1):
            trigger = "ctl00$ContentPlaceHolder1$CompanyDetail1$btnNewsTab" if page == 1 else "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlNews1$btnPaging"
            extra = {
                "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlNews1$hdnPCID": "PC1",
                "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlNews1$hdnCurrentPage": str(page if page > 1 else 0),
                "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlNews2$hdnPCID": "PC2",
                "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlNews2$hdnCurrentPage": "0",
                "__EVENTTARGET": "" if page > 1 else "ctl00$ContentPlaceHolder1$CompanyDetail1$btnNewsTab",
            }
            try:
                txt = self.postback_company_detail(symbol, trigger, "#divNews", extra)
                tables = try_read_html_tables(txt)
                if not tables: break
                df = max(tables, key=len).copy()
                df.columns = dedupe_columns([str(c) for c in df.columns])
                df["symbol"] = symbol
                df["page_number"] = page
                frames.append(df)
                if len(df) < 10: break
            except Exception as e:
                logger.warning(f"News page {page} failed for {symbol}: {e}")
                break
        return pd.concat(frames, ignore_index=True).drop_duplicates() if frames else pd.DataFrame()

    def fetch_floorsheet_for_date(self, symbol: str, market_date_filter: str, page_number: int = 1) -> pd.DataFrame:
        trigger = "ctl00$ContentPlaceHolder1$CompanyDetail1$lbtnSearchFloorsheet" if page_number == 1 else "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlFloorsheet1$btnPaging"
        extra = {
            "ctl00$ContentPlaceHolder1$CompanyDetail1$txtFloorsheetDateFilter": market_date_filter,
            "ctl00$ContentPlaceHolder1$CompanyDetail1$txtBuyerFilter": "",
            "ctl00$ContentPlaceHolder1$CompanyDetail1$txtSellerFilter": "",
            "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlFloorsheet1$hdnPCID": "PC1",
            "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlFloorsheet1$hdnCurrentPage": str(page_number if page_number > 1 else 0),
            "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlFloorsheet2$hdnPCID": "PC2",
            "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlFloorsheet2$hdnCurrentPage": "0",
            "__EVENTTARGET": trigger,
        }
        txt = self.postback_company_detail(symbol, trigger, "#divFloorsheet", extra)
        tables = try_read_html_tables(txt)
        if not tables: return pd.DataFrame()
        
        df = max(tables, key=len).copy()
        if df.empty: return df

        df.columns = [normalize_sql_column(str(c)) for c in df.columns]
        rename_map = {}
        for c in df.columns:
            lc = c.lower()
            if "row" in lc and "no" in lc: rename_map[c] = "row_no"
            elif "transaction" in lc: rename_map[c] = "transaction_no"
            elif "buyer" in lc: rename_map[c] = "buyer_broker"
            elif "seller" in lc: rename_map[c] = "seller_broker"
            elif "quantity" in lc: rename_map[c] = "quantity"
            elif lc == "rate": rename_map[c] = "rate"
            elif "amount" in lc: rename_map[c] = "amount"
            elif lc == "date": rename_map[c] = "date"
        
        df = df.rename(columns=rename_map)
        expected = ["row_no", "date", "transaction_no", "buyer_broker", "seller_broker", "quantity", "rate", "amount"]
        for col in expected:
            if col not in df.columns: df[col] = None

        df = df[expected].copy()
        df["symbol"] = symbol
        
        # VERY IMPORTANT FIX: Ensure numeric types for DB compatibility with nepse.db format
        for num_col in ['buyer_broker', 'seller_broker', 'quantity', 'rate', 'amount']:
            df[num_col] = pd.to_numeric(df[num_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
            
        return df

    def fetch_all_floorsheet_pages(self, symbol: str, market_date_filter: str) -> pd.DataFrame:
        frames = []
        for page in range(1, (MAX_FLOORSHEET_PAGES_OVERRIDE or 50) + 1):
            try:
                df = self.fetch_floorsheet_for_date(symbol, market_date_filter, page_number=page)
                if df.empty: break
                frames.append(df)
                if len(df) < 100: break
            except Exception as e:
                logger.warning(f"Floorsheet failed {symbol} @ {market_date_filter}: {e}")
                break
        return pd.concat(frames, ignore_index=True).drop_duplicates() if frames else pd.DataFrame()


def get_all_active_symbols():
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT symbol FROM floor_sheet")
        symbols = [row[0] for row in cursor.fetchall() if row[0]]
        conn.close()
        return sorted(symbols)
    except:
        return []

def run_pipeline():
    logger.info("Initializing Native Merolagani Pipeline Integration")
    storage = DataStorage(DB_PATH, OUTPUT_DIR)
    scraper = MerolaganiScraper(storage)

    try:
        all_symbols = get_all_active_symbols()
        if not all_symbols:
            logger.warning("No symbols found in db. Falling back to active text.")
            all_symbols = extract_symbols_from_text(ACTIVE_SYMBOLS_TEXT)

        # 1. ALWAYS persist the FULL instrument mapping to SQLite
        instruments_df = pd.DataFrame([{"symbol": s, "label": s, "source": "nepse.db"} for s in all_symbols])
        storage.save_df_to_sqlite(instruments_df, "instruments", if_exists="replace", add_fetched_at=False)
        storage.save_df_to_sqlite(instruments_df, "companies", if_exists="replace", add_fetched_at=False)
        logger.info(f"Saved {len(all_symbols)} instruments to company index")

        process_symbols = all_symbols[:LIMIT_SYMBOLS] if LIMIT_SYMBOLS else all_symbols
        logger.info(f"Processing details for {len(process_symbols)} symbols...")

        for symbol in process_symbols:
            # 2. ABOUT INFO (cleans up junk as per request logic)
            try:
                about_df = scraper.fetch_about_company(symbol)
                if not about_df.empty:
                    storage.save_df_to_sqlite(about_df, "about_company", if_exists="append")
                    logger.info(f"Saved about info for {symbol}")
            except Exception as e:
                logger.error(f"About failed for {symbol}: {e}")
            
            # 3. NEWS 
            try:
                news_df = scraper.fetch_all_news(symbol)
                if not news_df.empty:
                    storage.save_df_to_sqlite(news_df, "news", if_exists="append")
                    logger.info(f"Saved {len(news_df)} news for {symbol}")
            except Exception as e:
                logger.error(f"News failed for {symbol}: {e}")

            # 4. DIVIDEND
            try:
                div_df = scraper.fetch_all_dividend(symbol)
                if not div_df.empty:
                    storage.save_df_to_sqlite(div_df, "dividend", if_exists="append")
                    logger.info(f"Saved {len(div_df)} dividend for {symbol}")
            except Exception as e:
                logger.error(f"Dividend failed for {symbol}: {e}")
                
            # 5. FLOORSHEET
            try:
                start_dt = pd.to_datetime(FLOOR_START_DATE)
                end_dt = pd.to_datetime(FLOOR_END_DATE)
                for dt in daterange(start_dt, end_dt):
                    dt_str = to_mmddyyyy(dt)
                    df_floor = scraper.fetch_all_floorsheet_pages(symbol, dt_str)
                    if not df_floor.empty:
                        # Append directly into standard nepse floor_sheet!
                        storage.save_df_to_sqlite(df_floor, "floor_sheet", if_exists="append")
                        logger.info(f"Saved {len(df_floor)} floorsheet rows for {symbol} on {dt_str}")
            except Exception as e:
                logger.error(f"Floorsheet failed for {symbol}: {e}")

        # Post-process the databases
        storage.create_indexes()
        # Compute central accumulation summaries right after updating table
        compute_broker_summary()
        compute_daily_price()
        
        logger.info("Pipeline Complete.")

    finally:
        storage.close()

if __name__ == "__main__":
    run_pipeline()
