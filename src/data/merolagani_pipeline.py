# src/data/merolagani_pipeline.py
# Scrapes Merolagani for company metadata, news, dividends, OHLCV, and floorsheet.

import os
import re
import io
import json
import time
import math
import argparse
import sqlite3
import logging
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup

try:
    from active_symbols import load_active_symbols
except ImportError:
    try:
        from data.active_symbols import load_active_symbols
    except ImportError:
        from src.data.active_symbols import load_active_symbols

try:
    from floorsheet_pipeline import get_db, ensure_schema, compute_broker_summary, compute_daily_price, normalize_trade_date, recompute_broker_summary_for_symbol_dates
except ImportError:
    try:
        from data.floorsheet_pipeline import get_db, ensure_schema, compute_broker_summary, compute_daily_price, normalize_trade_date, recompute_broker_summary_for_symbol_dates
    except ImportError:
        from src.data.floorsheet_pipeline import get_db, ensure_schema, compute_broker_summary, compute_daily_price, normalize_trade_date, recompute_broker_summary_for_symbol_dates

try:
    from fetcher import create_tables as create_market_tables, fetch_price_history
except ImportError:
    try:
        from data.fetcher import create_tables as create_market_tables, fetch_price_history
    except ImportError:
        from src.data.fetcher import create_tables as create_market_tables, fetch_price_history

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
MAX_FLOORSHEET_PAGES_OVERRIDE = None # None = auto
MAX_NEWS_PAGES_OVERRIDE = None        # None = auto
MAX_DIVIDEND_PAGES_OVERRIDE = None    # None = auto

# ---------- DATES ----------
# Practical full-history defaults for this project.
# Use Unix epoch start so the scraper keeps everything available.
OHLCV_START_DATE = "1970-01-01"
OHLCV_END_DATE = datetime.now().strftime("%Y-%m-%d")

# Floorsheet data does not appear to exist as far back as OHLCV, so keep the
# default full backfill bounded to the earliest practical market-wide range.
FLOOR_START_DATE = "2020-01-01"
FLOOR_END_DATE = datetime.now().strftime("%Y-%m-%d")

# ---------- NETWORK ----------
REQUEST_TIMEOUT = 40
SLEEP_BETWEEN_REQUESTS = 0.75
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
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


def extract_aspx_updatepanel_fragments(text: str) -> List[str]:
    fragments = []
    pattern = re.compile(r"\|\d+\|(\d+)\|updatePanel\|([^|]+)\|")
    pos = 0

    while True:
        match = pattern.search(text, pos)
        if not match:
            break

        length = int(match.group(1))
        start = match.end()
        fragment = text[start:start + length]
        if fragment:
            fragments.append(fragment)
        pos = start + length

    return fragments


def extract_aspx_hidden_fields(text: str) -> Dict[str, str]:
    fields: Dict[str, str] = {}
    for name, value in re.findall(r"\|hiddenField\|([^|]+)\|([^|]*)", text):
        fields[name] = value
    return fields

def try_read_html_tables(text: str) -> List[pd.DataFrame]:
    dfs = []
    try:
        dfs.extend(pd.read_html(io.StringIO(text)))
    except Exception:
        pass
    dfs.extend(parse_aspx_delta_tables(text))
    for fragment in extract_aspx_updatepanel_fragments(text):
        try:
            dfs.extend(pd.read_html(io.StringIO(fragment)))
        except Exception:
            pass
        dfs.extend(parse_aspx_delta_tables(fragment))
    return dfs


def get_soup_from_response(text: str) -> BeautifulSoup:
    fragments = extract_aspx_updatepanel_fragments(text)
    html = fragments[0] if fragments else text
    return BeautifulSoup(html, "html.parser")


def html_table_to_dataframe(table, single_column_name: str = "value") -> pd.DataFrame:
    header = None
    rows = []

    for tr in table.select("tr"):
        ths = [clean_text(cell.get_text(" ", strip=True)) for cell in tr.select("th")]
        tds = [clean_text(cell.get_text(" ", strip=True)) for cell in tr.select("td")]
        cells = ths + tds if ths and not tds else [clean_text(cell.get_text(" ", strip=True)) for cell in tr.select("th,td")]
        cells = [cell for cell in cells if cell != ""]
        if not cells:
            continue

        if header is None and tr.select("th"):
            header = dedupe_columns(cells)
            continue

        rows.append(cells)

    if not rows:
        return pd.DataFrame()

    if header is None and all(len(row) == 1 for row in rows):
        return pd.DataFrame([{single_column_name: row[0]} for row in rows if row[0]])

    width = max(len(row) for row in rows)
    if header is None:
        header = [f"col_{i + 1}" for i in range(width)]
    elif len(header) < width:
        header = header + [f"col_{i + 1}" for i in range(len(header), width)]

    normalized_rows = []
    for row in rows:
        if len(row) < len(header):
            row = row + [None] * (len(header) - len(row))
        normalized_rows.append(row[:len(header)])

    return pd.DataFrame(normalized_rows, columns=header)


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
        self.failed_dates = {}  # Track symbol -> list of (date, error) tuples

    def get_company_page(self, symbol: str) -> Tuple[str, BeautifulSoup]:
        url = f"{BASE_MEROLAGANI}/CompanyDetail.aspx?symbol={symbol}"
        resp = self.client.get(url)
        return resp.text, BeautifulSoup(resp.text, "html.parser")

    def extract_hidden_fields(self, soup: BeautifulSoup) -> Dict[str, str]:
        fields = {}
        for inp in soup.select("input[type='hidden']"):
            name = inp.get("name")
            value = inp.get("value", "")
            if name:
                fields[name] = value
        return fields

    def make_base_post_data(self, symbol: str, soup: BeautifulSoup) -> Dict[str, str]:
        fields = self.extract_hidden_fields(soup)

        data = {
            "symbol": symbol.lower(),
            "ctl00$ASCompany$hdnAutoSuggest": "0",
            "ctl00$ASCompany$txtAutoSuggest": "",
            "ctl00$txtNews": "",
            "ctl00$AutoSuggest1$hdnAutoSuggest": "0",
            "ctl00$AutoSuggest1$txtAutoSuggest": "",
            "ctl00$ContentPlaceHolder1$CompanyDetail1$hdnStockSymbol": symbol.upper(),
            "ctl00$ContentPlaceHolder1$CompanyDetail1$StockGraph1$hdnStockSymbol": symbol.upper(),
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

    def _postback_company_detail_with_state(
        self,
        symbol: str,
        state: Dict[str, str],
        trigger_control: str,
        active_tab_id: str,
        extra_data: Optional[Dict[str, str]] = None,
    ) -> str:
        """POST against CompanyDetail while preserving ASP.NET state across requests."""
        url = f"{BASE_MEROLAGANI}/CompanyDetail.aspx?symbol={symbol}"

        data = {
            "symbol": symbol.lower(),
            "ctl00$ASCompany$hdnAutoSuggest": "0",
            "ctl00$ASCompany$txtAutoSuggest": "",
            "ctl00$txtNews": "",
            "ctl00$AutoSuggest1$hdnAutoSuggest": "0",
            "ctl00$AutoSuggest1$txtAutoSuggest": "",
            "ctl00$ContentPlaceHolder1$CompanyDetail1$hdnStockSymbol": symbol.upper(),
            "ctl00$ContentPlaceHolder1$CompanyDetail1$StockGraph1$hdnStockSymbol": symbol.upper(),
            "ctl00$ContentPlaceHolder1$CompanyDetail1$hdnActiveTabID": active_tab_id,
            "ctl00$ScriptManager1": f"ctl00$ContentPlaceHolder1$CompanyDetail1$tabPanel|{trigger_control}",
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__ASYNCPOST": "true",
        }
        data.update(state)
        if extra_data:
            data.update(extra_data)

        headers = {
            "X-MicrosoftAjax": "Delta=true",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Referer": url,
        }
        resp = self.client.post(url, data=data, headers=headers)
        text = resp.text

        # Keep viewstate/eventvalidation in sync for the next postback.
        state.update(extract_aspx_hidden_fields(text))
        for fragment in extract_aspx_updatepanel_fragments(text):
            fragment_soup = BeautifulSoup(fragment, "html.parser")
            state.update(self.extract_hidden_fields(fragment_soup))

        return text

    def _parse_floorsheet_response(self, symbol: str, text: str) -> pd.DataFrame:
        soup = get_soup_from_response(text)
        table = soup.select_one("#divFloorsheet table")
        if table is None:
            return pd.DataFrame()

        df = html_table_to_dataframe(table)
        if df.empty:
            return df

        df.columns = [normalize_sql_column(str(c)) for c in df.columns]
        rename_map = {}
        for c in df.columns:
            lc = c.lower()
            if "row" in lc and "no" in lc:
                rename_map[c] = "row_no"
            elif "transaction" in lc:
                rename_map[c] = "transaction_no"
            elif "buyer" in lc:
                rename_map[c] = "buyer_broker"
            elif "seller" in lc:
                rename_map[c] = "seller_broker"
            elif "quantity" in lc:
                rename_map[c] = "quantity"
            elif lc == "rate":
                rename_map[c] = "rate"
            elif "amount" in lc:
                rename_map[c] = "amount"
            elif lc == "date":
                rename_map[c] = "date"

        df = df.rename(columns=rename_map)
        expected = ["row_no", "date", "transaction_no", "buyer_broker", "seller_broker", "quantity", "rate", "amount"]
        for col in expected:
            if col not in df.columns:
                df[col] = None

        df = df[expected].copy()
        df["symbol"] = symbol

        for num_col in ["buyer_broker", "seller_broker", "quantity", "rate", "amount"]:
            df[num_col] = pd.to_numeric(df[num_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)

        return df

    def _extract_total_pages(self, text: str) -> Optional[int]:
        fragments = extract_aspx_updatepanel_fragments(text)
        haystack = "\n".join(fragments) if fragments else text
        match = re.search(r"Total\s+pages\s*:\s*(\d+)", haystack, flags=re.IGNORECASE)
        if not match:
            return None
        try:
            return max(1, int(match.group(1)))
        except Exception:
            return None

    def fetch_about_company(self, symbol: str) -> pd.DataFrame:
        html, soup = self.get_company_page(symbol)
        table = soup.select_one("#divAbout table")
        if table is None:
            state = self.extract_hidden_fields(soup)
            txt = self._postback_company_detail_with_state(
                symbol,
                state,
                "ctl00$ContentPlaceHolder1$CompanyDetail1$btnAboutTab",
                "#divAbout",
                {
                    # About tab behaves as button submit in ASP.NET; sending the
                    # button field is more reliable than relying only on EVENTTARGET.
                    "__EVENTTARGET": "",
                    "__EVENTARGUMENT": "",
                    "ctl00$ContentPlaceHolder1$CompanyDetail1$btnAboutTab": "",
                },
            )
            soup = get_soup_from_response(txt)
            table = soup.select_one("#divAbout table")

        if table is None:
            return pd.DataFrame()

        row = {"symbol": symbol}
        for tr in table.select("tr"):
            key_cell = tr.select_one("th")
            value_cell = tr.select_one("td")
            if key_cell is None or value_cell is None:
                continue

            key = normalize_sql_column(clean_text(key_cell.get_text(" ", strip=True)))
            value = clean_text(value_cell.get_text(" ", strip=True))
            if key and key != "symbol" and value:
                row[key] = value

        if len(row) == 1:
            return pd.DataFrame()

        return pd.DataFrame([row])

    def fetch_news_page(self, symbol: str, page_number: int = 1) -> pd.DataFrame:
        # Backward-compatible convenience wrapper; page retrieval is now fully
        # stateful in fetch_all_news().
        all_pages = self.fetch_all_news(symbol)
        if all_pages.empty:
            return all_pages
        return all_pages[all_pages.get("page_number", 1) == page_number].copy()

    def _parse_news_response(self, symbol: str, text: str) -> pd.DataFrame:
        soup = get_soup_from_response(text)
        table = soup.select_one("#divNews table")
        if table is None:
            return pd.DataFrame()

        df = html_table_to_dataframe(table, single_column_name="title")
        if df.empty:
            return pd.DataFrame()

        df.columns = dedupe_columns([str(c) for c in df.columns])
        df["symbol"] = symbol
        return df

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
                soup = get_soup_from_response(txt)
                table = soup.select_one("#divDividend table")
                if table is None:
                    break

                df = html_table_to_dataframe(table)
                if df.empty:
                    break

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
        try:
            _, soup = self.get_company_page(symbol)
            state = self.extract_hidden_fields(soup)

            common = {
                "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlNews1$hdnPCID": "PC1",
                "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlNews2$hdnPCID": "PC2",
                "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlNews2$hdnCurrentPage": "0",
                "ctl00$ContentPlaceHolder1$CompanyDetail1$txtNews": "",
            }

            # 1) Open news tab (page 1)
            first_text = self._postback_company_detail_with_state(
                symbol,
                state,
                "ctl00$ContentPlaceHolder1$CompanyDetail1$btnNewsTab",
                "#divNews",
                {
                    **common,
                    "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlNews1$hdnCurrentPage": "0",
                    "__EVENTTARGET": "ctl00$ContentPlaceHolder1$CompanyDetail1$btnNewsTab",
                    "ctl00$ContentPlaceHolder1$CompanyDetail1$btnNewsTab": "",
                },
            )

            frames: List[pd.DataFrame] = []
            page1 = self._parse_news_response(symbol, first_text)
            if page1.empty:
                return pd.DataFrame()
            page1["page_number"] = 1
            frames.append(page1)

            total_pages = self._extract_total_pages(first_text)
            max_pages = MAX_NEWS_PAGES_OVERRIDE or total_pages or 250
            if total_pages:
                max_pages = min(max_pages, total_pages)

            seen_signatures = {
                tuple(page1.head(5).astype(str).itertuples(index=False, name=None))
            }
            empty_or_repeat_streak = 0

            # News pagination on CompanyDetail behaves like floorsheet: after
            # initial page, pager uses hdnCurrentPage values 2..N.
            for page_index in range(2, max_pages + 1):
                page_text = self._postback_company_detail_with_state(
                    symbol,
                    state,
                    "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlNews1$btnPaging",
                    "#divNews",
                    {
                        **common,
                        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlNews1$hdnCurrentPage": str(page_index),
                        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlNews1$btnPaging": "",
                        "__EVENTTARGET": "",
                        "__EVENTARGUMENT": "",
                    },
                )

                df = self._parse_news_response(symbol, page_text)
                if df.empty:
                    empty_or_repeat_streak += 1
                    if total_pages or empty_or_repeat_streak >= 2:
                        break
                    continue

                signature = tuple(df.head(5).astype(str).itertuples(index=False, name=None))
                if signature in seen_signatures:
                    empty_or_repeat_streak += 1
                    if total_pages or empty_or_repeat_streak >= 2:
                        break
                    continue

                seen_signatures.add(signature)
                empty_or_repeat_streak = 0
                df["page_number"] = page_index
                frames.append(df)

            if not frames:
                return pd.DataFrame()
            return pd.concat(frames, ignore_index=True).drop_duplicates()
        except Exception as e:
            logger.warning(f"News pagination failed for {symbol}: {e}")
            return pd.DataFrame()

    def fetch_floorsheet_for_date(self, symbol: str, market_date_filter: str, page_number: int = 1) -> pd.DataFrame:
        # Keep backward compatibility for callers that request one specific page.
        all_pages = self.fetch_all_floorsheet_pages(symbol, market_date_filter)
        if all_pages.empty:
            return all_pages
        if page_number <= 1:
            return all_pages[all_pages.get("page_number", 1) == 1].copy()
        return all_pages[all_pages.get("page_number", 1) == page_number].copy()

    def fetch_all_floorsheet_pages(self, symbol: str, market_date_filter: str) -> pd.DataFrame:
        # Retry configuration for transient network errors (DNS, connection timeout)
        max_retries = int(os.getenv("MERO_MAX_RETRIES", "3"))
        retry_delay = 1.0  # Start with 1 second, will double on each retry
        
        for attempt in range(max_retries):
            try:
                _, soup = self.get_company_page(symbol)
                state = self.extract_hidden_fields(soup)

                # 1) Open Floorsheet tab
                self._postback_company_detail_with_state(
                    symbol,
                    state,
                    "ctl00$ContentPlaceHolder1$CompanyDetail1$btnFloorsheetTab",
                    "#divFloorsheet",
                    {
                        "__EVENTTARGET": "ctl00$ContentPlaceHolder1$CompanyDetail1$btnFloorsheetTab",
                        "ctl00$ContentPlaceHolder1$CompanyDetail1$btnFloorsheetTab": "",
                    },
                )

                common = {
                    "ctl00$ContentPlaceHolder1$CompanyDetail1$txtFloorsheetDateFilter": market_date_filter,
                    "ctl00$ContentPlaceHolder1$CompanyDetail1$txtBuyerFilter": "",
                    "ctl00$ContentPlaceHolder1$CompanyDetail1$txtSellerFilter": "",
                    "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlFloorsheet1$hdnPCID": "PC1",
                    "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlFloorsheet2$hdnPCID": "PC2",
                    "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlFloorsheet2$hdnCurrentPage": "0",
                }

                # 2) Search by date (page 1)
                search_text = self._postback_company_detail_with_state(
                    symbol,
                    state,
                    "ctl00$ContentPlaceHolder1$CompanyDetail1$lbtnSearchFloorsheet",
                    "#divFloorsheet",
                    {
                        **common,
                        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlFloorsheet1$hdnCurrentPage": "0",
                        "__EVENTTARGET": "ctl00$ContentPlaceHolder1$CompanyDetail1$lbtnSearchFloorsheet",
                    },
                )

                frames: List[pd.DataFrame] = []
                page1 = self._parse_floorsheet_response(symbol, search_text)
                if page1.empty:
                    return pd.DataFrame()
                page1["page_number"] = 1
                frames.append(page1)

                total_pages = self._extract_total_pages(search_text)
                max_pages = MAX_FLOORSHEET_PAGES_OVERRIDE or total_pages or 250
                if total_pages:
                    max_pages = min(max_pages, total_pages)

                # 3) Page through all available pages for the selected date.
                seen_signatures = {
                    tuple(page1[["date", "transaction_no", "buyer_broker", "seller_broker", "quantity", "rate", "amount"]].head(5).astype(str).itertuples(index=False, name=None))
                }
                empty_or_repeat_streak = 0

                # Merolagani floorsheet pager uses page index 0 for the initial
                # search page; subsequent pages are addressed with indices 2..N.
                for page_index in range(2, max_pages + 1):
                    page_text = self._postback_company_detail_with_state(
                        symbol,
                        state,
                        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlFloorsheet1$btnPaging",
                        "#divFloorsheet",
                        {
                            **common,
                            "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlFloorsheet1$hdnCurrentPage": str(page_index),
                            "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlFloorsheet1$btnPaging": "",
                            "__EVENTTARGET": "",
                            "__EVENTARGUMENT": "",
                        },
                    )

                    df = self._parse_floorsheet_response(symbol, page_text)
                    if df.empty:
                        empty_or_repeat_streak += 1
                        if total_pages or empty_or_repeat_streak >= 2:
                            break
                        continue

                    signature = tuple(df[["date", "transaction_no", "buyer_broker", "seller_broker", "quantity", "rate", "amount"]].head(5).astype(str).itertuples(index=False, name=None))
                    if signature in seen_signatures:
                        empty_or_repeat_streak += 1
                        if total_pages or empty_or_repeat_streak >= 2:
                            break
                        continue

                    seen_signatures.add(signature)
                    empty_or_repeat_streak = 0
                    df["page_number"] = page_index
                    frames.append(df)

                if not frames:
                    return pd.DataFrame()

                return pd.concat(frames, ignore_index=True).drop_duplicates(
                    subset=["symbol", "date", "transaction_no", "buyer_broker", "seller_broker", "quantity", "rate", "amount"]
                )
            except Exception as e:
                error_str = str(e).lower()
                is_transient = any(x in error_str for x in ["nameresolutionerror", "connectionerror", "errno 11001", "failed to resolve", "max retries"])
                
                if is_transient and attempt < max_retries - 1:
                    logger.warning(f"Floorsheet transient error for {symbol} @ {market_date_filter} (attempt {attempt + 1}/{max_retries}): {type(e).__name__}. Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    continue
                
                # Permanent failure or max retries exceeded
                logger.warning(f"Floorsheet failed {symbol} @ {market_date_filter}: {e}")
                if symbol not in self.failed_dates:
                    self.failed_dates[symbol] = []
                self.failed_dates[symbol].append((market_date_filter, str(e)[:100]))
                return pd.DataFrame()


def get_all_active_symbols():
    file_symbols = load_active_symbols()
    if file_symbols:
        return file_symbols

    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT symbol FROM floor_sheet")
        symbols = [row[0] for row in cursor.fetchall() if row[0]]
        conn.close()
        return sorted(symbols)
    except:
        return []


def parse_cli_args():
    parser = argparse.ArgumentParser(description="Run the Merolagani data pipeline")
    parser.add_argument("--full-history", action="store_true", help="Run a full historical backfill through today")
    parser.add_argument("--ohlcv-start", default=None, help="OHLCV start date in YYYY-MM-DD format")
    parser.add_argument("--ohlcv-end", default=OHLCV_END_DATE, help="OHLCV end date in YYYY-MM-DD format")
    parser.add_argument("--floor-start", default=None, help="Floorsheet start date in YYYY-MM-DD format")
    parser.add_argument("--floor-end", default=FLOOR_END_DATE, help="Floorsheet end date in YYYY-MM-DD format")
    parser.add_argument("--limit-symbols", type=int, default=LIMIT_SYMBOLS, help="Limit the number of symbols processed")
    parser.add_argument("--resume-after-symbol", default=None, help="Only process symbols after this symbol (exclusive), e.g. SCB")
    parser.add_argument("--symbols", default=None, help="Comma-separated list of specific symbols to process, e.g. NABIL,NMB")
    parser.add_argument("--broker-only", action="store_true", help="Fetch only floorsheet/broker data; skip OHLCV, about, news, and dividend tabs")
    parser.add_argument("--skip-ohlcv", action="store_true", help="Skip OHLCV fetching")
    parser.add_argument("--skip-floorsheet", action="store_true", help="Skip floorsheet fetching")
    return parser.parse_args()


def resolve_run_dates(args):
    ohlcv_start = args.ohlcv_start or OHLCV_START_DATE
    floor_start = args.floor_start or FLOOR_START_DATE
    if args.full_history:
        ohlcv_start = args.ohlcv_start or OHLCV_START_DATE
        floor_start = args.floor_start or FLOOR_START_DATE
    return {
        "ohlcv_start": ohlcv_start,
        "ohlcv_end": args.ohlcv_end or OHLCV_END_DATE,
        "floor_start": floor_start,
        "floor_end": args.floor_end or FLOOR_END_DATE,
    }


def run_pipeline(args=None):
    if args is None:
        args = parse_cli_args()

    date_config = resolve_run_dates(args)
    logger.info("Initializing Native Merolagani Pipeline Integration")
    logger.info(
        "Run config: full_history=%s ohlcv=%s..%s floorsheet=%s..%s limit_symbols=%s skip_ohlcv=%s skip_floorsheet=%s",
        args.full_history,
        date_config["ohlcv_start"],
        date_config["ohlcv_end"],
        date_config["floor_start"],
        date_config["floor_end"],
        args.limit_symbols,
        args.skip_ohlcv,
        args.skip_floorsheet,
    )

    create_market_tables()
    storage = DataStorage(DB_PATH, OUTPUT_DIR)
    scraper = MerolaganiScraper(storage)

    try:
        all_symbols = get_all_active_symbols()
        if not all_symbols:
            logger.warning("No symbols found in db. Falling back to active text.")
            all_symbols = extract_symbols_from_text(ACTIVE_SYMBOLS_TEXT)

        # 1. Persist full instrument mapping only for broad runs.
        # Sharded runs (broker-only/symbol-filtered) execute many concurrent
        # processes; replacing these global tables in each process can race.
        should_refresh_company_index = not (
            args.broker_only or args.symbols or args.resume_after_symbol or args.limit_symbols
        )
        if should_refresh_company_index:
            instruments_df = pd.DataFrame([{"symbol": s, "label": s, "source": "nepse.db"} for s in all_symbols])
            storage.save_df_to_sqlite(instruments_df, "instruments", if_exists="replace", add_fetched_at=False)
            storage.save_df_to_sqlite(instruments_df, "companies", if_exists="replace", add_fetched_at=False)
            logger.info(f"Saved {len(all_symbols)} instruments to company index")
        else:
            logger.info("Skipping company index refresh for filtered/broker-only run")

        process_symbols = all_symbols
        if args.resume_after_symbol:
            marker = args.resume_after_symbol.strip().upper()
            process_symbols = [s for s in process_symbols if str(s).upper() > marker]
            logger.info("Resume filter active: processing symbols after %s (%d symbols)", marker, len(process_symbols))

        if args.symbols:
            only = {s.strip().upper() for s in args.symbols.split(",")}
            process_symbols = [s for s in process_symbols if str(s).upper() in only]
            logger.info("Symbol filter active: processing %s (%d symbols)", sorted(only), len(process_symbols))

        if args.limit_symbols:
            process_symbols = process_symbols[:args.limit_symbols]

        logger.info(f"Processing details for {len(process_symbols)} symbols...")

        for symbol in process_symbols:
            if not args.skip_ohlcv and not args.broker_only:
                try:
                    ohlcv_df = fetch_price_history(
                        symbol,
                        start_date=date_config["ohlcv_start"],
                        end_date=date_config["ohlcv_end"],
                    )
                    if ohlcv_df is not None and not ohlcv_df.empty:
                        logger.info(f"Saved {len(ohlcv_df)} OHLCV rows for {symbol}")
                except Exception as e:
                    logger.error(f"OHLCV failed for {symbol}: {e}")

            # 2. ABOUT INFO (cleans up junk as per request logic)
            if not args.broker_only:
                try:
                    about_df = scraper.fetch_about_company(symbol)
                    if not about_df.empty:
                        # about_company has symbol as primary key; reruns should
                        # update existing rows, not fail on duplicate inserts.
                        storage.conn.execute("DELETE FROM about_company WHERE symbol = ?", (symbol,))
                        storage.conn.commit()
                        storage.save_df_to_sqlite(about_df, "about_company", if_exists="append")
                        logger.info(f"Saved about info for {symbol}")
                except Exception as e:
                    logger.error(f"About failed for {symbol}: {e}")
            
            # 3. NEWS 
            if not args.broker_only:
                try:
                    news_df = scraper.fetch_all_news(symbol)
                    if not news_df.empty:
                        storage.save_df_to_sqlite(news_df, "news", if_exists="append")
                        logger.info(f"Saved {len(news_df)} news for {symbol}")
                except Exception as e:
                    logger.error(f"News failed for {symbol}: {e}")

            # 4. DIVIDEND
            if not args.broker_only:
                try:
                    div_df = scraper.fetch_all_dividend(symbol)
                    if not div_df.empty:
                        storage.save_df_to_sqlite(div_df, "dividend", if_exists="append")
                        logger.info(f"Saved {len(div_df)} dividend for {symbol}")
                except Exception as e:
                    logger.error(f"Dividend failed for {symbol}: {e}")
                
            # 5. FLOORSHEET
            if not args.skip_floorsheet:
                try:
                    start_dt = pd.to_datetime(date_config["floor_start"])
                    end_dt = pd.to_datetime(date_config["floor_end"])
                    for dt in daterange(start_dt, end_dt):
                        dt_str = to_mmddyyyy(dt)
                        df_floor = scraper.fetch_all_floorsheet_pages(symbol, dt_str)
                        if not df_floor.empty:
                            df_floor = df_floor.copy()
                            df_floor["date"] = df_floor["date"].apply(normalize_trade_date)
                            unique_dates = sorted({d for d in df_floor["date"].dropna().tolist() if d})
                            for trade_date in unique_dates:
                                storage.conn.execute(
                                    "DELETE FROM floor_sheet WHERE symbol = ? AND date = ?",
                                    (symbol, trade_date),
                                )
                            storage.conn.commit()
                            storage.save_df_to_sqlite(df_floor, "floor_sheet", if_exists="append")
                            recompute_broker_summary_for_symbol_dates(symbol, unique_dates)
                            logger.info(f"Saved {len(df_floor)} floorsheet rows for {symbol} on {dt_str} -> {', '.join(unique_dates)}")
                except Exception as e:
                    logger.error(f"Floorsheet failed for {symbol}: {e}")

        # Post-process the databases
        storage.create_indexes()
        # Compute central accumulation summaries right after updating table
        if not args.broker_only:
            compute_broker_summary()
        compute_daily_price()
        
        # Report skipped/failed dates
        if scraper.failed_dates:
            logger.warning("\n" + "="*80)
            logger.warning("INCOMPLETE DATA WARNING: The following dates failed to fetch (skipped):")
            logger.warning("="*80)
            total_failed = 0
            for symbol in sorted(scraper.failed_dates.keys()):
                failed_list = scraper.failed_dates[symbol]
                total_failed += len(failed_list)
                logger.warning(f"  {symbol}: {len(failed_list)} date(s) failed")
                for date_str, error in failed_list[:5]:  # Show first 5 failures per symbol
                    logger.warning(f"    - {date_str}: {error}")
                if len(failed_list) > 5:
                    logger.warning(f"    ... and {len(failed_list) - 5} more")
            logger.warning("="*80)
            logger.warning(f"TOTAL: {total_failed} date(s) across {len(scraper.failed_dates)} symbol(s) failed to fetch.")
            logger.warning("These dates were SKIPPED. Re-run the pipeline to retry these symbols.")
            logger.warning("="*80 + "\n")
        
        logger.info("Pipeline Complete.")

    finally:
        storage.close()

if __name__ == "__main__":
    run_pipeline()
