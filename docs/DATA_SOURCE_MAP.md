# NEPSE Data Source Map

## Purpose

This document answers a simple operational question:

**As of now, what data in this project is fetched from which website?**

It reflects the current implemented source paths in the backend codebase at `C:\nepse-algo`.

## High-Level Summary

The project currently fetches data from these external sources:

- [ShareHub Nepal](https://sharehubnepal.com/)
- [Chukul](https://chukul.com/)
- [MeroLagani](https://merolagani.com/)
- [ShareSansar](https://www.sharesansar.com/) as a limited helper/fallback base in the fetcher
- [CDSC MeroShare backend](https://backend.cdsc.com.np/api)

Each source serves a different role:

- ShareHub: primary price-history feed, latest daily-price feed, and index history feed
- Chukul: fallback market-wide floorsheet-by-date feed
- MeroLagani: company metadata, fallback price history, floorsheet scraping, news/dividends style scraping pipeline
- CDSC MeroShare: user account / holdings / portfolio integration

## Source-by-Feature Map

### 1. Company List / Company Metadata

**Primary source:**

- [MeroLagani Company List](https://merolagani.com/CompanyList.aspx)

Used for:

- company list
- symbol list
- company names
- sectors

Implemented in:

- `C:\nepse-algo\src\data\fetcher.py`
- `fetch_company_list()`

Relevant code constants:

- `BASE_URL = "https://merolagani.com"`
- `COMPANY_LIST_URL = "https://merolagani.com/CompanyList.aspx"`

### 2. Price History

**Primary source:**

- [ShareHub price history API](https://sharehubnepal.com/data/api/v1/price-history)

Used for:

- `price_history`
- latest symbol-by-symbol OHLCV refresh

Implemented in:

- `C:\nepse-algo\src\data\fetcher.py`
- `_fetch_price_history_from_sharehub(...)`
- `fetch_price_history(...)`

ShareHub is currently the preferred source for modern incremental price history.

**Fallback source 1:**

- MeroLagani company history tab on `CompanyDetail.aspx`

Implemented in:

- `_fetch_price_history_from_merolagani_history_tab(...)`

**Fallback source 2:**

- MeroLagani technical chart handler

Endpoint family:

- `https://www.merolagani.com/handlers/TechnicalChartHandler.ashx`

Used as an additional fallback when ShareHub data is unavailable.

### 3. Market Summary / Index Summary

**Primary source:**

- [ShareHub index history API](https://sharehubnepal.com/data/api/v1/index/date-wise-data?indexId=1&size=500)

Used for:

- `market_summary`
- `sector_index`

Implemented in:

- `C:\nepse-algo\src\data\fetcher.py`
- `fetch_market_summary()`

Current status:

- this is the preferred source for NEPSE index and sector/sub-index history

**Fallback sources:**

1. Chukul market summary API
2. MeroLagani market summary page
3. MeroLagani technical chart/index handler

### 4. Floorsheet Data

The project currently uses more than one route depending on the workflow.

#### 4A. Daily floorsheet updater

Current implemented path:

1. ShareHub by-date floorsheet API first
2. Chukul by-date floorsheet fallback
3. MeroLagani fallback if needed

Implemented in:

- `C:\nepse-algo\src\data\floorsheet_pipeline.py`
- `run_daily_update(...)`

Daily path:

1. try [ShareHub floorsheet API](https://sharehubnepal.com/live/api/v2/floorsheet?Size=100&currentPage=1&date=2026-3-29)
2. if needed, try Chukul by-date floorsheet API
3. if no data or error, fall back to MeroLagani market-wide scrape

#### 4B. Historical floorsheet backfill

**Primary implemented source in pipeline:**

- ShareHub floorsheet API

Used for:

- historical full-market floorsheet backfills
- `floor_sheet`
- downstream `broker_summary`
- downstream `daily_price`

Implemented in:

- `C:\nepse-algo\src\data\floorsheet_pipeline.py`
- `fetch_by_date_via_sharehub(...)`

Fallback:

1. Chukul by-date floorsheet API
2. [MeroLagani Floorsheet page](https://merolagani.com/Floorsheet.aspx)

#### 4C. Symbol-specific floorsheet helper

**Source:**

- ShareHub by-date floorsheet API for latest market date

Used for:

- per-symbol current floorsheet fetch helper

Implemented in:

- `C:\nepse-algo\src\data\fetcher.py`
- `fetch_floor_sheet_for(symbol)`

### 5. Broker Summary

**External source:**

- not fetched directly from a separate website as a finished table

Instead:

- `broker_summary` is derived from raw `floor_sheet`

So its true upstream source is:

- daily/historical floorsheet source
- now primarily ShareHub by-date feed, with Chukul and MeroLagani fallback

Computed in:

- `C:\nepse-algo\src\data\floorsheet_pipeline.py`

### 6. Daily Price

**External source:**

- latest day is fetched from ShareHub live daily-price API

For the latest day:

- [ShareHub todays-price API](https://sharehubnepal.com/live/api/v2/nepselive/todays-price)

For older dates:

- `daily_price` still falls back to local floor-sheet aggregation

Computed in:

- `C:\nepse-algo\src\data\floorsheet_pipeline.py`

### 7. News / Dividend / Company Detail Scraping

**Source family:**

- MeroLagani company detail pages

Implemented in:

- `C:\nepse-algo\src\data\merolagani_pipeline.py`

This pipeline is explicitly described as scraping:

- company metadata
- news
- dividends
- OHLCV
- floorsheet

Even if not every sub-flow is the current primary incremental updater, MeroLagani remains the implemented website for those company-detail scraping workflows.

### 8. MeroShare Account / Holdings / Portfolio Import

**Primary source:**

- [CDSC backend API](https://backend.cdsc.com.np/api)

Used for:

- MeroShare login
- depository participant list
- own detail
- linked accounts
- portfolio holdings
- transaction history
- portfolio import into local PMS tables

Implemented in:

- `C:\nepse-algo\src\integrations\meroshare\client.py`
- `C:\nepse-algo\src\integrations\meroshare\service.py`

Important endpoints used in code include:

- `meroShare/auth/`
- `meroShare/capital/`
- `meroShare/ownDetail/`
- `meroShare/dependentCustomer/...`
- `meroShareView/myPortfolio/`
- `meroShareView/myTransaction/`

Origin used by the connector:

- `https://meroshare.cdsc.com.np`

API base used by the connector:

- `https://backend.cdsc.com.np/api`

## Practical Table-to-Source Mapping

This is the easiest operational view.

| Table / Feature | Current Main Source | Notes |
|---|---|---|
| `companies` / company list | MeroLagani | `CompanyList.aspx` |
| `about_company` | MeroLagani | company detail scraping |
| `price_history` | ShareHub | MeroLagani used as fallback |
| `market_summary` | ShareHub index history | Chukul and MeroLagani remain fallbacks |
| `sector_index` | ShareHub index history | preferred index/sub-index feed |
| `floor_sheet` | ShareHub by-date API | Chukul and MeroLagani are fallbacks |
| `broker_summary` | Derived from `floor_sheet` | not direct from website |
| `daily_price` | ShareHub todays-price for latest date | older dates use floor-sheet-derived fallback |
| `signals` | Derived locally | built from DB tables |
| `nepse_signals` | Derived locally | built from DB tables |
| derived analytics tables | Derived locally | scanners, factors, support/resistance, ML features |
| MeroShare accounts/holdings | CDSC backend | official MeroShare backend integration |

## Important Distinction: Raw vs Derived Data

Several major tables in this repo are **not fetched directly from a website**. They are derived locally from raw upstream data.

Examples:

- `broker_summary`
- `daily_price`
- `signals`
- `nepse_signals`
- `stock_factor_snapshot`
- `sector_factor_snapshot`
- `support_resistance_levels`
- `scanner_results_daily`
- `ml_feature_snapshot`
- `ml_predictions_daily`

So when asking “which website does this come from,” the real answer is often:

- fetched raw from a source like ShareHub / Chukul / MeroLagani
- then transformed and stored locally in SQLite

## Current Source Priority Order

### Price history priority

1. ShareHub
2. MeroLagani history tab
3. MeroLagani technical chart fallback

### Market summary priority

1. ShareHub index history
2. Chukul
3. MeroLagani fallbacks

### Floorsheet priority

1. ShareHub by-date API
2. Chukul by-date API
3. MeroLagani fallback

### Portfolio / account data priority

1. CDSC backend via MeroShare integration

## Files To Check When This Changes

If you want to audit this again later, the first files to inspect are:

- `C:\nepse-algo\src\data\fetcher.py`
- `C:\nepse-algo\src\data\floorsheet_pipeline.py`
- `C:\nepse-algo\src\data\merolagani_pipeline.py`
- `C:\nepse-algo\src\integrations\meroshare\client.py`

## Bottom Line

As of now, the project mainly fetches:

- **price history from ShareHub**
- **latest daily-price and index history from ShareHub**
- **market-wide floorsheet from ShareHub**
- **company metadata, fallback price history, and fallback floorsheet scraping from MeroLagani**
- **user holdings/portfolio data from CDSC MeroShare backend**

Everything else important is mostly derived locally from those upstream datasets inside `nepse.db`.
