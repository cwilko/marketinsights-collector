# Data Sources & Database Schema

## Overview
This document provides comprehensive information about all data sources and database schema used in the economic dashboard project. Each section includes API details, collection methods, and corresponding database table structures.

## US Economic Indicators

### Consumer Price Index (CPI)
- **Source**: U.S. Bureau of Labor Statistics (BLS)
- **API**: BLS Public Data API
- **Endpoint**: `https://api.bls.gov/publicAPI/v2/timeseries/data/`
- **Series ID**: `CUUR0000SA0` (All items in U.S. city average)
- **API Key**: Required for more than 25 queries/day
- **Rate Limit**: 500 queries/day with key, 25 without
- **Update Frequency**: Monthly (around 10th of month)
- **Database Table**: `consumer_price_index`
- **SQL Schema**: `/dags/econometrics-pipeline/config/create_tables.sql`
- **Key Columns**: `date`, `value`, `month_over_month_change`, `year_over_year_change`
- **Collector Function**: `collect_cpi()`

### Federal Funds Rate
- **Source**: Federal Reserve Bank of St. Louis (FRED)
- **API**: FRED API
- **Endpoint**: `https://api.stlouisfed.org/fred/series/observations`
- **Series ID**: `FEDFUNDS` (Effective Federal Funds Rate)
- **API Key**: Required (free)
- **Rate Limit**: 120 requests/minute
- **Update Frequency**: Monthly/Daily
- **Database Tables**: 
  - `federal_funds_rate` (monthly)
  - `daily_federal_funds_rate` (daily)
- **SQL Schema**: `/dags/econometrics-pipeline/config/create_tables.sql`
- **Key Columns**: `date`, `effective_rate`, `target_rate_lower`, `target_rate_upper`
- **Collector Functions**: `collect_fed_funds_rate()`, `collect_fed_funds_daily()`

### Unemployment Rate
- **Source**: U.S. Bureau of Labor Statistics (BLS)
- **API**: BLS Public Data API
- **Series ID**: `LNS14000000` (Unemployment Rate)
- **Update Frequency**: Monthly (first Friday of month)
- **Database Table**: `unemployment_rate`
- **SQL Schema**: `/dags/econometrics-pipeline/config/create_tables.sql`
- **Key Columns**: `date`, `rate`, `labor_force`, `employed`, `unemployed`
- **Collector Function**: `collect_unemployment_rate()`

### Gross Domestic Product (GDP)
- **Source**: U.S. Bureau of Economic Analysis (BEA)
- **API**: BEA Data API
- **Endpoint**: `https://apps.bea.gov/api/data`
- **Dataset**: NIPA (National Income and Product Accounts)
- **API Key**: Required (free)
- **Update Frequency**: Quarterly
- **Database Table**: `gross_domestic_product`
- **SQL Schema**: `/dags/econometrics-pipeline/config/create_tables.sql`
- **Key Columns**: `quarter`, `gdp_billions`, `gdp_growth_rate`, `gdp_per_capita`
- **Collector Function**: `collect_gdp()`

## UK Economic Indicators

### UK Consumer Price Index (CPIH)
- **Source**: Office for National Statistics (ONS) Beta API
- **API**: ONS Beta API v1
- **Endpoint**: `https://api.beta.ons.gov.uk/v1/datasets/cpih01/editions/time-series/versions/{version}/observations`
- **Dataset ID**: `cpih01`
- **Dimensions**: 
  - `geography=K02000001` (United Kingdom)
  - `aggregate=CP00` (Overall Index)
- **API Key**: Not required (public API)
- **Rate Limit**: Not specified (reasonable use)
- **Update Frequency**: Monthly
- **Records**: ~307 observations
- **Database Table**: `uk_consumer_price_index`
- **SQL Schema**: `/dags/econometrics-pipeline/config/create_tables.sql`
- **Key Columns**: `date`, `value`, `month_over_month_change`, `year_over_year_change`
- **Collector Function**: `collect_uk_cpi()`

### UK Unemployment Rate
- **Source**: Office for National Statistics (ONS) Beta API
- **API**: ONS Beta API v1
- **Endpoint**: `https://api.beta.ons.gov.uk/v1/datasets/labour-market/editions/time-series/versions/{version}/observations`
- **Dataset ID**: `labour-market`
- **Dimensions**:
  - `geography=K02000001` (United Kingdom)
  - `economicactivity=unemployed`
  - `unitofmeasure=rates`
  - `seasonaladjustment=seasonal-adjustment`
  - `sex=all-adults`
  - `agegroups=16+`
- **Update Frequency**: Quarterly
- **Records**: ~108 observations
- **Database Table**: `uk_unemployment_rate`
- **SQL Schema**: `/dags/uk-metrics-pipeline/config/create_tables.sql`
- **Key Columns**: `date`, `unemployment_rate`, `labor_force_thousands`, `unemployed_thousands`
- **Collector Function**: `collect_uk_unemployment()`

### UK Gross Domestic Product (Monthly)
- **Source**: Office for National Statistics (ONS) Beta API
- **API**: ONS Beta API v1
- **Endpoint**: `https://api.beta.ons.gov.uk/v1/datasets/gdp-to-four-decimal-places/editions/time-series/versions/{version}/observations`
- **Dataset ID**: `gdp-to-four-decimal-places`
- **Dimensions**:
  - `geography=K02000001` (United Kingdom)
  - `unofficialstandardindustrialclassification` (All 5 sector classifications):
    - `A--T` (A-T : Monthly GDP)
    - `A` (A : Agriculture)
    - `B-E` (B-E : Production Industries)
    - `F` (F : Construction)
    - `G-T` (G-T : Index of Services)
- **Update Frequency**: Monthly
- **Records**: ~1,530 observations (306 per sector × 5 sectors)
- **Database Table**: `uk_gross_domestic_product`
- **SQL Schema**: `/dags/econometrics-pipeline/config/create_tables.sql`
- **Key Columns**: `date`, `sector_classification`, `gdp_index`
- **Collector Function**: `collect_uk_gdp()`

### UK Bank Rate
- **Source**: Bank of England Interactive Database (IADB)
- **API**: CSV Download API
- **Endpoint**: `http://www.bankofengland.co.uk/boeapps/iadb/fromshowcolumns.asp?csv.x=yes`
- **Series Code**: `IUMABEDR` (Monthly Average Bank Rate)
- **Parameters**:
  - `Datefrom=DD/MMM/YYYY` (start date)
  - `Dateto=DD/MMM/YYYY` (end date)
  - `SeriesCodes=IUMABEDR`
  - `CSVF=TN` (tabular, no titles)
  - `UsingCodes=Y`
  - `VPD=Y`
- **API Key**: Not required
- **Update Frequency**: Monthly
- **Records**: ~427 observations
- **Database Tables**: 
  - `uk_monthly_bank_rate` (monthly)
  - `uk_daily_bank_rate` (daily)
- **SQL Schema**: `/dags/uk-metrics-pipeline/config/create_tables.sql`
- **Key Columns**: `date`, `rate`, `change_from_previous`, `monthly_average`
- **Collector Functions**: `collect_uk_monthly_bank_rate()`, `collect_uk_daily_bank_rate()`

### UK BoE Yield Curves (Comprehensive)
- **Source**: Bank of England Statistics - Yield Curves ZIP Downloads
- **API**: ZIP File Downloads
- **Base URL**: `https://www.bankofengland.co.uk/-/media/boe/files/statistics/yield-curves`
- **Coverage**: 80+ maturities from 0.5Y to 50Y+
- **Yield Types**: 4 comprehensive types
  - **Nominal**: Standard government bond yields
  - **Real**: Inflation-indexed bond yields (RPI-linked)  
  - **Inflation**: Implied inflation expectations
  - **OIS**: Overnight Index Swap rates
- **Update Frequency**: Daily (business days)
- **Records**: ~80,000+ observations per year
- **Database Table**: `boe_yield_curves`
- **SQL Schema**: `/dags/uk-metrics-pipeline/config/create_tables.sql`
- **Key Columns**: `date`, `maturity_years`, `yield_rate`, `yield_type`, `data_source`
- **Collector Function**: `collect_boe_yield_curves()`

## US Market Data

### S&P 500 Index
- **Source**: Federal Reserve Bank of St. Louis (FRED)
- **API**: FRED API
- **Endpoint**: `https://api.stlouisfed.org/fred/series/observations`
- **Series ID**: `SP500` (S&P 500)
- **Alternative**: Alpha Vantage, Yahoo Finance API
- **API Key**: Required (FRED API Key)
- **Rate Limit**: 120 requests/minute
- **Update Frequency**: Daily (market days)
- **Database Table**: `sp500_index`
- **SQL Schema**: `/dags/econometrics-pipeline/config/create_tables.sql`
- **Key Columns**: `date`, `closing_price`, `daily_change`, `daily_change_percent`
- **Collector Function**: `collect_sp500()`

### VIX (Volatility Index)
- **Source**: Federal Reserve Bank of St. Louis (FRED)
- **API**: FRED API
- **Endpoint**: `https://api.stlouisfed.org/fred/series/observations`
- **Series ID**: `VIXCLS` (CBOE Volatility Index: VIX)
- **Alternative**: Alpha Vantage
- **API Key**: Required (FRED API Key)
- **Rate Limit**: 120 requests/minute
- **Update Frequency**: Daily (market days)
- **Database Table**: `vix_index`
- **SQL Schema**: `/dags/econometrics-pipeline/config/create_tables.sql`
- **Key Columns**: `date`, `vix_close`, `daily_change`, `volatility_level`
- **Collector Function**: `collect_vix()`

### Treasury Yield Curve
- **Source**: U.S. Department of Treasury
- **API**: Treasury Data API
- **Endpoint**: `https://api.fiscaldata.treasury.gov/services/api/v1/accounting/od/daily_treasury_yield_curve`
- **API Key**: Not required
- **Rate Limit**: No explicit limit, reasonable use
- **Update Frequency**: Daily (business days)
- **Database Table**: `treasury_yields`
- **SQL Schema**: `/dags/econometrics-pipeline/config/create_tables.sql`
- **Key Columns**: `date`, `maturity`, `yield_rate`, `term_structure_type`
- **Collector Function**: `collect_treasury_yields()`

### P/E Ratios
- **Source**: Multiple sources needed
- **S&P 500 P/E**: Multpl.com API or scraping
- **Shiller P/E**: Yale/Robert Shiller data (manual download or scraping)
- **API Key**: Not required
- **Update Frequency**: Daily for S&P P/E, Monthly for Shiller P/E
- **Database Table**: `pe_ratios`
- **SQL Schema**: `/dags/econometrics-pipeline/config/create_tables.sql`
- **Key Columns**: `date`, `sp500_pe`, `shiller_pe`, `earnings_yield`, `market_cap`
- **Collector Function**: `collect_pe_ratios()`

## UK Market Data

### FTSE 100 Index
- **Source**: MarketWatch (Primary for incremental updates)
- **API**: CSV Download API
- **Endpoint**: `https://www.marketwatch.com/investing/index/ukx/downloaddatapartial`
- **Parameters**: `startdate=MM/DD/YYYY&enddate=MM/DD/YYYY&countrycode=UK&frequency=1&csvdownload=true`
- **Limitation**: ⚠️ **Maximum 1 year of data per request**
- **Alternative Sources**:
  - Historical CSV files (manual loading for 2001-2025 data)
  - Yahoo Finance API
  - Alpha Vantage API
- **API Key**: Not required
- **Update Frequency**: Daily (market days)
- **Records**: 6,230+ daily records (2001-2025)
- **Gap Handling**: For gaps > 1 year, requires manual CSV import or sequential API calls
- **Database Table**: `ftse_100_index`
- **SQL Schema**: `/dags/uk-metrics-pipeline/config/create_tables.sql`
- **Key Columns**: `date`, `opening_price`, `high_price`, `low_price`, `closing_price`, `volume`
- **Collector Function**: `collect_ftse_100()`

### FTSE 100 Data Sources Detail
- **Historical Data**: Complete dataset 2001-2025 loaded via CSV files
- **Incremental Updates**: MarketWatch API for recent data (≤ 365 days)
- **Production Usage**: Daily/weekly automated collection works fine
- **Recovery Scenarios**: Large gaps require manual CSV data loading

## German Market Data

### German Bund Yields (Comprehensive)
- **Source**: Deutsche Bundesbank StatisticDownload API
- **API**: Statistical Download Service
- **Base URL**: `https://api.statistiken.bundesbank.de/rest/download`
- **Dataset**: BBK01.WZ9770 (Government bond yields)
- **Coverage**: 1-30 year maturities, daily data from 1997 to present
- **Maturities**: 16 complete maturities (1Y, 2Y, 3Y, 4Y, 5Y, 6Y, 7Y, 8Y, 9Y, 10Y, 15Y, 20Y, 25Y, 30Y)
- **Data Format**: CSV download with historical time series
- **API Key**: Not required
- **Rate Limit**: No explicit limit, reasonable use
- **Update Frequency**: Daily (business days)
- **Records**: 200,000+ observations (28 years × 16 maturities × 250 business days/year)
- **Database Table**: `german_bund_yields`
- **SQL Schema**: `/dags/econometrics-pipeline/config/create_tables.sql`
- **Key Columns**: `date`, `maturity_years`, `yield_rate`, `data_source`, `instrument_type`
- **Collector Function**: `collect_german_bund_yields()`

### German Bund Implementation Details
- **Data Quality**: Official central bank data with complete historical coverage
- **Integration**: Seamless cross-market analysis with UK and US yield data
- **Performance**: Single API call retrieves complete historical dataset
- **Reliability**: Deutsche Bundesbank official statistics, no authentication required

## ETF Data Sources

### ETF NAV and Price Data
- **Source**: iShares ETF Provider Excel Downloads
- **Coverage**: UK Gilt ETFs for arbitrage analysis
- **Primary ETFs**:
  - **IGLT**: iShares Core UK Gilts UCITS ETF (Broad gilt exposure)
  - **INXG**: iShares UK Index-Linked Gilts UCITS ETF (Inflation protection)
- **Data Format**: Excel (.xls) files with multiple worksheets
- **API Key**: Not required
- **Update Frequency**: Daily after market close
- **Database Tables**: 
  - `etf_nav_history` (historical NAV data)
  - `etf_price_history` (market price data)
- **SQL Schema**: `/dags/etf-pipeline/config/etf_tables.sql`
- **Key Columns**: `date`, `etf_ticker`, `nav`, `market_price`, `currency`, `data_source`
- **Collector Functions**: `collect_etf_nav_history()`, `collect_etf_price_history()`

### ETF Arbitrage Analysis
- **Purpose**: Support Gilt Market Analysis Guide arbitrage strategies
- **Premium/Discount Calculation**: `(ETF_Price - NAV) / NAV`
- **Signals**: BUY (discount >0.5%), SELL (premium >0.3%), HOLD (±0.3-0.5%)
- **Integration**: Compare ETF yields vs BoE official curves for arbitrage opportunities
- **Database Storage**: Real-time arbitrage opportunity identification and historical tracking

## API Keys Required

### US Data Sources
- **BLS API Key** (optional but recommended) - Bureau of Labor Statistics
- **FRED API Key** (required) - Federal Reserve Economic Data
- **BEA API Key** (required) - Bureau of Economic Analysis
- **Alpha Vantage API Key** (optional, for backup) - Market data backup

### UK Data Sources
- **No API keys required** - All UK data sources are public APIs:
  - ONS Beta API (no authentication)
  - Bank of England IADB (no authentication)
  - MarketWatch CSV download (no authentication)

## Rate Limiting Strategy

### US APIs
- **FRED**: 120 requests/minute
- **BLS**: 500 requests/day with key, 25 without
- **BEA**: No explicit limit, reasonable use
- **Treasury**: No explicit limit, reasonable use

### UK APIs
- **ONS Beta API**: No explicit limit, reasonable use
- **Bank of England IADB**: No explicit limit, reasonable use
- **MarketWatch**: No explicit limit, but 1-year data limit per request

### General Strategy
- Implement exponential backoff for all APIs
- Cache responses when possible (5-minute TTL in dashboard)
- Batch requests where supported (ONS Beta API supports multiple series)
- Monitor usage to avoid limits
- Use safe-by-default collectors (database_url=None for testing)

## Data Coverage Summary

### US Economic & Market Data
- **Total Series**: 9 (Fed Funds, Unemployment, CPI, GDP, S&P 500, VIX, Treasury Yields, P/E Ratios, Daily Fed Funds)
- **Historical Coverage**: Varies by source (1950s+ for most indicators)
- **Update Frequency**: Daily (market data) to Quarterly (GDP)

### UK Economic & Market Data
- **Total Series**: 6 (CPI, Unemployment, GDP, Bank Rate, Gilt Yields, FTSE 100)
- **Historical Coverage**: 
  - Economic: ~20+ years (varies by ONS dataset)
  - FTSE 100: Complete 2001-2025 (24+ years)
  - Gilt Yields: 5+ years of daily data
- **Update Frequency**: Daily (market data, gilt yields) to Quarterly (unemployment)
- **Total Records**: ~11,000+ observations across all UK datasets