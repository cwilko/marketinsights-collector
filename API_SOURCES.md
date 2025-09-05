# Data Source APIs

## US Economic Indicators

### Consumer Price Index (CPI)
- **Source**: U.S. Bureau of Labor Statistics (BLS)
- **API**: BLS Public Data API
- **Endpoint**: `https://api.bls.gov/publicAPI/v2/timeseries/data/`
- **Series ID**: `CUUR0000SA0` (All items in U.S. city average)
- **API Key**: Required for more than 25 queries/day
- **Rate Limit**: 500 queries/day with key, 25 without
- **Update Frequency**: Monthly (around 10th of month)

### Federal Funds Rate
- **Source**: Federal Reserve Bank of St. Louis (FRED)
- **API**: FRED API
- **Endpoint**: `https://api.stlouisfed.org/fred/series/observations`
- **Series ID**: `FEDFUNDS` (Effective Federal Funds Rate)
- **API Key**: Required (free)
- **Rate Limit**: 120 requests/minute
- **Update Frequency**: Monthly

### Unemployment Rate
- **Source**: U.S. Bureau of Labor Statistics (BLS)
- **API**: BLS Public Data API
- **Series ID**: `LNS14000000` (Unemployment Rate)
- **Update Frequency**: Monthly (first Friday of month)

### Gross Domestic Product (GDP)
- **Source**: U.S. Bureau of Economic Analysis (BEA)
- **API**: BEA Data API
- **Endpoint**: `https://apps.bea.gov/api/data`
- **Dataset**: NIPA (National Income and Product Accounts)
- **API Key**: Required (free)
- **Update Frequency**: Quarterly

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

### UK Gross Domestic Product (Monthly)
- **Source**: Office for National Statistics (ONS) Beta API
- **API**: ONS Beta API v1
- **Endpoint**: `https://api.beta.ons.gov.uk/v1/datasets/gdp-to-four-decimal-places/editions/time-series/versions/{version}/observations`
- **Dataset ID**: `gdp-to-four-decimal-places`
- **Dimensions**:
  - `geography=K02000001` (United Kingdom)
  - `unofficialstandardindustrialclassification=A--T` (A-T : Monthly GDP)
- **Update Frequency**: Monthly
- **Records**: ~306 observations

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

### UK Gilt Yields (Government Bond Yields)
- **Source**: Bank of England Interactive Database (IADB)
- **API**: CSV Download API
- **Endpoint**: `http://www.bankofengland.co.uk/boeapps/iadb/fromshowcolumns.asp?csv.x=yes`
- **Series Codes**:
  - `IUDSNPY` (5 Year Gilt Yield - daily)
  - `IUDMNPY` (10 Year Gilt Yield - daily)
  - `IUDLNPY` (20 Year Gilt Yield - daily)
- **Parameters**: Same as UK Bank Rate but with multiple series codes
- **Update Frequency**: Daily (business days)
- **Records**: ~3,777 observations (5 years × 3 maturities)

## US Market Data

### S&P 500 Index
- **Source**: Federal Reserve Bank of St. Louis (FRED)
- **API**: FRED API
- **Series ID**: `SP500` (S&P 500)
- **Alternative**: Alpha Vantage, Yahoo Finance API
- **Update Frequency**: Daily (market days)

### VIX (Volatility Index)
- **Source**: Federal Reserve Bank of St. Louis (FRED)
- **API**: FRED API
- **Series ID**: `VIXCLS` (CBOE Volatility Index: VIX)
- **Alternative**: Alpha Vantage
- **Update Frequency**: Daily (market days)

### Treasury Yield Curve
- **Source**: U.S. Department of Treasury
- **API**: Treasury Data API
- **Endpoint**: `https://api.fiscaldata.treasury.gov/services/api/v1/accounting/od/daily_treasury_yield_curve`
- **Update Frequency**: Daily (business days)

### P/E Ratios
- **Source**: Multiple sources needed
- **S&P 500 P/E**: Multpl.com API or scraping
- **Shiller P/E**: Yale/Robert Shiller data (manual download or scraping)
- **Update Frequency**: Daily for S&P P/E, Monthly for Shiller P/E

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

### FTSE 100 Data Sources Detail
- **Historical Data**: Complete dataset 2001-2025 loaded via CSV files
- **Incremental Updates**: MarketWatch API for recent data (≤ 365 days)
- **Production Usage**: Daily/weekly automated collection works fine
- **Recovery Scenarios**: Large gaps require manual CSV data loading

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