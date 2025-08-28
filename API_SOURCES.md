# Data Source APIs

## Economic Indicators

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

## Market Data

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

## API Keys Required
- BLS API Key (optional but recommended)
- FRED API Key (required)
- BEA API Key (required)
- Alpha Vantage API Key (optional, for backup)

## Rate Limiting Strategy
- Implement exponential backoff
- Cache responses when possible
- Batch requests where supported
- Monitor usage to avoid limits