# Economic & Market Data Metrics

This document provides a comprehensive overview of all metrics collected by the econometrics data pipeline, including their sources, update frequencies, and historical data coverage.

## 📊 Economic Indicators

| Metric | Function | Source | Series/Dataset | Frequency | Update Schedule | History | Database Table |
|--------|----------|--------|----------------|-----------|----------------|---------|----------------|
| **Daily Fed Funds Rate** | `collect_daily_fed_funds_rate()` | Federal Reserve (FRED) | `DFF` | Daily | Next business day | 2 years | `daily_federal_funds_rate` |
| **Treasury Yields** | `collect_fred_treasury_yields()` | Federal Reserve (FRED) | 10 series (DGS1MO-DGS30) | Daily | End of business day | 5 years | `fred_treasury_yields` |
| **Consumer Price Index** | `collect_cpi()` | Bureau of Labor Statistics (BLS) | `CUUR0000SA0` | Monthly | Mid-month for previous month | 10 years | `consumer_price_index` |
| **Federal Funds Rate** | `collect_fed_funds_rate()` | Federal Reserve (FRED) | `FEDFUNDS` | Monthly | 1st business day of next month | 10 years | `federal_funds_rate` |
| **Unemployment Rate** | `collect_unemployment()` | Bureau of Labor Statistics (BLS) | `LNS14000000` | Monthly | 1st Friday of next month | 10 years | `unemployment_rate` |
| **GDP** | `collect_gdp()` | Bureau of Economic Analysis (BEA) | NIPA T10101 | Quarterly | ~30 days after quarter end | All years | `gross_domestic_product` |

### Key Fields by Metric
- **Daily Fed Funds**: `date`, `effective_rate`
- **Treasury Yields**: `date`, `series_id`, `maturity`, `yield_rate`
- **CPI**: `date`, `value`, `year_over_year_change`
- **Fed Funds**: `date`, `effective_rate`, `target_rate_lower/upper`
- **Unemployment**: `date`, `rate`, `labor_force`, `employed`, `unemployed`
- **GDP**: `quarter`, `gdp_billions`, `gdp_growth_rate`, `gdp_per_capita`

### Treasury Yield Maturities
**10 different yield curve points collected:**
- **Short-term**: 1M, 3M, 6M
- **Medium-term**: 1Y, 2Y, 5Y, 7Y  
- **Long-term**: 10Y, 20Y, 30Y

## 📈 Market Data

| Metric | Function | Source | Series/Dataset | Frequency | Update Schedule | History | Database Table |
|--------|----------|--------|----------------|-----------|----------------|---------|----------------|
| **S&P 500 Index** | `collect_sp500()` | Federal Reserve (FRED) | `SP500` | Daily | End of trading day | 10 years | `sp500_index` |
| **VIX Volatility** | `collect_vix()` | Federal Reserve (FRED) | `VIXCLS` | Daily | End of trading day | 10 years | `vix_index` |
| **P/E Ratios** | `collect_pe_ratios()` | multpl.com | Web scraping | Daily | Variable (scraping dependent) | Daily snapshots | `pe_ratios` |

### Key Fields by Metric
- **S&P 500**: `date`, `close_price`, `open_price`, `high_price`, `low_price`, `volume`
- **VIX**: `date`, `close_price`, `open_price`, `high_price`, `low_price`
- **P/E Ratios**: `date`, `sp500_pe`, `sp500_shiller_pe`

## 🔄 Update Patterns & Scheduling

### Data Release Schedule
| Metric | Frequency | Typical Release Time | Delay |
|--------|-----------|---------------------|-------|
| Daily Fed Funds Rate | Daily | Next business day, 4:30 PM ET | 1 day |
| S&P 500 | Daily | End of trading day, ~4:00 PM ET | Same day |
| VIX | Daily | End of trading day, ~4:15 PM ET | Same day |
| Treasury Yields | Daily | ~6:00 PM ET | Same day |
| P/E Ratios | Daily | Variable (web scraping) | Same day |
| Fed Funds Rate (Monthly) | Monthly | 1st business day of next month | ~30 days |
| CPI | Monthly | Mid-month for previous month | ~15 days |
| Unemployment | Monthly | 1st Friday of next month | ~5 days |
| GDP | Quarterly | ~30 days after quarter end | ~90 days |

### Incremental Collection Strategy
- **Empty Database**: Collects full historical period (see coverage above)
- **Existing Data**: Only fetches records since last stored date
- **Daily Check**: Pipeline runs daily, automatically skips if data is current
- **Bulk Fetching**: Uses API date ranges to minimize API calls

## 📊 Data Volume Estimates

### Annual Data Points (Approximate)
- **Daily Metrics**: ~250 records/year (business days)
  - S&P 500, VIX: 250 records/year each
  - Treasury Yields: 250 × 13 maturities = 3,250 records/year
  - Daily Fed Funds: 250 records/year
- **Monthly Metrics**: ~12 records/year
  - CPI, Unemployment, Monthly Fed Funds: 12 records/year each
- **Quarterly Metrics**: ~4 records/year
  - GDP: 4 records/year

### Total Annual Volume: ~4,300 new records per year

## 🔧 API Specifications

### Rate Limits & Authentication
| API | Rate Limit | Authentication | Cost |
|-----|------------|----------------|------|
| FRED | 120 calls/minute | API Key (required) | Free |
| BLS | 500 calls/day (with key), 25/day (without) | API Key (optional) | Free |
| BEA | 1,000 calls/day | API Key (required) | Free |
| FRED (Treasury) | 120 calls/minute | API Key (required) | Free |
| multpl.com | No published limit | None (web scraping) | Free |

### Bulk Fetching Capabilities
- **FRED**: ✅ Date ranges (`observation_start`/`observation_end`)
- **BLS**: ✅ Multi-year requests (up to 20 years with API key)
- **BEA**: ✅ "ALL" years parameter for full history
- **FRED (Treasury)**: ✅ Date ranges (`observation_start`/`observation_end`) for each series
- **multpl.com**: ❌ Current values only (no historical API)

## 🎯 Collection Efficiency

The pipeline is optimized for minimal API usage through:
- **Incremental updates**: Only new data since last collection
- **Bulk requests**: Single API calls for date ranges instead of individual requests
- **Smart defaults**: Historical periods matched to data update frequency
- **Rate limiting**: Automatic delays to respect API limits
- **Error handling**: Graceful failures with retry logic

This design ensures efficient, reliable collection of comprehensive economic and market data while respecting API provider constraints.