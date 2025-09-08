# Economic & Market Data Metrics

This document provides a comprehensive overview of all metrics collected by the econometrics data pipeline, including their sources, update frequencies, and historical data coverage.

## 🇺🇸 US Economic Indicators

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

**US Economic & Market Data:**
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

**UK Economic & Market Data:**
| Metric | Frequency | Typical Release Time | Delay |
|--------|-----------|---------------------|-------|
| FTSE 100 | Daily | End of trading day, ~4:30 PM GMT | Same day |
| BoE Yield Curves | Daily | Next business day | 1 day |
| UK Bank Rate | Monthly | Next month for previous month | ~30 days |
| UK CPI (CPIH) | Monthly | Mid-month for previous month | ~15 days |
| UK GDP (Monthly) | Monthly | ~40 days after month end | ~40 days |
| UK Unemployment | Quarterly | ~45 days after quarter end | ~45 days |
| GBP/USD | Daily | End of trading day | Same day |

### Incremental Collection Strategy

**All Markets (US & UK):**
- **Empty Database**: Collects full historical period (see coverage above)
- **Existing Data**: Only fetches records since last stored date
- **Daily Check**: Pipeline runs daily, automatically skips if data is current
- **Bulk Fetching**: Uses API date ranges to minimize API calls
- **Multi-region**: Parallel collection of US and UK data sources

## 📊 Data Volume Estimates

### Annual Data Points (Approximate)

**US Market Data:**
- **Daily Metrics**: ~250 records/year (business days)
  - S&P 500, VIX: 250 records/year each
  - Treasury Yields: 250 × 10 maturities = 2,500 records/year
  - Daily Fed Funds: 250 records/year
- **Monthly Metrics**: ~12 records/year
  - CPI, Unemployment, Monthly Fed Funds: 12 records/year each
- **Quarterly Metrics**: ~4 records/year
  - GDP: 4 records/year

**UK Market Data:**
- **Daily Metrics**: ~250 records/year (business days)
  - FTSE 100: 250 records/year
  - BoE Yield Curves: 250 × 80+ maturities × 4 types = 80,000+ records/year
  - GBP/USD: 250 records/year
- **Monthly Metrics**: ~12 records/year
  - UK CPI, UK GDP, UK Bank Rate: 12 records/year each
- **Quarterly Metrics**: ~4 records/year
  - UK Unemployment: 4 records/year

### Total Annual Volume: ~15,000 new records per year (US + UK combined)

**US Market Data**: ~4,300 records/year
**UK Market Data**: ~11,000 records/year (includes comprehensive BoE yield curves)

## 🔧 API Specifications

### Rate Limits & Authentication

**US Data Sources:**
| API | Rate Limit | Authentication | Cost |
|-----|------------|----------------|------|
| FRED | 120 calls/minute | API Key (required) | Free |
| BLS | 500 calls/day (with key), 25/day (without) | API Key (optional) | Free |
| BEA | 1,000 calls/day | API Key (required) | Free |
| multpl.com | No published limit | None (web scraping) | Free |

**UK Data Sources:**
| API | Rate Limit | Authentication | Cost |
|-----|------------|----------------|------|
| ONS Beta API | No published limit | None | Free |
| Bank of England IADB | No published limit | None | Free |
| Bank of England ZIP files | No published limit | None | Free |
| MarketWatch | No published limit | None (web scraping) | Free |
| Alpha Vantage (GBP/USD) | 5 calls/minute (free tier) | API Key (required) | Free tier available |

### Bulk Fetching Capabilities

**US APIs:**
- **FRED**: ✅ Date ranges (`observation_start`/`observation_end`)
- **BLS**: ✅ Multi-year requests (up to 20 years with API key)
- **BEA**: ✅ "ALL" years parameter for full history
- **multpl.com**: ❌ Current values only (no historical API)

**UK APIs:**
- **ONS Beta API**: ✅ Full historical data with `time=*` parameter
- **Bank of England IADB**: ✅ Date ranges (`Datefrom`/`Dateto`)
- **Bank of England ZIP**: ✅ Complete historical and latest data files
- **MarketWatch**: ✅ Up to 1 year per request (sequential calls for longer periods)
- **Alpha Vantage**: ✅ Full historical data (with API key)

## 🎯 Collection Efficiency

The pipeline is optimized for minimal API usage through:
- **Incremental updates**: Only new data since last collection
- **Bulk requests**: Single API calls for date ranges instead of individual requests
- **Smart defaults**: Historical periods matched to data update frequency
- **Rate limiting**: Automatic delays to respect API limits
- **Error handling**: Graceful failures with retry logic

## 🇬🇧 UK Economic Indicators

| Metric | Function | Source | Series/Dataset | Frequency | Update Schedule | History | Database Table |
|--------|----------|--------|----------------|-----------|----------------|---------|----------------|
| **Consumer Price Index (CPIH)** | `collect_uk_cpi()` | ONS Beta API | `cpih01` | Monthly | Mid-month for previous month | 25+ years | `uk_consumer_price_index` |
| **Unemployment Rate** | `collect_uk_unemployment()` | ONS Beta API | `labour-market` | Quarterly | Variable (quarterly) | 9+ years | `uk_unemployment_rate` |
| **GDP (Monthly Index)** | `collect_uk_gdp()` | ONS Beta API | `gdp-to-four-decimal-places` | Monthly | ~40 days after month end | 25+ years | `uk_gross_domestic_product` |
| **Bank Rate** | `collect_uk_monthly_bank_rate()` | Bank of England IADB | `IUMABEDR` | Monthly | Next month for previous month | 35+ years | `uk_monthly_bank_rate` |
| **BoE Yield Curves** | `collect_boe_yield_curves()` | Bank of England ZIP files | Comprehensive yield data | Daily | Next business day | 10+ years | `boe_yield_curves` |

### Key Fields by Metric
- **UK CPI**: `date`, `value`, `year_over_year_change`
- **UK Unemployment**: `date`, `rate`
- **UK GDP**: `date`, `gdp_index`
- **UK Bank Rate**: `date`, `rate`
- **BoE Yield Curves**: `date`, `maturity_years`, `yield_rate`, `yield_type`

### BoE Yield Curve Coverage
**Comprehensive maturity coverage (80+ points):**
- **Short-term**: 0.5Y, 1Y, 1.5Y, 2Y, 2.5Y, 3Y
- **Medium-term**: 4Y, 5Y, 6Y, 7Y, 8Y, 9Y, 10Y, 12Y, 15Y
- **Long-term**: 20Y, 25Y, 30Y, 40Y, 50Y+

**Four yield types collected:**
- **Nominal**: Standard government bond yields
- **Real**: Inflation-indexed bond yields
- **Inflation**: Implied inflation expectations
- **OIS**: Overnight Index Swap rates

## 🇬🇧 UK Market Data

| Metric | Function | Source | Series/Dataset | Frequency | Update Schedule | History | Database Table |
|--------|----------|--------|----------------|-----------|----------------|---------|----------------|
| **FTSE 100 Index** | `collect_ftse_100()` | MarketWatch + Historical CSV | `UKX` | Daily | End of trading day | 24+ years (2001-present) | `ftse_100_index` |
| **GBP/USD Exchange Rate** | `collect_gbp_usd()` | Alpha Vantage (requires API key) | `FX_DAILY` | Daily | End of trading day | 20+ years | `gbp_usd_exchange_rate` |

### Key Fields by Metric
- **FTSE 100**: `date`, `open_price`, `high_price`, `low_price`, `close_price`, `volume`
- **GBP/USD**: `date`, `exchange_rate`

This design ensures efficient, reliable collection of comprehensive US and UK economic and market data while respecting API provider constraints.