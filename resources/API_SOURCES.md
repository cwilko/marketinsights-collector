# Data Sources & Database Schema

Reference for every data source this project collects from, organized by
provider/API rather than by pipeline — provider is the signal that matters when
deciding whether a new data point should reuse an existing integration rather than
introduce a new one. If a provider below already covers something close to what
you're adding, prefer it over a new provider, all else equal.

See `CANDIDATE_SOURCES.md` in this same folder for sources proposed but not yet
implemented.

## FRED (Federal Reserve Economic Data)

- **Endpoint**: `https://api.stlouisfed.org/fred/series/observations`
- **API key**: required (free) — [fred.stlouisfed.org/docs/api/api_key.html](https://fred.stlouisfed.org/docs/api/api_key.html)
- **Rate limit**: 120 requests/minute

| Metric | Series ID | Update frequency | DB table | Collector |
|---|---|---|---|---|
| Federal Funds Rate | `FEDFUNDS` | Monthly/Daily | `federal_funds_rate`, `daily_federal_funds_rate` | `collect_fed_funds_rate()`, `collect_fed_funds_daily()` |
| S&P 500 | `SP500` | Daily (market days) | `sp500_index` | `collect_sp500()` |
| VIX | `VIXCLS` | Daily (market days) | `vix_index` | `collect_vix()` |

## BLS (Bureau of Labor Statistics)

- **Endpoint**: `https://api.bls.gov/publicAPI/v2/timeseries/data/`
- **API key**: optional — required above 25 queries/day, 500/day with key
- **Rate limit**: 25/day without key, 500/day with key

| Metric | Series ID | Update frequency | DB table | Collector |
|---|---|---|---|---|
| CPI | `CUUR0000SA0` | Monthly (~10th) | `consumer_price_index` | `collect_cpi()` |
| Unemployment Rate | `LNS14000000` | Monthly (1st Friday) | `unemployment_rate` | `collect_unemployment_rate()` |

## BEA (Bureau of Economic Analysis)

- **Endpoint**: `https://apps.bea.gov/api/data` (NIPA dataset)
- **API key**: required (free)
- **Rate limit**: not specified, reasonable use

| Metric | Series ID | Update frequency | DB table | Collector |
|---|---|---|---|---|
| GDP | NIPA | Quarterly | `gross_domestic_product` | `collect_gdp()` |

## US Department of the Treasury

- **Endpoint**: `https://api.fiscaldata.treasury.gov/services/api/v1/accounting/od/daily_treasury_yield_curve`
- **API key**: not required
- **Rate limit**: not specified, reasonable use

| Metric | Update frequency | DB table | Collector |
|---|---|---|---|
| Treasury yield curve | Daily (business days) | `treasury_yields` | `collect_treasury_yields()` |

## ONS Beta API (UK Office for National Statistics)

- **Endpoint**: `https://api.beta.ons.gov.uk/v1/datasets/{dataset_id}/editions/time-series/versions/{version}/observations` (version auto-discovered via `links.latest_version`)
- **API key**: not required (public API)
- **Rate limit**: not specified, reasonable use

| Metric | Dataset ID | Update frequency | DB table | Collector |
|---|---|---|---|---|
| UK CPI (CPIH) | `cpih01` | Monthly | `uk_consumer_price_index` | `collect_uk_cpi()` |
| UK Unemployment Rate | `labour-market` | Quarterly | `uk_unemployment_rate` | `collect_uk_unemployment()` |
| UK GDP (monthly, 5 sector classifications) | `gdp-to-four-decimal-places` | Monthly | `uk_gross_domestic_product` | `collect_uk_gdp()` |
| UK GDP sector weights (A-T) | ONS GDP Source Catalogue Excel files | — | — | `UKGDPSectorWeightsCollector` |

## Bank of England IADB (Interactive Database)

- **Endpoint**: `http://www.bankofengland.co.uk/boeapps/iadb/fromshowcolumns.asp?csv.x=yes` (CSV download)
- **API key**: not required
- **Rate limit**: not specified, reasonable use

| Metric | Series code | Update frequency | DB table | Collector |
|---|---|---|---|---|
| UK Bank Rate (monthly avg) | `IUMABEDR` | Monthly | `uk_monthly_bank_rate` | `collect_uk_monthly_bank_rate()` |
| UK Bank Rate (daily) | `IUDBEDR` | Daily | `uk_daily_bank_rate` | `collect_uk_daily_bank_rate()` |

## Bank of England yield curve ZIP downloads

- **Endpoint**: `https://www.bankofengland.co.uk/-/media/boe/files/statistics/yield-curves` (ZIP of Excel files)
- **API key**: not required
- **Rate limit**: not specified, reasonable use

| Metric | Coverage | Update frequency | DB table | Collector |
|---|---|---|---|---|
| BoE comprehensive yield curves | 80+ maturities (0.5Y-50Y+), 4 types (nominal/real/inflation/OIS) | Daily (business days) | `boe_yield_curves` | `collect_boe_yield_curves()` |

## Bundesbank StatisticDownload API

- **Endpoint**: `https://api.statistiken.bundesbank.de/rest/download`
- **API key**: not required
- **Rate limit**: not specified, reasonable use

| Metric | Dataset | Coverage | DB table | Collector |
|---|---|---|---|---|
| German Bund yields | `BBK01.WZ9770` | 1-30Y maturities, daily since 1997 | `german_bund_yields` | `collect_german_bund_yields()` |

## MarketWatch CSV download

- **Endpoint**: `https://www.marketwatch.com/investing/index/ukx/downloaddatapartial`
- **API key**: not required
- **Limitation**: max 1 year of data per request — gaps beyond that need manual CSV import or sequential calls

| Metric | Coverage | DB table | Collector |
|---|---|---|---|
| FTSE 100 index | 2001-2025 (historical CSV) + incremental daily | `ftse_100_index` | `collect_ftse_100()` |

## iShares ETF Excel downloads

- **Endpoint**: `https://www.ishares.com/uk/individual/en/products/{product_id}/.../{ajax_id}.ajax?fileType=xls`
- **API key**: not required
- **Format**: XLS, multiple worksheets (Holdings, Historical, Performance, Distributions)

| Metric | Tickers | Update frequency | DB tables | Collector |
|---|---|---|---|---|
| UK gilt ETF NAV/price | IGLT, INXG (VGOV, GLTY via other mechanisms — see Selenium below) | Daily after market close | `etf_nav_history`, `etf_current_prices`, `etf_premium_discount`, `etf_holdings`, `etf_metadata` | `collect_ishares_etf_nav()` |

## Broker scraping (Selenium, Chrome)

No official API — scrapes broker websites directly.

| Source | URL | Coverage | Collector |
|---|---|---|---|
| Hargreaves Lansdown | `hl.co.uk/shares/corporate-bonds-gilts/bond-prices/uk-gilts` | 60-80 UK gilts, clean+dirty price, YTM, after-tax YTM | `collect_gilt_market_prices()` |
| AJ Bell | `ajbell.co.uk/investment/bonds/gilts/prices` | 25+ UK gilts, clean price only, delayed ~15min, ISIN | `collect_ajbell_gilt_prices()` |

**Notes**: requires `scipy`, `selenium`, `webdriver-manager` (optional deps, `pip install -e .[gilt_market]`). 7s page-load delay for rate-limiting; Chrome headless, auto-installed driver.

## barchart.com

No official API — scraping.

| Metric | DB table | Pipeline |
|---|---|---|
| Options data | — | `options` (smart-home-config DAG) |

## Property listing site scraping

No official API — scraping (New Forest area listings).

| Metric | Pipeline |
|---|---|
| UK housing listings | `housemarket-etl` (smart-home-config DAG) |

## Not yet traced to a specific provider

- **US TIPS, P/E ratios**: multiple sources per project history (Multpl.com, Shiller
  data) — manual or scraping, not confirmed to a single API here.
- **UK swap rates** (`uk_swap_rates` module, `collect_uk_swap_rates()`): provider not
  confirmed from this document.
