# Candidate Data Sources

Proposed, not yet implemented — for manual review only. Nothing here gets
automatically turned into a collector; this is a landing place for suggestions
(including from the `data_collector` Airflow agent) pending human review and merge.

See `API_SOURCES.md` in this same folder for what's already live — check there first
to avoid duplicating existing coverage, and to prefer an already-integrated provider
over a new one where one fits.

Use this structure for each entry:

```
### <Source/provider name>

- **API endpoint** (base URL):
- **Series/Dataset ID** (if applicable):
- **API key**: required / not required — where to get one if required
- **Rate limit**:
- **Update frequency**:
- **Proposed database table** + key columns:
- **Notes**: any gotchas — scraping technology needed (e.g. Selenium), known gaps or
  per-request limits, authentication quirks, anything that would trip up a future
  implementation.
```

---

### FRED — US TIPS real yield and breakeven inflation

- **API endpoint** (base URL): `https://api.stlouisfed.org/fred/series/observations` (same as the existing FRED integration)
- **Series/Dataset ID** (if applicable): `DFII10` (10-Year Treasury Inflation-Indexed Security, constant maturity); `T10YIE` (10-Year Breakeven Inflation Rate). Optionally `DFII5`/`T5YIE` for 5Y.
- **API key**: required (free) — already held for the existing FRED collectors; [fred.stlouisfed.org/docs/api/api_key.html](https://fred.stlouisfed.org/docs/api/api_key.html)
- **Rate limit**: 120 requests/minute (shared with existing FRED collectors)
- **Update frequency**: Daily (market days)
- **Proposed database table** + key columns: `us_tips_yields` — `date`, `series_id`, `value` (percent). Long format, one row per series per day.
- **Notes**: Fills the "US TIPS" item currently listed under "Not yet traced to a specific provider" in `API_SOURCES.md` with an API-backed source, so no scraping is needed. Missing days are returned as the string `"."` rather than null and must be filtered/converted (the existing FRED collectors presumably already handle this). `T10YIE` is derived by FRED from nominal minus TIPS yields, so it pairs naturally with `treasury_yields` already collected from the Treasury. Data is published with roughly a 1-business-day lag. Series IDs are from memory of FRED's catalogue and were not re-verified against the live API in this run; confirm before implementing.

### Bank of England IADB — GBP exchange rates

- **API endpoint** (base URL): `http://www.bankofengland.co.uk/boeapps/iadb/fromshowcolumns.asp?csv.x=yes` (same as the existing BoE IADB integration)
- **Series/Dataset ID** (if applicable): `XUDLUSS` (spot USD per GBP), `XUDLERS` (spot EUR per GBP). Optionally `XUDLJYS` (JPY per GBP).
- **API key**: not required
- **Rate limit**: not specified, reasonable use
- **Update frequency**: Daily (business days)
- **Proposed database table** + key columns: `uk_fx_rates` — `date`, `series_code`, `rate`. Long format, one row per series per day.
- **Notes**: Reuses the existing `IUDBEDR`/`IUMABEDR` CSV mechanism, so only new series codes are needed in the request parameters (`SeriesCodes=`). Useful context for the UK gilt/ETF and UK CPI pipelines (sterling moves vs. inflation and gilt yields). These are BoE-published daily spot rates (sourced from a market snapshot at a fixed time, not a close); check the series is still live. Series codes are from memory and were not re-verified against the live service in this run; confirm before implementing.
