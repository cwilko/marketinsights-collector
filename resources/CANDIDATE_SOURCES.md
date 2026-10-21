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

(none proposed yet)
