# Changelog

## [0.3.1] - 2026-06-08

### Fixed
- **Data reliability recovery** — the daily pipeline had silently stalled (97% of tickers frozen at 2026-04-17 while the workflow still reported success). Root causes fixed and full RS history rebuilt:
  - Replace the global max-date cursor with a fixed trailing window, so a single ticker racing ahead can no longer starve the rest (the core stall bug)
  - yfinance failure detection via return-coverage check (`yf.shared._ERRORS` was removed in yfinance 1.x)
  - Per-ticker valid-trading-day ROC (was calendar-row shift); a date is left unrated when valid coverage is below 90%, so a partial day no longer produces meaningless ratings
  - Reference tickers (SPY/QQQ) are correctly ranked within the population (stale comment corrected)
- Implement 13-month `close` retention (was declared but never executed); RS ratings are retained indefinitely
- Add a silent-stall watchdog: `update` exits non-zero when the latest trading day's coverage drops below threshold, surfacing stalls through the existing failure email

### Changed
- Pin CI dependencies via `requirements.lock`; PyPI deploy metadata stays loose (unpinned yfinance/pandas major bumps had broken the pipeline)
- `init.yml` now installs engine dependencies (was missing them)

## [0.3.0] - 2026-03-20

### Added
- **Sector & Industry data**: `tickers` table with sector/industry for all stocks
- **6 new library methods**: `sectors()`, `industries()`, `sector_ranking()`, `industry_ranking()`, `sector_top()`, `industry_top()`
- `tickers.csv` now includes sector and industry columns

### Changed
- **Schema consolidation**: Merged `price` table into `rs` table (single table with `close`, `rs_raw`, `rs_rating`)
- Removed `prune_old_prices()` — no longer needed with unified table
- `get()` response now includes `close` price

## [0.2.0] - 2026-03-20

### Added
- `movers()` — stocks with biggest RS Rating change over N days
- `dates()` — available date range for RS data

### Changed
- SPY/QQQ now included in percentile ranking (previously excluded)
- `reference()` returns `rs_rating` alongside `rs_raw`

### Fixed
- `reference()` returning empty list after SPY/QQQ percentile inclusion

## [0.1.0] - 2026-03-20

### Added
- Initial release
- RS Rating calculation engine (`ibd_rs` package)
- Python client library (`rs_rating` package) with zero dependencies
- 7 core methods: `get()`, `history()`, `top()`, `bottom()`, `filter()`, `compare()`, `reference()`
- Supabase PostgreSQL backend with REST API
- GitHub Actions daily update workflow
- SQLite fallback for local development
- Stock split detection and repair
- Monthly ticker list caching via Finviz
