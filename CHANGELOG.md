# Changelog

## [0.4.0] - 2026-07-11

### Changed
- **Breaking:** the public read client (`rs_rating.RS`) is now backed by Neon instead of Supabase. Supabase stopped receiving new data on 2026-07-11 and will be decommissioned.
- **Breaking:** `RS(url=None, key=None)` → `RS(url=None, auth_url=None)`. Neon issues short-lived anonymous tokens automatically; there's no static key for callers to supply. The client fetches and caches a token per session, refreshing it before it expires.
- Engine's write database migrated from Supabase to Neon (serverless Postgres) — smaller footprint after bloat reclamation, and connection keepalives for long-running batch jobs.

### Fixed
- Ticker-universe completeness check compared the post-filter ticker count against Finviz's raw page total, causing false-positive "truncated" rejections and unnecessary fallback to a lower-quality ticker source.
- A dropped database connection mid-write could crash the whole `update` run; price-download batches now fail and retry independently instead.
- `update` could crash if Neon's serverless compute auto-suspended during the ticker-list fetch (a multi-minute Finviz scrape with no DB activity); the client now detects and transparently reconnects.
- Self-healing RS recompute window widened from 10 to 15 trading days, reducing the chance that a cohort of tickers crossing the 252-trading-day history minimum around the same time has a date permanently skipped once the cursor moves past it.

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
