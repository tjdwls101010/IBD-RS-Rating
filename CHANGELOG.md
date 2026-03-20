# Changelog

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
