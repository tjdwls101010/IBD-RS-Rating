"""Configuration constants."""

import os
from pathlib import Path

# Database backend: set DATABASE_URL env var for PostgreSQL (Supabase)
# If not set, falls back to local SQLite
DATABASE_URL = os.environ.get("DATABASE_URL")

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "rs.db"

# RS formula weights: {lookback_days: weight}
RS_WEIGHTS = {63: 0.4, 126: 0.2, 189: 0.2, 252: 0.2}
RS_UNIVERSE_THRESHOLD = 0.90
PRICE_COMPLETENESS_THRESHOLD = 0.90

# Reference tickers are included in the RS Rating population and ranked together.
REFERENCE_TICKERS = ["SPY", "QQQ"]

# Finviz screener filters (finvizfinance filters_dict format)
SCREENER_FILTERS = {"Market Cap.": "+Micro (over $50mln)"}
EXCLUDED_INDUSTRIES = ["Exchange Traded Fund", "Shell Companies"]

# Ticker universe caching and validation (see docs/plans/2026-07-11-neon-migration-and-reliability.md Slice 1)
CACHE_DAYS = 7
UNIVERSE_FLOOR = 3000  # absolute reject threshold; expected universe is ~4,600
UNIVERSE_DROP_GUARD = 0.90  # reject if fetched < this fraction of the last-good count
UNIVERSE_COMPLETENESS_RATIO = 0.98  # reject if fetched < this fraction of Finviz's own reported total
UNIVERSE_FETCH_RETRIES = 4

# Download settings
BATCH_SIZE = 500
INITIAL_PERIOD = "2y"
TRAILING_WINDOW_DAYS = 10
PRICE_RETENTION_MONTHS = 13

# Split detection
SPLIT_THRESHOLD = 0.40  # flag daily changes > 40%
SPLIT_LOOKBACK_DAYS = 7

# Rate limit handling
RATE_LIMIT_PAUSE = 60  # seconds
