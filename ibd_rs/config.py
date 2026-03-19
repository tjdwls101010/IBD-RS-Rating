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

# Reference tickers (RS Raw stored but excluded from percentile ranking)
REFERENCE_TICKERS = ["SPY", "QQQ"]

# Finviz screener filters
# Note: exact filter codes verified via Screener.load_filter_dict()
SCREENER_FILTERS = ["cap_microover"]
EXCLUDED_INDUSTRIES = ["Exchange Traded Fund", "Shell Companies"]

# Download settings
BATCH_SIZE = 500
INITIAL_PERIOD = "2y"
PRICE_RETENTION_MONTHS = 13

# Split detection
SPLIT_THRESHOLD = 0.40  # flag daily changes > 40%
SPLIT_LOOKBACK_DAYS = 7

# Rate limit handling
RATE_LIMIT_PAUSE = 60  # seconds
