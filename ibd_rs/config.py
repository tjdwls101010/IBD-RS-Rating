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
ANCHOR_TICKERS = [
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "GOOGL",
    "META",
    "TSLA",
    "BRK-B",
    "JPM",
    "V",
    "UNH",
    "LLY",
    "JNJ",
    "XOM",
]

# Finviz screener filters (finvizfinance filters_dict format)
SCREENER_FILTERS = {"Market Cap.": "+Micro (over $50mln)"}
EXCLUDED_INDUSTRIES = ["Exchange Traded Fund", "Shell Companies"]

# Ticker universe validation (see docs/wiki/Concepts.md "Universe validation").
# Refresh cadence is owned by .github/workflows/finviz_weekly.yml (weekly).
UNIVERSE_FLOOR = 3000  # absolute reject threshold; expected universe is ~4,600
UNIVERSE_DROP_GUARD = 0.90  # reject if fetched < this fraction of the last-good count
UNIVERSE_COMPLETENESS_RATIO = 0.98  # reject if fetched < this fraction of Finviz's own reported total
SYMBOL_SHAPE_PATTERN = r"^[A-Z][A-Z.\-]{0,5}$"
UNIVERSE_JACCARD_MIN = 0.9
UNIVERSE_SHAPE_MAX_BAD_FRACTION = 0.02
UNIVERSE_FETCH_RETRIES = 4

# Download settings
BATCH_SIZE = 75  # was 500; smaller batches limit the blast radius of a rate-limit block
DOWNLOAD_THREADS = 4  # reduced concurrency vs. yf.download's default thread pool (threads=True)
DOWNLOAD_RETRY_ATTEMPTS = 5
DOWNLOAD_BACKOFF_BASE = 1.0  # seconds; doubles each attempt (1, 2, 4, 8) plus jitter
INTER_BATCH_SLEEP_SECONDS = 2
INITIAL_PERIOD = "2y"
TRAILING_WINDOW_DAYS = 10
# Retention: keep ~15 months of close history. Widened from 13 so the trailing
# recompute window keeps >=252 trading days of lookback with a real margin --
# the window is PRICE_RETENTION_MONTHS*30 calendar days ~= *252/365 trading days
# (~310 at 15 months), leaving ~43 over 252 + RS_RECOMPUTE_WINDOW_DAYS.
PRICE_RETENTION_MONTHS = 15

# RS incremental recompute: re-clear and re-store the most recent N trading
# days on every run (in addition to any dates newer than the cursor), so a
# day left unrated by a prior low-coverage run is re-rated once its data
# completes. Every recomputed date must still have >=252 trading days of
# lookback inside the retention window; keep this small relative to that
# window's ~43-trading-day margin
# (see test_recompute_window_leaves_enough_lookback_margin_in_retention_window).
RS_RECOMPUTE_WINDOW_DAYS = 15

# Split detection. SPLIT_LOOKBACK_DAYS (the scan window) must strictly EXCEED
# the price re-download window (TRAILING_WINDOW_DAYS = 10): auto_adjust
# back-adjusts the whole re-downloaded window smoothly, so the split
# discontinuity sits at the seam just OUTSIDE it -- a 7-day scan never saw it.
SPLIT_THRESHOLD = 0.40  # flag daily changes > 40%
SPLIT_LOOKBACK_DAYS = 15  # > TRAILING_WINDOW_DAYS so the auto-adjust seam is in range
SPLIT_REPAIR_MAX_TICKERS = 25  # per-run cap on split re-download work
