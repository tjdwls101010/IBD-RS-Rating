"""Fetch and manage ticker universe from Finviz with monthly caching."""

import logging
from datetime import date

from finviz.screener import Screener

from .config import SCREENER_FILTERS, EXCLUDED_INDUSTRIES, REFERENCE_TICKERS
from . import db

logger = logging.getLogger(__name__)

CACHE_DAYS = 30  # refresh ticker list every 30 days


def _resolve_filters():
    """Verify filter codes against Finviz's available filters."""
    try:
        available = Screener.load_filter_dict()
        valid_codes = set()
        for category in available.values():
            if isinstance(category, dict):
                valid_codes.update(category.values())

        verified = []
        for f in SCREENER_FILTERS:
            if f in valid_codes:
                verified.append(f)
            else:
                logger.warning("Filter code '%s' not found in Finviz. Skipping.", f)
        return verified
    except Exception as e:
        logger.warning("Could not verify filters (%s), using as-is", e)
        return list(SCREENER_FILTERS)


def _fetch_from_finviz():
    """Fetch filtered ticker list from Finviz screener."""
    filters = _resolve_filters()
    logger.info("Fetching tickers from Finviz with filters: %s", filters)
    screener = Screener(filters=filters, table="Overview")

    data = screener.data
    initial_count = len(data)
    logger.info("Finviz returned %d stocks", initial_count)

    filtered = []
    excluded = 0
    for row in data:
        industry = row.get("Industry", "").strip()
        if industry in EXCLUDED_INDUSTRIES:
            excluded += 1
        else:
            filtered.append(row)
    if excluded:
        logger.info("Excluded %d ETFs/SPACs by Industry filter", excluded)

    tickers = sorted(row["Ticker"] for row in filtered)

    for ref in REFERENCE_TICKERS:
        if ref not in tickers:
            tickers.append(ref)

    tickers = sorted(set(tickers))
    logger.info("Final ticker count: %d", len(tickers))
    return tickers


def fetch_ticker_list(conn=None, force_refresh=False):
    """Get ticker list, using cached version if available and fresh.

    Args:
        conn: DB connection for caching. If None, always fetches from Finviz.
        force_refresh: If True, ignore cache and fetch fresh.

    Returns:
        Sorted list of ticker strings.
    """
    if conn and not force_refresh:
        cached = db.get_meta(conn, "ticker_list")
        last_fetch = db.get_meta(conn, "ticker_list_date")
        if cached and last_fetch:
            days_since = (date.today() - date.fromisoformat(last_fetch)).days
            if days_since < CACHE_DAYS:
                tickers = cached.split(",")
                logger.info("Using cached ticker list (%d tickers, %d days old)",
                           len(tickers), days_since)
                return tickers

    # Fetch fresh from Finviz
    tickers = _fetch_from_finviz()

    # Cache if conn is available
    if conn:
        db.set_meta(conn, "ticker_list", ",".join(tickers))
        db.set_meta(conn, "ticker_list_date", date.today().isoformat())

    return tickers
