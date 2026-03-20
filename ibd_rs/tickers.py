"""Fetch and manage ticker universe from Finviz with monthly caching."""

import logging
from datetime import date

from finviz.screener import Screener

from .config import SCREENER_FILTERS, EXCLUDED_INDUSTRIES, REFERENCE_TICKERS
from . import db

logger = logging.getLogger(__name__)

CACHE_DAYS = 30


def _resolve_filters():
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
    """Fetch filtered ticker list with sector/industry from Finviz screener.

    Returns list of dicts: [{"ticker": "NVDA", "sector": "Technology", "industry": "Semiconductors"}, ...]
    """
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
            filtered.append({
                "ticker": row["Ticker"],
                "sector": row.get("Sector", "").strip() or None,
                "industry": industry or None,
            })
    if excluded:
        logger.info("Excluded %d ETFs/SPACs by Industry filter", excluded)

    # Add reference tickers
    existing = {r["ticker"] for r in filtered}
    for ref in REFERENCE_TICKERS:
        if ref not in existing:
            filtered.append({"ticker": ref, "sector": None, "industry": None})

    filtered.sort(key=lambda x: x["ticker"])
    logger.info("Final ticker count: %d", len(filtered))
    return filtered


def fetch_ticker_list(conn=None, force_refresh=False):
    """Get ticker list, using cached version if available and fresh.

    Also stores sector/industry data in the tickers table.

    Returns sorted list of ticker strings.
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
    ticker_data = _fetch_from_finviz()
    tickers = [t["ticker"] for t in ticker_data]

    # Cache and store sector/industry
    if conn:
        db.set_meta(conn, "ticker_list", ",".join(tickers))
        db.set_meta(conn, "ticker_list_date", date.today().isoformat())

        # Upsert ticker info (sector/industry)
        records = [(t["ticker"], t["sector"], t["industry"]) for t in ticker_data]
        db.upsert_tickers(conn, records)

    return tickers
