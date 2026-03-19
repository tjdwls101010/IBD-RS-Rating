"""Fetch and manage ticker universe from Finviz."""

import logging

from finviz.screener import Screener

from .config import SCREENER_FILTERS, EXCLUDED_INDUSTRIES, REFERENCE_TICKERS

logger = logging.getLogger(__name__)


def _resolve_filters():
    """Verify filter codes against Finviz's available filters.

    Returns the list of valid filter codes, logging warnings for any
    that couldn't be verified.
    """
    try:
        available = Screener.load_filter_dict()
        # Flatten all valid filter codes
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


def fetch_ticker_list(verify_filters=True):
    """Fetch filtered ticker list from Finviz screener.

    Returns sorted list of ticker strings, including reference tickers (SPY, QQQ).
    """
    filters = _resolve_filters() if verify_filters else list(SCREENER_FILTERS)

    logger.info("Fetching tickers from Finviz with filters: %s", filters)
    screener = Screener(filters=filters, table="Overview")

    data = screener.data  # list of dicts
    initial_count = len(data)
    logger.info("Finviz returned %d stocks", initial_count)

    # Post-filter: exclude ETFs and SPACs by Industry
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
    logger.info("Final ticker count: %d (+ %d reference)", len(tickers), len(REFERENCE_TICKERS))

    # Add reference tickers if not already present
    for ref in REFERENCE_TICKERS:
        if ref not in tickers:
            tickers.append(ref)

    return sorted(set(tickers))
