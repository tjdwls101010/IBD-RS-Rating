"""Fetch, validate, cache, and serve the ticker universe.

Finviz refreshes are isolated to the explicit weekly refresh path. Daily
updates only consume a structurally valid cache; a missing or invalid cache
falls back to Nasdaq Trader and is marked untrusted.
"""

import logging
import random
import re
import time
from collections import namedtuple
from datetime import date

from . import db
from . import finviz
from . import nasdaq_trader
from .config import (
    ANCHOR_TICKERS,
    EXCLUDED_INDUSTRIES,
    REFERENCE_TICKERS,
    SCREENER_FILTERS,
    SYMBOL_SHAPE_PATTERN,
    UNIVERSE_COMPLETENESS_RATIO,
    UNIVERSE_DROP_GUARD,
    UNIVERSE_FETCH_RETRIES,
    UNIVERSE_FLOOR,
    UNIVERSE_JACCARD_MIN,
    UNIVERSE_SHAPE_MAX_BAD_FRACTION,
)

logger = logging.getLogger(__name__)

# tickers: list of ticker symbols
# trusted: False means this run should be treated as degraded (a fallback was served)
# reason: human-readable explanation, always set
UniverseResult = namedtuple("UniverseResult", ["tickers", "trusted", "reason"])


def _fetch_universe_attempt():
    """Fetch, filter, and normalize one complete Finviz screener attempt."""
    records, raw_count, reported_total = finviz.fetch_screener_records(SCREENER_FILTERS)

    filtered = []
    excluded = 0
    for record in records:
        industry = (record.get("industry") or "").strip()
        if industry in EXCLUDED_INDUSTRIES:
            excluded += 1
            continue
        filtered.append({
            "ticker": record["ticker"],
            "sector": (record.get("sector") or "").strip() or None,
            "industry": industry or None,
        })
    if excluded:
        logger.info("Excluded %d ETFs/SPACs by Industry filter", excluded)

    existing = {record["ticker"] for record in filtered}
    for reference in REFERENCE_TICKERS:
        if reference not in existing:
            filtered.append({"ticker": reference, "sector": None, "industry": None})

    filtered.sort(key=lambda record: record["ticker"])
    return filtered, raw_count, reported_total


def _fetch_with_retries():
    """Retry one Finviz universe fetch with exponential backoff and jitter."""
    last_exc = None
    for attempt in range(1, UNIVERSE_FETCH_RETRIES + 1):
        try:
            return _fetch_universe_attempt()
        except Exception as exc:
            last_exc = exc
            if attempt < UNIVERSE_FETCH_RETRIES:
                delay = (2 ** (attempt - 1)) + random.uniform(0, 1)
                logger.warning(
                    "Finviz fetch attempt %d/%d failed (%s); retrying in %.1fs",
                    attempt,
                    UNIVERSE_FETCH_RETRIES,
                    exc,
                    delay,
                )
                time.sleep(delay)
    raise last_exc


def _validate_universe_size(count, last_good_count, raw_count, reported_total):
    """Decide whether fetched row counts are trustworthy."""
    if count < UNIVERSE_FLOOR:
        return False, f"fetched {count} tickers, below absolute floor {UNIVERSE_FLOOR}"
    if last_good_count and count < UNIVERSE_DROP_GUARD * last_good_count:
        return False, (
            f"fetched {count} tickers, below {UNIVERSE_DROP_GUARD:.0%} of "
            f"last-good count {last_good_count}"
        )
    if reported_total and raw_count < UNIVERSE_COMPLETENESS_RATIO * reported_total:
        return False, (
            f"fetched {raw_count} raw rows, below {UNIVERSE_COMPLETENESS_RATIO:.0%} of "
            f"Finviz's reported total {reported_total} (likely truncated)"
        )
    return True, "ok"


def _validate_universe(records, last_good_tickers):
    """Validate anchor presence, symbol shape, and continuity."""
    fetched_tickers = [record.get("ticker") for record in records]
    fetched_set = {ticker for ticker in fetched_tickers if isinstance(ticker, str)}

    # ANCHOR_TICKERS are Finviz-sourced mega-cap commons, disjoint from the
    # post-fetch REFERENCE_TICKERS, so this validates real Finviz membership
    # whether or not references have been appended.
    missing_anchors = [ticker for ticker in ANCHOR_TICKERS if ticker not in fetched_set]
    if missing_anchors:
        return False, f"missing anchor tickers: {', '.join(missing_anchors)}"

    malformed_count = sum(
        not isinstance(ticker, str) or re.fullmatch(SYMBOL_SHAPE_PATTERN, ticker) is None
        for ticker in fetched_tickers
    )
    malformed_fraction = malformed_count / len(fetched_tickers)
    if malformed_fraction > UNIVERSE_SHAPE_MAX_BAD_FRACTION:
        return False, (
            f"malformed symbol fraction {malformed_fraction:.2%} exceeds "
            f"{UNIVERSE_SHAPE_MAX_BAD_FRACTION:.2%}"
        )

    if last_good_tickers:
        last_good_set = set(last_good_tickers)
        union = fetched_set | last_good_set
        jaccard = len(fetched_set & last_good_set) / len(union) if union else 1.0
        if jaccard < UNIVERSE_JACCARD_MIN:
            return False, (
                f"universe Jaccard {jaccard:.3f} is below "
                f"{UNIVERSE_JACCARD_MIN:.3f} vs last-good"
            )

    return True, "ok"


def _validate_cache(cached_tickers):
    """Read-time cache validity: absolute floor + anchor + shape (no Jaccard).

    The floor is enforced here too: without it a degenerate tiny cache that
    happens to contain the anchors would read as trusted on the daily path AND
    poison the refresh Jaccard baseline (a clean full fetch would score ~0
    against it and be rejected, so the cache could never self-heal).
    """
    unique = {ticker for ticker in cached_tickers if isinstance(ticker, str) and ticker}
    if len(unique) < UNIVERSE_FLOOR:
        return False, (
            f"cached universe has {len(unique)} unique tickers, below floor {UNIVERSE_FLOOR}"
        )
    return _validate_universe(
        [{"ticker": ticker} for ticker in cached_tickers],
        last_good_tickers=None,
    )


def _fetch_and_validate(last_good_tickers):
    """Fetch with retries and apply both size and semantic validation."""
    try:
        records, raw_count, reported_total = _fetch_with_retries()
    except Exception as exc:
        return None, False, (
            f"Finviz fetch failed after {UNIVERSE_FETCH_RETRIES} attempts: {exc}"
        )

    last_good_count = len(last_good_tickers) if last_good_tickers else None
    size_ok, size_reason = _validate_universe_size(
        len(records), last_good_count, raw_count, reported_total,
    )
    universe_ok, universe_reason = _validate_universe(records, last_good_tickers)
    if not size_ok:
        return None, False, size_reason
    if not universe_ok:
        return None, False, universe_reason
    return records, True, "ok"


def _read_cached_tickers(conn):
    cached = db.get_meta(conn, "ticker_list")
    return cached.split(",") if cached else None


def _nasdaq_fallback(conn, reason):
    fallback = nasdaq_trader.fetch_common_stock_tickers()
    conn = db.reconnect(conn)
    fallback_tickers = [record["ticker"] for record in fallback]
    return UniverseResult(
        fallback_tickers,
        False,
        f"{reason}; served Nasdaq Trader fallback ({len(fallback_tickers)} tickers)",
    ), conn


def refresh_universe(conn):
    """Fetch and validate Finviz, advancing the weekly cache only if trusted."""
    cached_tickers = _read_cached_tickers(conn)
    last_good_tickers = None
    invalid_cache_reason = None
    if cached_tickers:
        cache_ok, cache_reason = _validate_cache(cached_tickers)
        if cache_ok:
            last_good_tickers = cached_tickers
        else:
            invalid_cache_reason = cache_reason
            logger.warning("Ignoring invalid universe cache as a baseline: %s", cache_reason)

    records, trusted, reason = _fetch_and_validate(last_good_tickers)
    conn = db.reconnect(conn)

    if trusted:
        tickers = [record["ticker"] for record in records]
        db.upsert_tickers(
            conn,
            [
                (record["ticker"], record["sector"], record["industry"])
                for record in records
            ],
        )
        today = date.today().isoformat()
        db.set_meta(conn, "ticker_list", ",".join(tickers))
        db.set_meta(conn, "ticker_list_date", today)
        db.set_meta(conn, "last_successful_fetch", today)
        logger.info("Fetched and cached %d tickers from Finviz", len(tickers))
        return UniverseResult(tickers, True, reason), conn

    logger.warning("Universe refresh degraded: %s", reason)
    if last_good_tickers:
        return UniverseResult(
            last_good_tickers,
            False,
            f"{reason}; served last-good cache ({len(last_good_tickers)} tickers)",
        ), conn

    if invalid_cache_reason:
        reason = f"{reason}; cached universe invalid: {invalid_cache_reason}"
    else:
        reason = f"{reason}; no cache available"
    return _nasdaq_fallback(conn, reason)


def get_cached_universe(conn):
    """Return a valid cached universe without ever contacting Finviz."""
    cached_tickers = _read_cached_tickers(conn)
    if cached_tickers:
        ok, reason = _validate_cache(cached_tickers)
        if ok:
            logger.info("Using cached ticker list (%d tickers)", len(cached_tickers))
            return UniverseResult(cached_tickers, True, "cache-valid"), conn
        logger.warning("Cached universe rejected: %s", reason)
        return _nasdaq_fallback(conn, f"cached universe invalid: {reason}")

    return _nasdaq_fallback(conn, "no cached universe available")
