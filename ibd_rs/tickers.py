"""Fetch and manage the ticker universe from Finviz, guarded against truncated or blocked fetches.

A truncated/blocked Finviz fetch must never silently overwrite the cached
universe (that was the 2026-07-08 incident: 55 tickers instead of ~4,600 got
cached for 30 days). Every fetch is validated before it is trusted; an
untrusted fetch falls back to the last-good cache, or to a Nasdaq Trader
derived universe if no cache exists at all -- and is reported back to the
caller as untrusted so the daily run can signal failure.
"""

import logging
import random
import time
from collections import namedtuple
from datetime import date

from finvizfinance.screener.overview import Overview
from finvizfinance.util import web_scrap

from .config import (
    SCREENER_FILTERS,
    EXCLUDED_INDUSTRIES,
    REFERENCE_TICKERS,
    CACHE_DAYS,
    UNIVERSE_FLOOR,
    UNIVERSE_DROP_GUARD,
    UNIVERSE_COMPLETENESS_RATIO,
    UNIVERSE_FETCH_RETRIES,
)
from . import db
from . import nasdaq_trader

logger = logging.getLogger(__name__)

# tickers: list of {"ticker", "sector", "industry"}
# trusted: False means this run should be treated as degraded (a fallback was served)
# reason: human-readable explanation, always set
UniverseResult = namedtuple("UniverseResult", ["tickers", "trusted", "reason"])


def _reported_total(foverview):
    """Best-effort peek at Finviz's own result total for the current filter.

    Costs one extra page request. Returns None (skip the completeness check)
    if the page can't be parsed -- this is defense in depth, not required for
    correct operation.
    """
    try:
        soup = web_scrap(foverview.url, foverview.request_params)
        page_select = soup.find(id="pageSelect")
        page_count = len(page_select.findAll("option")) if page_select else 1
        return page_count * foverview.size
    except Exception:
        logger.warning("Could not determine Finviz's reported total; skipping completeness check", exc_info=True)
        return None


def _fetch_universe_attempt():
    """One attempt to fetch the full filtered universe from Finviz.

    Raises on network/parse failure. Returns (records, raw_count,
    reported_total): records is [{"ticker", "sector", "industry"}, ...];
    raw_count is the row count Finviz actually returned, before our own
    EXCLUDED_INDUSTRIES filtering or reference-ticker addition; reported_total
    is Finviz's own claimed result count for the filter (or None if it
    couldn't be determined). raw_count and reported_total are compared
    against each other for truncation detection -- both are pre-filter, so
    the comparison isn't skewed by how many rows we choose to exclude.
    """
    foverview = Overview()
    foverview.set_filter(filters_dict=SCREENER_FILTERS)
    reported_total = _reported_total(foverview)

    df = foverview.screener_view(verbose=0)
    if df is None:
        raise RuntimeError("Finviz screener returned no results")
    raw_count = len(df)

    filtered = []
    excluded = 0
    for _, row in df.iterrows():
        industry = (row.get("Industry") or "").strip()
        if industry in EXCLUDED_INDUSTRIES:
            excluded += 1
            continue
        filtered.append({
            "ticker": row["Ticker"],
            "sector": (row.get("Sector") or "").strip() or None,
            "industry": industry or None,
        })
    if excluded:
        logger.info("Excluded %d ETFs/SPACs by Industry filter", excluded)

    existing = {r["ticker"] for r in filtered}
    for ref in REFERENCE_TICKERS:
        if ref not in existing:
            filtered.append({"ticker": ref, "sector": None, "industry": None})

    filtered.sort(key=lambda x: x["ticker"])
    return filtered, raw_count, reported_total


def _fetch_with_retries():
    """Retry _fetch_universe_attempt with exponential backoff + jitter. Raises after exhausting retries."""
    last_exc = None
    for attempt in range(1, UNIVERSE_FETCH_RETRIES + 1):
        try:
            return _fetch_universe_attempt()
        except Exception as e:
            last_exc = e
            if attempt < UNIVERSE_FETCH_RETRIES:
                delay = (2 ** (attempt - 1)) + random.uniform(0, 1)
                logger.warning(
                    "Finviz fetch attempt %d/%d failed (%s); retrying in %.1fs",
                    attempt, UNIVERSE_FETCH_RETRIES, e, delay,
                )
                time.sleep(delay)
    raise last_exc


def _validate_universe_size(count, last_good_count, raw_count, reported_total):
    """Decide whether a fetched count is trustworthy. Returns (ok, reason).

    `count` (our final, filtered + reference-tickers-added universe) is
    checked against the absolute floor and the last-good drop-guard.
    `raw_count` (Finviz's row count before our own filtering) is checked
    against `reported_total` (Finviz's own claimed total) for truncation --
    comparing raw to raw avoids the systematic gap that our own
    EXCLUDED_INDUSTRIES filtering would otherwise introduce.
    """
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


def _fetch_and_validate(last_good_count):
    """Fetch (with retries) and validate. Never raises -- returns (records_or_None, trusted, reason)."""
    try:
        records, raw_count, reported_total = _fetch_with_retries()
    except Exception as e:
        return None, False, f"Finviz fetch failed after {UNIVERSE_FETCH_RETRIES} attempts: {e}"

    ok, reason = _validate_universe_size(len(records), last_good_count, raw_count, reported_total)
    if not ok:
        return None, False, reason
    return records, True, reason


def fetch_ticker_list(conn, force_refresh=False):
    """Get the ticker universe, preferring a fresh validated Finviz fetch.

    Uses a cached universe (<= CACHE_DAYS old) unless force_refresh. On a
    fresh fetch, stores sector/industry in the tickers table and advances the
    cache only if the fetch is trusted (see _validate_universe_size); an
    untrusted fetch never overwrites the cache. Falls back to the last-good
    cache, or to a Nasdaq Trader derived universe if no cache exists.

    Returns (UniverseResult, conn). The connection is returned because a slow
    Finviz fetch (multi-minute screener scrape) can outlive Neon's serverless
    auto-suspend; if the original connection died in the gap, this transparently
    reconnects before writing results, and callers must use the returned
    connection for anything after this call.

    `trusted=False` means this run is degraded and callers should signal
    failure even though a usable ticker list is returned (self-healing: the
    pipeline keeps running on the best available data rather than freezing).
    """
    cached_str = db.get_meta(conn, "ticker_list")
    cached_date = db.get_meta(conn, "ticker_list_date")
    cached_tickers = cached_str.split(",") if cached_str else None

    if cached_tickers and cached_date and not force_refresh:
        days_since = (date.today() - date.fromisoformat(cached_date)).days
        if days_since < CACHE_DAYS:
            logger.info("Using cached ticker list (%d tickers, %d days old)", len(cached_tickers), days_since)
            return UniverseResult(cached_tickers, True, "cache-fresh"), conn

    last_good_count = len(cached_tickers) if cached_tickers else None
    records, trusted, reason = _fetch_and_validate(last_good_count)
    conn = db.reconnect(conn)

    if trusted:
        tickers = [r["ticker"] for r in records]
        db.set_meta(conn, "ticker_list", ",".join(tickers))
        db.set_meta(conn, "ticker_list_date", date.today().isoformat())
        db.set_meta(conn, "last_successful_fetch", date.today().isoformat())
        db.upsert_tickers(conn, [(r["ticker"], r["sector"], r["industry"]) for r in records])
        logger.info("Fetched and cached %d tickers from Finviz", len(tickers))
        return UniverseResult(tickers, True, reason), conn

    logger.warning("Universe fetch degraded: %s", reason)

    if cached_tickers:
        return UniverseResult(
            cached_tickers, False,
            f"{reason}; served last-good cache ({len(cached_tickers)} tickers)",
        ), conn

    fallback = nasdaq_trader.fetch_common_stock_tickers()
    fallback_tickers = [r["ticker"] for r in fallback]
    return UniverseResult(
        fallback_tickers, False,
        f"{reason}; no cache available, served Nasdaq Trader fallback ({len(fallback_tickers)} tickers)",
    ), conn
