"""RS Rating calculation engine."""

import logging

import pandas as pd

from .config import (
    REFERENCE_TICKERS,
    RS_RECOMPUTE_WINDOW_DAYS,
    RS_UNIVERSE_THRESHOLD,
    RS_WEIGHTS,
    UNIVERSE_FLOOR,
)
from . import db

logger = logging.getLogger(__name__)


def compute_rs_raw(price_df):
    """Compute RS Raw Score for all tickers across all dates.

    RS Raw = 0.4 * ROC(63) + 0.2 * ROC(126) + 0.2 * ROC(189) + 0.2 * ROC(252)
    where ROC(n) = (price_today / price_n_valid_trading_days_ago) - 1

    Args:
        price_df: DataFrame with DatetimeIndex rows and ticker columns. NaN cells
            represent dates where that ticker has no valid close.

    Returns:
        DataFrame of same shape with RS Raw scores.
    """
    if price_df.empty:
        return pd.DataFrame(index=price_df.index, columns=price_df.columns)

    rs_raw = pd.DataFrame(index=price_df.index, columns=price_df.columns, dtype=float)
    max_lookback = max(RS_WEIGHTS)

    for ticker in price_df.columns:
        prices = price_df[ticker].dropna()
        if len(prices) <= max_lookback:
            continue

        ticker_raw = pd.Series(0.0, index=prices.index, dtype=float)
        for days, weight in RS_WEIGHTS.items():
            ticker_raw += weight * (prices / prices.shift(days) - 1)
        rs_raw.loc[ticker_raw.index, ticker] = ticker_raw

    return rs_raw


def compute_rs_rating(
    rs_raw_df,
    active_universe,
    min_universe_fraction=RS_UNIVERSE_THRESHOLD,
):
    """Compute RS Rating (1-99 percentile rank) for each ticker on each date.

    All valid RS Raw values on a date are ranked together, including reference
    tickers such as SPY and QQQ. A date is rated only if enough of the *active
    universe* has a valid RS Raw that day.

    The gate denominator is ``max(len(active_universe), UNIVERSE_FLOOR)`` -- the
    same stationary denominator the completeness watchdog uses -- so it does not
    drift as delisted/garbage tickers pile up in the stored price union. The
    numerator counts only tickers in ``active_universe`` plus the reference
    tickers, so ghosts that are no longer members can neither inflate the
    denominator nor pad coverage. Ranking, by contrast, still spans every valid
    RS Raw that day (methodology unchanged).

    Args:
        rs_raw_df: DataFrame with RS Raw scores (dates × tickers).
        active_universe: Current validated membership (set/iterable of tickers),
            floored at UNIVERSE_FLOOR for the gate denominator.
        min_universe_fraction: Minimum valid fraction required to rate a date.

    Returns:
        DataFrame of same shape with RS Ratings (1-99).
    """
    if rs_raw_df.empty:
        return pd.DataFrame(index=rs_raw_df.index, columns=rs_raw_df.columns)

    active = set(active_universe)
    denominator = max(len(active), UNIVERSE_FLOOR, 1)

    # Percentile rank across ALL tickers for each date (row); ranking population
    # is unchanged. rank(pct=True) returns values in (0, 1], scaled to 1-99.
    pct_rank = rs_raw_df.rank(axis=1, pct=True, method="average")
    rs_rating = (pct_rank * 98 + 1).round(0)

    # Gate coverage counts only active-universe members (plus reference
    # tickers), measured against the stationary floored denominator.
    gate_members = active | set(REFERENCE_TICKERS)
    gate_cols = [c for c in rs_raw_df.columns if c in gate_members]
    if gate_cols:
        valid_counts = rs_raw_df[gate_cols].notna().sum(axis=1)
    else:
        valid_counts = pd.Series(0, index=rs_raw_df.index)
    trusted_dates = valid_counts / denominator >= min_universe_fraction
    rs_rating.loc[~trusted_dates, :] = pd.NA

    return rs_rating.astype("Int64")


def _assert_full_recompute_is_safe(conn, force_full):
    """Refuse an unforced destructive recompute when stored RS Raw exists."""
    if db.get_oldest_rs_date(conn) is not None and not force_full:
        raise SystemExit(
            "Refusing full recompute without --force-full: it clears every rating "
            "and only restores dates with full lookback. Use "
            "`recalc --from <date> --to <date>` for a safe rating-only backfill."
        )


def _assert_recompute_preserves_stored_rs_raw(conn, price_df, rs_raw_df):
    """Refuse a recompute that cannot restore every stored RS Raw cell it clears."""
    clear_dates = {date.strftime("%Y-%m-%d") for date in price_df.index}
    stored = db.get_stored_rs_raw_keys(conn, clear_dates)
    lost = set()
    for ticker, date_str in stored:
        date = pd.Timestamp(date_str)
        if (
            ticker not in rs_raw_df.columns
            or date not in rs_raw_df.index
            or pd.isna(rs_raw_df.at[date, ticker])
        ):
            lost.add((ticker, date_str))

    if lost:
        raise SystemExit(
            f"Refusing full recompute: {len(lost)} stored rs_raw cells would be "
            "cleared but not recomputed (pruned per-ticker lookback), e.g. "
            f"{sorted(lost)[:3]}. A full recompute would drop them irrecoverably. "
            "Use `recalc --from/--to`."
        )


def backfill_ratings(conn, start, end, active_universe=None):
    """Re-rank stored RS Raw values and update only their RS ratings."""
    rs_raw_df = db.get_rs_raw_df(conn, start, end)
    if rs_raw_df.empty:
        return 0

    if active_universe is None:
        active_universe = db.get_active_universe(conn)
    rs_rating_df = compute_rs_rating(
        rs_raw_df,
        active_universe=active_universe,
    )

    records = []
    for date in rs_raw_df.index:
        date_str = date.strftime("%Y-%m-%d")
        for ticker in rs_raw_df.columns:
            raw = rs_raw_df.at[date, ticker]
            if pd.isna(raw):
                continue
            rating = rs_rating_df.at[date, ticker]
            records.append(
                (ticker, date_str, None if pd.isna(rating) else int(rating))
            )

    db.update_rs_ratings(conn, records)
    return len(records)


def calculate_and_store(
    conn,
    recalc_all=False,
    active_universe=None,
    force_full=False,
):
    """Calculate RS ratings and store in database.

    Args:
        conn: SQLite connection.
        recalc_all: If True, recalculate for all dates. Otherwise, only new dates.
        active_universe: Validated membership for the rating gate. When None,
            the cached ticker_list is used; an empty/unknown universe warns and
            gates every date off (no fallback to the delisting-inflated price
            union).
        force_full: Explicit acknowledgement for a destructive full recompute.

    Returns:
        Number of records written.
    """
    if recalc_all:
        _assert_full_recompute_is_safe(conn, force_full)

    price_df = db.get_prices_df(conn)
    if price_df.empty:
        logger.error("No price data available")
        return 0

    logger.info("Computing RS for %d tickers × %d dates", len(price_df.columns), len(price_df))

    # Resolve the active universe for the stationary rating gate: the validated
    # membership (cached ticker_list). An unknown/empty membership gates every
    # date off (floored denominator with a ~0 numerator -> all NULL) rather than
    # falling back to the delisting-inflated price union -- that union is the
    # non-stationary denominator that caused the rating outage. The co-shipped
    # watchdog then surfaces an empty-universe run as a red build.
    if active_universe is None:
        active_universe = db.get_active_universe(conn)
    active_universe = set(active_universe)
    if not active_universe:
        logger.warning(
            "No active universe (cached ticker_list empty and none passed); "
            "RS ratings will be gated off for this run"
        )

    # Compute RS Raw and Rating
    rs_raw_df = compute_rs_raw(price_df)
    if recalc_all:
        _assert_recompute_preserves_stored_rs_raw(conn, price_df, rs_raw_df)
    rs_rating_df = compute_rs_rating(rs_raw_df, active_universe=active_universe)

    # Determine which dates to store. The incremental path recomputes dates
    # newer than the cursor (catch-up after a gap) UNION a trailing window of
    # the most recent trading days (self-healing: a day left unrated by an
    # earlier low-coverage run is re-rated once its data completes, without
    # requiring the cursor itself to move).
    if not recalc_all:
        last_rs_date = db.get_latest_rs_date(conn)
        if last_rs_date:
            mask = rs_raw_df.index > pd.Timestamp(last_rs_date)
            if len(rs_raw_df.index) > 0:
                trailing_start = rs_raw_df.index[-RS_RECOMPUTE_WINDOW_DAYS:].min()
                mask |= rs_raw_df.index >= trailing_start
            rs_raw_df = rs_raw_df.loc[mask]
            rs_rating_df = rs_rating_df.loc[mask]
        else:
            raise SystemExit(
                "No RS cursor: rs_raw is empty. Run `init --force-full` to build "
                "ratings from scratch; refusing to silently full-recompute."
            )

    dates_to_recalculate = [d.strftime("%Y-%m-%d") for d in rs_raw_df.index]

    # Drop rows where all RS Raw values are NaN (insufficient history)
    valid_mask = rs_raw_df.notna().any(axis=1)
    rs_raw_df = rs_raw_df.loc[valid_mask]
    rs_rating_df = rs_rating_df.loc[valid_mask]

    if rs_raw_df.empty:
        logger.info("No new RS ratings to compute")
        return 0

    # Build records: (ticker, date, rs_raw, rs_rating)
    records = []
    for date in rs_raw_df.index:
        date_str = date.strftime("%Y-%m-%d")
        for ticker in rs_raw_df.columns:
            raw = rs_raw_df.at[date, ticker]
            if pd.isna(raw):
                continue
            rating = rs_rating_df.at[date, ticker] if ticker in rs_rating_df.columns else None
            if pd.isna(rating):
                rating = None
            else:
                rating = int(rating)
            records.append((ticker, date_str, float(raw), rating))

    latest_date = rs_raw_df.index.max().strftime("%Y-%m-%d")
    try:
        db.clear_rs_for_dates(conn, dates_to_recalculate, commit=False)
        if records:
            # Insert in batches for performance
            batch_size = 50000
            for i in range(0, len(records), batch_size):
                db.upsert_rs(conn, records[i : i + batch_size], commit=False)
        db.set_meta(conn, "last_rs_date", latest_date, commit=False)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    logger.info("Stored %d RS records (latest: %s)", len(records), latest_date)
    return len(records)
