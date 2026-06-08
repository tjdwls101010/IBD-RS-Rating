"""RS Rating calculation engine."""

import logging

import pandas as pd

from .config import RS_UNIVERSE_THRESHOLD, RS_WEIGHTS
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
    universe_size,
    min_universe_fraction=RS_UNIVERSE_THRESHOLD,
):
    """Compute RS Rating (1-99 percentile rank) for each ticker on each date.

    All valid RS Raw values on a date are ranked together, including reference
    tickers such as SPY and QQQ. Dates below the universe threshold are left
    unrated.

    Args:
        rs_raw_df: DataFrame with RS Raw scores (dates × tickers).
        universe_size: Total ticker universe size for threshold gating.
        min_universe_fraction: Minimum valid fraction required to rate a date.

    Returns:
        DataFrame of same shape with RS Ratings (1-99).
    """
    if rs_raw_df.empty:
        return pd.DataFrame(index=rs_raw_df.index, columns=rs_raw_df.columns)
    if universe_size <= 0:
        raise ValueError("universe_size must be positive for non-empty RS Raw input")

    # Percentile rank across ALL tickers for each date (row)
    # rank(pct=True) returns values in (0, 1], scale to 1-99
    pct_rank = rs_raw_df.rank(axis=1, pct=True, method="average")
    rs_rating = (pct_rank * 98 + 1).round(0)

    valid_counts = rs_raw_df.notna().sum(axis=1)
    trusted_dates = valid_counts / universe_size >= min_universe_fraction
    rs_rating.loc[~trusted_dates, :] = pd.NA

    return rs_rating.astype("Int64")


def calculate_and_store(conn, recalc_all=False):
    """Calculate RS ratings and store in database.

    Args:
        conn: SQLite connection.
        recalc_all: If True, recalculate for all dates. Otherwise, only new dates.

    Returns:
        Number of records written.
    """
    price_df = db.get_prices_df(conn)
    if price_df.empty:
        logger.error("No price data available")
        return 0

    logger.info("Computing RS for %d tickers × %d dates", len(price_df.columns), len(price_df))

    # Compute RS Raw and Rating
    rs_raw_df = compute_rs_raw(price_df)
    rs_rating_df = compute_rs_rating(rs_raw_df, universe_size=len(price_df.columns))

    # Determine which dates to store
    if not recalc_all:
        last_rs_date = db.get_latest_rs_date(conn)
        if last_rs_date:
            mask = rs_raw_df.index > pd.Timestamp(last_rs_date)
            rs_raw_df = rs_raw_df.loc[mask]
            rs_rating_df = rs_rating_df.loc[mask]

    dates_to_recalculate = [d.strftime("%Y-%m-%d") for d in rs_raw_df.index]
    db.clear_rs_for_dates(conn, dates_to_recalculate)

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

    if records:
        # Insert in batches for performance
        batch_size = 50000
        for i in range(0, len(records), batch_size):
            db.upsert_rs(conn, records[i : i + batch_size])

    latest_date = rs_raw_df.index.max().strftime("%Y-%m-%d")
    db.set_meta(conn, "last_rs_date", latest_date)
    logger.info("Stored %d RS records (latest: %s)", len(records), latest_date)
    return len(records)
