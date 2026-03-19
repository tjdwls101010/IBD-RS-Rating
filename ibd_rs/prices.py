"""Download and manage price data via yfinance."""

import logging
import time

import pandas as pd
import yfinance as yf

from .config import BATCH_SIZE, INITIAL_PERIOD, RATE_LIMIT_PAUSE
from . import db

logger = logging.getLogger(__name__)


def _download_batch(tickers, **kwargs):
    """Download price data for a batch of tickers with rate limit handling.

    Returns DataFrame with Close prices (dates × tickers).
    """
    try:
        data = yf.download(
            tickers,
            auto_adjust=True,
            threads=True,
            progress=True,
            **kwargs,
        )
    except Exception as e:
        if "Rate" in str(type(e).__name__) or "429" in str(e):
            logger.warning("Rate limited. Waiting %ds...", RATE_LIMIT_PAUSE)
            time.sleep(RATE_LIMIT_PAUSE)
            data = yf.download(
                tickers,
                auto_adjust=True,
                threads=True,
                progress=True,
                **kwargs,
            )
        else:
            raise

    if data.empty:
        return pd.DataFrame()

    # Extract Close prices
    if isinstance(data.columns, pd.MultiIndex):
        close = data["Close"]
    else:
        # Single ticker returns flat columns
        close = data[["Close"]].rename(columns={"Close": tickers[0] if isinstance(tickers, list) and len(tickers) == 1 else tickers})

    return close


def _to_records(close_df):
    """Convert wide DataFrame to list of (ticker, date, close) tuples."""
    records = []
    for ticker in close_df.columns:
        series = close_df[ticker].dropna()
        for date, close in series.items():
            records.append((ticker, date.strftime("%Y-%m-%d"), float(close)))
    return records


def download_initial(tickers, conn):
    """Download 2 years of price history for all tickers.

    Returns dict of failed tickers and their error messages.
    """
    all_failed = {}
    total = len(tickers)

    for i in range(0, total, BATCH_SIZE):
        batch = tickers[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
        logger.info("Batch %d/%d: downloading %d tickers...", batch_num, total_batches, len(batch))

        try:
            close_df = _download_batch(batch, period=INITIAL_PERIOD)
        except Exception as e:
            logger.error("Batch %d failed: %s", batch_num, e)
            for t in batch:
                all_failed[t] = str(e)
            continue

        if close_df.empty:
            logger.warning("Batch %d returned no data", batch_num)
            continue

        # Check yfinance errors
        errors = getattr(yf.shared, "_ERRORS", {})
        if errors:
            for ticker, err in errors.items():
                all_failed[ticker] = str(err)
            logger.warning("Batch %d had %d ticker errors", batch_num, len(errors))

        records = _to_records(close_df)
        if records:
            db.upsert_prices(conn, records)
            logger.info("Batch %d: stored %d records", batch_num, len(records))

    if all_failed:
        logger.warning("Total failed tickers: %d", len(all_failed))
        db.set_meta(conn, "failed_tickers", ",".join(all_failed.keys()))

    return all_failed


def download_update(tickers, conn):
    """Download new price data since last update.

    Returns dict of failed tickers and their error messages.
    """
    last_date = db.get_latest_price_date(conn)
    if not last_date:
        logger.error("No existing data. Run 'init' first.")
        return {}

    # Start from the day after last stored date
    start = (pd.Timestamp(last_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    today = pd.Timestamp.now().strftime("%Y-%m-%d")

    if start > today:
        logger.info("Already up to date (last: %s)", last_date)
        return {}

    logger.info("Updating prices from %s to %s for %d tickers", start, today, len(tickers))

    all_failed = {}
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1

        try:
            close_df = _download_batch(batch, start=start)
        except Exception as e:
            logger.error("Update batch %d failed: %s", batch_num, e)
            for t in batch:
                all_failed[t] = str(e)
            continue

        if close_df.empty:
            continue

        errors = getattr(yf.shared, "_ERRORS", {})
        if errors:
            for ticker, err in errors.items():
                all_failed[ticker] = str(err)

        records = _to_records(close_df)
        if records:
            db.upsert_prices(conn, records)

    db.set_meta(conn, "last_update_date", today)
    return all_failed
