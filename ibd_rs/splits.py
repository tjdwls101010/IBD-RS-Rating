"""Stock split detection and repair."""

import logging
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

from .config import SPLIT_THRESHOLD, SPLIT_LOOKBACK_DAYS, INITIAL_PERIOD
from . import db

logger = logging.getLogger(__name__)


def detect_anomalous_changes(conn, threshold=None):
    """Detect tickers with suspicious daily price changes.

    Returns list of ticker symbols where |daily change| > threshold.
    """
    threshold = threshold or SPLIT_THRESHOLD

    # Get the last 5 trading days of data
    query = """
        SELECT ticker, date, close FROM price
        WHERE date >= (SELECT date(MAX(date), '-7 days') FROM price)
        ORDER BY ticker, date
    """
    df = pd.read_sql_query(query, conn)
    if df.empty:
        return []

    flagged = []
    for ticker, group in df.groupby("ticker"):
        if len(group) < 2:
            continue
        group = group.sort_values("date")
        closes = group["close"].values
        for i in range(1, len(closes)):
            if closes[i - 1] == 0:
                continue
            pct_change = abs(closes[i] / closes[i - 1] - 1)
            if pct_change > threshold:
                flagged.append(ticker)
                break

    if flagged:
        logger.info("Detected %d tickers with anomalous price changes: %s",
                     len(flagged), flagged[:10])
    return flagged


def verify_and_repair(conn, flagged_tickers):
    """Check flagged tickers for actual splits and re-download if needed.

    Returns list of tickers that were repaired.
    """
    if not flagged_tickers:
        return []

    repaired = []
    cutoff = datetime.now() - timedelta(days=SPLIT_LOOKBACK_DAYS)

    for ticker in flagged_tickers:
        try:
            t = yf.Ticker(ticker)
            splits = t.splits

            if splits.empty:
                logger.debug("%s: no split history, likely genuine price move", ticker)
                continue

            # Check for recent splits
            recent = splits[splits.index >= pd.Timestamp(cutoff, tz=splits.index.tz)]
            if recent.empty:
                logger.debug("%s: no recent splits, likely genuine price move", ticker)
                continue

            # Split confirmed — re-download full history
            logger.info("%s: split detected (ratio: %s), re-downloading...",
                        ticker, recent.values.tolist())

            data = yf.download(ticker, period=INITIAL_PERIOD, auto_adjust=True, progress=False)
            if data.empty:
                logger.warning("%s: re-download returned no data", ticker)
                continue

            # Delete old data for this ticker
            conn.execute("DELETE FROM price WHERE ticker = ?", (ticker,))

            # Insert fresh adjusted data
            if isinstance(data.columns, pd.MultiIndex):
                close = data["Close"].iloc[:, 0]
            else:
                close = data["Close"]

            records = [
                (ticker, date.strftime("%Y-%m-%d"), float(price))
                for date, price in close.dropna().items()
            ]
            db.upsert_prices(conn, records)
            repaired.append(ticker)
            logger.info("%s: repaired with %d price records", ticker, len(records))

        except Exception as e:
            logger.error("%s: error during split verification: %s", ticker, e)

    return repaired
