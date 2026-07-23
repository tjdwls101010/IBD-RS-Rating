"""Stock split detection and repair."""

import logging
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

from . import db, prices
from .config import (
    PRICE_RETENTION_MONTHS,
    SPLIT_LOOKBACK_DAYS,
    SPLIT_REPAIR_MAX_TICKERS,
    SPLIT_THRESHOLD,
)

logger = logging.getLogger(__name__)


def detect_anomalous_changes(conn, threshold=None):
    threshold = SPLIT_THRESHOLD if threshold is None else threshold

    latest = db.get_latest_price_date(conn)
    if not latest:
        return []

    today = pd.Timestamp.now().normalize().strftime("%Y-%m-%d")
    anchor = min(latest, today)
    scan_start = (
        pd.Timestamp(anchor) - pd.Timedelta(days=SPLIT_LOOKBACK_DAYS)
    ).strftime("%Y-%m-%d")
    placeholder = "%s" if db._conn_is_pg(conn) else "?"
    query = f"""
        SELECT ticker, date, close FROM rs
        WHERE close IS NOT NULL
          AND date >= {placeholder}
          AND date <= {placeholder}
        ORDER BY ticker, date
    """
    df = pd.read_sql_query(query, conn, params=(scan_start, anchor))
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


def verify_and_repair(conn, flagged_tickers, max_tickers=None):
    if not flagged_tickers:
        return []

    max_tickers = max_tickers or SPLIT_REPAIR_MAX_TICKERS
    to_check = flagged_tickers[:max_tickers]
    cutoff = datetime.now() - timedelta(days=SPLIT_LOOKBACK_DAYS)
    retention_start = (
        datetime.now() - timedelta(days=PRICE_RETENTION_MONTHS * 30)
    ).strftime("%Y-%m-%d")
    today_ts = pd.Timestamp.now().normalize()

    confirmed = []
    for ticker in to_check:
        try:
            splits = yf.Ticker(ticker).splits
            if splits is None or splits.empty:
                continue

            recent = splits[
                splits.index >= pd.Timestamp(cutoff, tz=splits.index.tz)
            ]
            if recent.empty:
                continue

            confirmed.append(ticker)
            logger.info("%s: split detected (ratio: %s), re-downloading...",
                        ticker, recent.values.tolist())
        except Exception as e:
            logger.error("%s: error during split verification: %s", ticker, e)

    if not confirmed:
        return []

    try:
        close_df = prices._download_batch(confirmed, start=retention_start)
    except Exception as e:
        logger.error("Split repair download failed: %s", e)
        return []

    if close_df is None or close_df.empty:
        return []

    repaired = []
    for ticker in confirmed:
        try:
            if ticker not in close_df.columns:
                continue

            series = close_df[ticker].dropna()
            records = [
                (ticker, date.strftime("%Y-%m-%d"), float(price))
                for date, price in series.items()
                if date <= today_ts
            ]
            if not records:
                continue

            db.upsert_prices(conn, records)
            repaired.append(ticker)
            logger.info("%s: repaired with %d price records", ticker, len(records))
        except Exception as e:
            logger.error("%s: error during split repair: %s", ticker, e)
            try:
                conn.rollback()
            except Exception:
                pass

    return repaired
