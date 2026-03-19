"""SQLite database operations."""

import sqlite3
import logging
from datetime import datetime, timedelta

import pandas as pd

from .config import DB_PATH, DATA_DIR, PRICE_RETENTION_MONTHS

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS price (
    ticker TEXT NOT NULL,
    date   TEXT NOT NULL,
    close  REAL NOT NULL,
    PRIMARY KEY (ticker, date)
);
CREATE INDEX IF NOT EXISTS idx_price_date ON price(date);

CREATE TABLE IF NOT EXISTS rs (
    ticker    TEXT NOT NULL,
    date      TEXT NOT NULL,
    rs_raw    REAL NOT NULL,
    rs_rating INTEGER,
    PRIMARY KEY (ticker, date)
);
CREATE INDEX IF NOT EXISTS idx_rs_date ON rs(date);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def get_connection(db_path=None):
    """Get a SQLite connection with WAL mode for better concurrency."""
    path = db_path or str(DB_PATH)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db(conn):
    """Create tables if they don't exist. Safe to call multiple times."""
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    logger.info("Database initialized")


def upsert_prices(conn, records):
    """Insert or replace price records. records: list of (ticker, date, close)."""
    conn.executemany(
        "INSERT OR REPLACE INTO price (ticker, date, close) VALUES (?, ?, ?)",
        records,
    )
    conn.commit()
    logger.info("Upserted %d price records", len(records))


def upsert_rs(conn, records):
    """Insert or replace RS records. records: list of (ticker, date, rs_raw, rs_rating)."""
    conn.executemany(
        "INSERT OR REPLACE INTO rs (ticker, date, rs_raw, rs_rating) VALUES (?, ?, ?, ?)",
        records,
    )
    conn.commit()
    logger.info("Upserted %d RS records", len(records))


def get_prices_df(conn, tickers=None):
    """Load prices as a wide DataFrame (dates × tickers).

    Returns DataFrame with DatetimeIndex rows and ticker columns, values = close.
    """
    query = "SELECT ticker, date, close FROM price ORDER BY date"
    df = pd.read_sql_query(query, conn)
    if df.empty:
        return pd.DataFrame()

    pivot = df.pivot(index="date", columns="ticker", values="close")
    pivot.index = pd.to_datetime(pivot.index)
    pivot.sort_index(inplace=True)

    if tickers:
        available = [t for t in tickers if t in pivot.columns]
        pivot = pivot[available]

    return pivot


def get_latest_price_date(conn):
    """Return the most recent date in the price table, or None."""
    cur = conn.execute("SELECT MAX(date) FROM price")
    row = cur.fetchone()
    return row[0] if row and row[0] else None


def get_latest_rs_date(conn):
    """Return the most recent date in the rs table, or None."""
    cur = conn.execute("SELECT MAX(date) FROM rs")
    row = cur.fetchone()
    return row[0] if row and row[0] else None


def get_meta(conn, key):
    """Get a metadata value by key, or None."""
    cur = conn.execute("SELECT value FROM meta WHERE key = ?", (key,))
    row = cur.fetchone()
    return row[0] if row else None


def set_meta(conn, key, value):
    """Set a metadata key-value pair."""
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (key, str(value))
    )
    conn.commit()


def prune_old_prices(conn):
    """Delete price records older than PRICE_RETENTION_MONTHS."""
    cutoff = (
        datetime.now() - timedelta(days=PRICE_RETENTION_MONTHS * 30)
    ).strftime("%Y-%m-%d")
    cur = conn.execute("DELETE FROM price WHERE date < ?", (cutoff,))
    conn.commit()
    deleted = cur.rowcount
    if deleted:
        logger.info("Pruned %d old price records (before %s)", deleted, cutoff)
    return deleted


def get_price_stats(conn):
    """Return dict with database statistics."""
    stats = {}
    cur = conn.execute("SELECT COUNT(*), COUNT(DISTINCT ticker), MIN(date), MAX(date) FROM price")
    row = cur.fetchone()
    stats["price_rows"] = row[0]
    stats["price_tickers"] = row[1]
    stats["price_min_date"] = row[2]
    stats["price_max_date"] = row[3]

    cur = conn.execute("SELECT COUNT(*), COUNT(DISTINCT ticker), MIN(date), MAX(date) FROM rs")
    row = cur.fetchone()
    stats["rs_rows"] = row[0]
    stats["rs_tickers"] = row[1]
    stats["rs_min_date"] = row[2]
    stats["rs_max_date"] = row[3]

    stats["last_update"] = get_meta(conn, "last_update_date")
    return stats
