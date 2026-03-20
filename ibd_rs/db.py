"""Database operations with SQLite/PostgreSQL dual backend.

Backend is selected via DATABASE_URL env var:
- Not set → SQLite (local development, tests)
- Set → PostgreSQL via psycopg2 (Supabase, GitHub Actions)
"""

import sqlite3
import logging
from datetime import datetime, timedelta

import pandas as pd

from .config import DB_PATH, DATA_DIR, PRICE_RETENTION_MONTHS, DATABASE_URL

logger = logging.getLogger(__name__)

# --- Schema ---

SCHEMA_SQL_SQLITE = """
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

SCHEMA_SQL_PG = """
CREATE TABLE IF NOT EXISTS price (
    ticker TEXT NOT NULL,
    date   TEXT NOT NULL,
    close  DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (ticker, date)
);
CREATE INDEX IF NOT EXISTS idx_price_date ON price(date);

CREATE TABLE IF NOT EXISTS rs (
    ticker    TEXT NOT NULL,
    date      TEXT NOT NULL,
    rs_raw    DOUBLE PRECISION NOT NULL,
    rs_rating INTEGER,
    PRIMARY KEY (ticker, date)
);
CREATE INDEX IF NOT EXISTS idx_rs_date ON rs(date);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


# --- Backend helpers ---

def _conn_is_pg(conn):
    """Check if connection is PostgreSQL (not SQLite)."""
    return not isinstance(conn, sqlite3.Connection)


def _cursor(conn):
    """Get a cursor, works for both backends."""
    return conn.cursor()


# --- Connection ---

def get_connection(db_path=None):
    """Get a database connection.

    If db_path is given, always uses SQLite (for tests).
    Otherwise, uses PostgreSQL if DATABASE_URL is set, else SQLite.
    """
    if DATABASE_URL and db_path is None:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        return conn

    path = db_path or str(DB_PATH)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


# --- Schema ---

def init_db(conn):
    """Create tables if they don't exist. Safe to call multiple times."""
    cur = _cursor(conn)
    if _conn_is_pg(conn):
        cur.execute(SCHEMA_SQL_PG)
    else:
        conn.executescript(SCHEMA_SQL_SQLITE)
    conn.commit()
    cur.close()
    logger.info("Database initialized")


# --- Write operations ---

def upsert_prices(conn, records):
    """Insert or replace price records. records: list of (ticker, date, close)."""
    if not records:
        return
    if _conn_is_pg(conn):
        from psycopg2.extras import execute_values
        sql = """
            INSERT INTO price (ticker, date, close) VALUES %s
            ON CONFLICT (ticker, date) DO UPDATE SET close = EXCLUDED.close
        """
        cur = _cursor(conn)
        execute_values(cur, sql, records, page_size=5000)
        cur.close()
    else:
        conn.executemany(
            "INSERT OR REPLACE INTO price (ticker, date, close) VALUES (?, ?, ?)",
            records,
        )
    conn.commit()
    logger.info("Upserted %d price records", len(records))


def upsert_rs(conn, records):
    """Insert or replace RS records. records: list of (ticker, date, rs_raw, rs_rating)."""
    if not records:
        return
    if _conn_is_pg(conn):
        from psycopg2.extras import execute_values
        sql = """
            INSERT INTO rs (ticker, date, rs_raw, rs_rating) VALUES %s
            ON CONFLICT (ticker, date) DO UPDATE SET
                rs_raw = EXCLUDED.rs_raw, rs_rating = EXCLUDED.rs_rating
        """
        cur = _cursor(conn)
        execute_values(cur, sql, records, page_size=5000)
        cur.close()
    else:
        conn.executemany(
            "INSERT OR REPLACE INTO rs (ticker, date, rs_raw, rs_rating) VALUES (?, ?, ?, ?)",
            records,
        )
    conn.commit()
    logger.info("Upserted %d RS records", len(records))


def set_meta(conn, key, value):
    """Set a metadata key-value pair."""
    cur = _cursor(conn)
    if _conn_is_pg(conn):
        cur.execute(
            "INSERT INTO meta (key, value) VALUES (%s, %s) "
            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
            (key, str(value)),
        )
    else:
        cur.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
            (key, str(value)),
        )
    cur.close()
    conn.commit()


def delete_ticker_prices(conn, ticker):
    """Delete all price records for a specific ticker."""
    cur = _cursor(conn)
    p = "%s" if _conn_is_pg(conn) else "?"
    cur.execute(f"DELETE FROM price WHERE ticker = {p}", (ticker,))
    cur.close()
    conn.commit()


# --- Read operations ---

def get_prices_df(conn, tickers=None):
    """Load prices as a wide DataFrame (dates x tickers).

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
    cur = _cursor(conn)
    cur.execute("SELECT MAX(date) FROM price")
    row = cur.fetchone()
    cur.close()
    return row[0] if row and row[0] else None


def get_latest_rs_date(conn):
    """Return the most recent date in the rs table, or None."""
    cur = _cursor(conn)
    cur.execute("SELECT MAX(date) FROM rs")
    row = cur.fetchone()
    cur.close()
    return row[0] if row and row[0] else None


def get_meta(conn, key):
    """Get a metadata value by key, or None."""
    cur = _cursor(conn)
    p = "%s" if _conn_is_pg(conn) else "?"
    cur.execute(f"SELECT value FROM meta WHERE key = {p}", (key,))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None


def prune_old_prices(conn):
    """Delete price records older than PRICE_RETENTION_MONTHS."""
    cutoff = (
        datetime.now() - timedelta(days=PRICE_RETENTION_MONTHS * 30)
    ).strftime("%Y-%m-%d")
    cur = _cursor(conn)
    p = "%s" if _conn_is_pg(conn) else "?"
    cur.execute(f"DELETE FROM price WHERE date < {p}", (cutoff,))
    conn.commit()
    deleted = cur.rowcount
    cur.close()
    if deleted:
        logger.info("Pruned %d old price records (before %s)", deleted, cutoff)
    return deleted


def get_price_stats(conn):
    """Return dict with database statistics."""
    stats = {}
    cur = _cursor(conn)

    cur.execute("SELECT COUNT(*), COUNT(DISTINCT ticker), MIN(date), MAX(date) FROM price")
    row = cur.fetchone()
    stats["price_rows"] = row[0]
    stats["price_tickers"] = row[1]
    stats["price_min_date"] = row[2]
    stats["price_max_date"] = row[3]

    cur.execute("SELECT COUNT(*), COUNT(DISTINCT ticker), MIN(date), MAX(date) FROM rs")
    row = cur.fetchone()
    stats["rs_rows"] = row[0]
    stats["rs_tickers"] = row[1]
    stats["rs_min_date"] = row[2]
    stats["rs_max_date"] = row[3]

    cur.close()
    stats["last_update"] = get_meta(conn, "last_update_date")
    return stats


# --- Query functions (used by CLI) ---

def get_top_rs(conn, date, n):
    """Get top N stocks by RS Rating for a given date.

    Returns list of (ticker, rs_rating, rs_raw).
    """
    cur = _cursor(conn)
    p = "%s" if _conn_is_pg(conn) else "?"
    cur.execute(
        f"SELECT ticker, rs_rating, rs_raw FROM rs "
        f"WHERE date = {p} AND rs_rating IS NOT NULL "
        f"ORDER BY rs_rating DESC, rs_raw DESC LIMIT {p}",
        (date, n),
    )
    rows = cur.fetchall()
    cur.close()
    return rows


def get_reference_rs(conn, date):
    """Get RS Raw for reference tickers (SPY, QQQ) on a given date.

    Returns list of (ticker, rs_raw).
    """
    cur = _cursor(conn)
    p = "%s" if _conn_is_pg(conn) else "?"
    cur.execute(
        f"SELECT ticker, rs_raw FROM rs WHERE date = {p} AND rs_rating IS NULL",
        (date,),
    )
    rows = cur.fetchall()
    cur.close()
    return rows


def get_rs_history(conn, ticker, days):
    """Get RS history for a specific ticker.

    Returns list of (date, rs_raw, rs_rating).
    """
    cur = _cursor(conn)
    p = "%s" if _conn_is_pg(conn) else "?"
    cur.execute(
        f"SELECT date, rs_raw, rs_rating FROM rs "
        f"WHERE ticker = {p} ORDER BY date DESC LIMIT {p}",
        (ticker, days),
    )
    rows = cur.fetchall()
    cur.close()
    return rows


def get_rs_for_export(conn, date):
    """Get all RS ratings for a given date as a DataFrame."""
    p = "%s" if _conn_is_pg(conn) else "?"
    query = (
        f"SELECT ticker, date, rs_raw, rs_rating FROM rs "
        f"WHERE date = {p} ORDER BY rs_raw DESC"
    )
    return pd.read_sql_query(query, conn, params=(date,))
