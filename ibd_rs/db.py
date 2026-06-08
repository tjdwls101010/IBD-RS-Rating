"""Database operations with SQLite/PostgreSQL dual backend."""

import sqlite3
import logging
from datetime import datetime, timedelta

import pandas as pd

from .config import (
    DB_PATH,
    DATA_DIR,
    DATABASE_URL,
    PRICE_COMPLETENESS_THRESHOLD,
    PRICE_RETENTION_MONTHS,
)

logger = logging.getLogger(__name__)

# --- Schema ---

SCHEMA_SQL_SQLITE = """
CREATE TABLE IF NOT EXISTS rs (
    ticker    TEXT NOT NULL,
    date      TEXT NOT NULL,
    close     REAL,
    rs_raw    REAL,
    rs_rating INTEGER,
    PRIMARY KEY (ticker, date)
);
CREATE INDEX IF NOT EXISTS idx_rs_date ON rs(date);

CREATE TABLE IF NOT EXISTS tickers (
    ticker   TEXT PRIMARY KEY,
    sector   TEXT,
    industry TEXT
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

SCHEMA_SQL_PG = """
CREATE TABLE IF NOT EXISTS rs (
    ticker    TEXT NOT NULL,
    date      TEXT NOT NULL,
    close     DOUBLE PRECISION,
    rs_raw    DOUBLE PRECISION,
    rs_rating INTEGER,
    PRIMARY KEY (ticker, date)
);
CREATE INDEX IF NOT EXISTS idx_rs_date ON rs(date);

CREATE TABLE IF NOT EXISTS tickers (
    ticker   TEXT PRIMARY KEY,
    sector   TEXT,
    industry TEXT
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


# --- Backend helpers ---

def _conn_is_pg(conn):
    return not isinstance(conn, sqlite3.Connection)


def _cursor(conn):
    return conn.cursor()


# --- Connection ---

def get_connection(db_path=None):
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
    """Insert or update close prices. records: list of (ticker, date, close)."""
    if not records:
        return
    if _conn_is_pg(conn):
        from psycopg2.extras import execute_values
        sql = """
            INSERT INTO rs (ticker, date, close) VALUES %s
            ON CONFLICT (ticker, date) DO UPDATE SET close = EXCLUDED.close
        """
        cur = _cursor(conn)
        execute_values(cur, sql, records, page_size=5000)
        cur.close()
    else:
        conn.executemany(
            "INSERT INTO rs (ticker, date, close) VALUES (?, ?, ?) "
            "ON CONFLICT (ticker, date) DO UPDATE SET close = excluded.close",
            records,
        )
    conn.commit()
    logger.info("Upserted %d price records", len(records))


def upsert_rs(conn, records):
    """Insert or update RS records. records: list of (ticker, date, rs_raw, rs_rating)."""
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
            "INSERT INTO rs (ticker, date, rs_raw, rs_rating) VALUES (?, ?, ?, ?) "
            "ON CONFLICT (ticker, date) DO UPDATE SET "
            "rs_raw = excluded.rs_raw, rs_rating = excluded.rs_rating",
            records,
        )
    conn.commit()
    logger.info("Upserted %d RS records", len(records))


def clear_rs_for_dates(conn, dates):
    """Clear RS fields for dates that are about to be recalculated."""
    date_values = list(dates)
    if not date_values:
        return

    p = "%s" if _conn_is_pg(conn) else "?"
    batch_size = 500
    cur = _cursor(conn)
    for i in range(0, len(date_values), batch_size):
        batch = date_values[i : i + batch_size]
        placeholders = ", ".join([p] * len(batch))
        cur.execute(
            f"UPDATE rs SET rs_raw = NULL, rs_rating = NULL WHERE date IN ({placeholders})",
            tuple(batch),
        )
    cur.close()
    conn.commit()


def upsert_tickers(conn, records):
    """Insert or update ticker info. records: list of (ticker, sector, industry)."""
    if not records:
        return
    if _conn_is_pg(conn):
        from psycopg2.extras import execute_values
        sql = """
            INSERT INTO tickers (ticker, sector, industry) VALUES %s
            ON CONFLICT (ticker) DO UPDATE SET
                sector = EXCLUDED.sector, industry = EXCLUDED.industry
        """
        cur = _cursor(conn)
        execute_values(cur, sql, records, page_size=5000)
        cur.close()
    else:
        conn.executemany(
            "INSERT INTO tickers (ticker, sector, industry) VALUES (?, ?, ?) "
            "ON CONFLICT (ticker) DO UPDATE SET "
            "sector = excluded.sector, industry = excluded.industry",
            records,
        )
    conn.commit()
    logger.info("Upserted %d ticker records", len(records))


def set_meta(conn, key, value):
    cur = _cursor(conn)
    if _conn_is_pg(conn):
        cur.execute(
            "INSERT INTO meta (key, value) VALUES (%s, %s) "
            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
            (key, str(value)),
        )
    else:
        cur.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) "
            "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            (key, str(value)),
        )
    cur.close()
    conn.commit()


def delete_ticker_prices(conn, ticker):
    """Clear close prices for a ticker (for split repair)."""
    cur = _cursor(conn)
    p = "%s" if _conn_is_pg(conn) else "?"
    cur.execute(f"UPDATE rs SET close = NULL WHERE ticker = {p}", (ticker,))
    cur.close()
    conn.commit()


def prune_old_close(conn):
    """Apply retention to rows older than the configured close window."""
    cutoff = (
        datetime.now() - timedelta(days=PRICE_RETENTION_MONTHS * 30)
    ).strftime("%Y-%m-%d")
    p = "%s" if _conn_is_pg(conn) else "?"
    cur = _cursor(conn)
    cur.execute(
        f"DELETE FROM rs WHERE date < {p} AND rs_rating IS NULL",
        (cutoff,),
    )
    deleted = cur.rowcount if cur.rowcount != -1 else 0
    cur.execute(
        f"UPDATE rs SET close = NULL "
        f"WHERE date < {p} AND rs_rating IS NOT NULL AND close IS NOT NULL",
        (cutoff,),
    )
    cleared = cur.rowcount if cur.rowcount != -1 else 0
    pruned = deleted + cleared
    cur.close()
    conn.commit()
    logger.info(
        "Pruned %d old records (%d deleted, %d close values cleared)",
        pruned,
        deleted,
        cleared,
    )
    return pruned


# --- Read operations ---

def get_prices_df(conn, tickers=None):
    """Load close prices as a wide DataFrame (dates x tickers)."""
    query = "SELECT ticker, date, close FROM rs WHERE close IS NOT NULL ORDER BY date"
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
    cur = _cursor(conn)
    cur.execute("SELECT MAX(date) FROM rs WHERE close IS NOT NULL")
    row = cur.fetchone()
    cur.close()
    return row[0] if row and row[0] else None


def get_latest_rs_date(conn):
    cur = _cursor(conn)
    cur.execute("SELECT MAX(date) FROM rs WHERE rs_raw IS NOT NULL")
    row = cur.fetchone()
    cur.close()
    return row[0] if row and row[0] else None


def classify_latest_trading_day_completeness(
    *,
    latest_date,
    universe_size,
    close_coverage,
    rating_coverage,
    threshold=PRICE_COMPLETENESS_THRESHOLD,
):
    """Classify whether latest trading day close coverage is complete enough."""
    universe_size = int(universe_size or 0)
    close_coverage = int(close_coverage or 0)
    rating_coverage = int(rating_coverage or 0)

    if universe_size <= 0:
        return {
            "latest_date": latest_date,
            "universe_size": 0,
            "close_coverage": close_coverage,
            "missing_close_count": None,
            "rating_coverage": rating_coverage,
            "missing_rating_count": None,
            "coverage_ratio": None,
            "threshold": threshold,
            "is_complete": False,
            "reason": "universe_unknown",
        }

    missing_close_count = max(universe_size - close_coverage, 0)
    missing_rating_count = max(universe_size - rating_coverage, 0)

    if latest_date is None:
        return {
            "latest_date": None,
            "universe_size": universe_size,
            "close_coverage": 0,
            "missing_close_count": universe_size,
            "rating_coverage": 0,
            "missing_rating_count": universe_size,
            "coverage_ratio": 0.0,
            "threshold": threshold,
            "is_complete": False,
            "reason": "no_price_data",
        }

    coverage_ratio = close_coverage / universe_size
    is_complete = coverage_ratio >= threshold
    return {
        "latest_date": latest_date,
        "universe_size": universe_size,
        "close_coverage": close_coverage,
        "missing_close_count": missing_close_count,
        "rating_coverage": rating_coverage,
        "missing_rating_count": missing_rating_count,
        "coverage_ratio": coverage_ratio,
        "threshold": threshold,
        "is_complete": is_complete,
        "reason": "complete" if is_complete else "close_coverage_below_threshold",
    }


def _normalize_universe_tickers(conn, universe_tickers):
    if universe_tickers is None:
        cached = get_meta(conn, "ticker_list")
        if not cached:
            return set()
        universe_tickers = cached.split(",")

    return {
        ticker.strip()
        for ticker in universe_tickers
        if ticker and ticker.strip()
    }


def _get_tickers_with_value_on_date(conn, date, column):
    if column not in {"close", "rs_rating"}:
        raise ValueError(f"unsupported completeness column: {column}")

    cur = _cursor(conn)
    p = "%s" if _conn_is_pg(conn) else "?"
    cur.execute(
        f"SELECT DISTINCT ticker FROM rs WHERE date = {p} AND {column} IS NOT NULL",
        (date,),
    )
    tickers = {row[0] for row in cur.fetchall()}
    cur.close()
    return tickers


def check_latest_trading_day_completeness(
    conn,
    universe_tickers=None,
    threshold=PRICE_COMPLETENESS_THRESHOLD,
):
    """Return latest trading day close/rating coverage against the ticker universe."""
    universe = _normalize_universe_tickers(conn, universe_tickers)
    latest_date = get_latest_price_date(conn)

    if latest_date is None or not universe:
        return classify_latest_trading_day_completeness(
            latest_date=latest_date,
            universe_size=len(universe),
            close_coverage=0,
            rating_coverage=0,
            threshold=threshold,
        )

    close_tickers = _get_tickers_with_value_on_date(conn, latest_date, "close")
    rating_tickers = _get_tickers_with_value_on_date(conn, latest_date, "rs_rating")
    return classify_latest_trading_day_completeness(
        latest_date=latest_date,
        universe_size=len(universe),
        close_coverage=len(universe & close_tickers),
        rating_coverage=len(universe & rating_tickers),
        threshold=threshold,
    )


def get_meta(conn, key):
    cur = _cursor(conn)
    p = "%s" if _conn_is_pg(conn) else "?"
    cur.execute(f"SELECT value FROM meta WHERE key = {p}", (key,))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None


def get_price_stats(conn):
    stats = {}
    cur = _cursor(conn)

    cur.execute("SELECT COUNT(*), COUNT(DISTINCT ticker), MIN(date), MAX(date) FROM rs WHERE close IS NOT NULL")
    row = cur.fetchone()
    stats["price_rows"] = row[0]
    stats["price_tickers"] = row[1]
    stats["price_min_date"] = row[2]
    stats["price_max_date"] = row[3]

    cur.execute("SELECT COUNT(*), COUNT(DISTINCT ticker), MIN(date), MAX(date) FROM rs WHERE rs_raw IS NOT NULL")
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
    cur = _cursor(conn)
    p = "%s" if _conn_is_pg(conn) else "?"
    cur.execute(
        f"SELECT ticker, rs_raw, rs_rating FROM rs "
        f"WHERE date = {p} AND ticker IN ('SPY', 'QQQ')",
        (date,),
    )
    rows = cur.fetchall()
    cur.close()
    return rows


def get_rs_history(conn, ticker, days):
    cur = _cursor(conn)
    p = "%s" if _conn_is_pg(conn) else "?"
    cur.execute(
        f"SELECT date, rs_raw, rs_rating FROM rs "
        f"WHERE ticker = {p} AND rs_raw IS NOT NULL ORDER BY date DESC LIMIT {p}",
        (ticker, days),
    )
    rows = cur.fetchall()
    cur.close()
    return rows


def get_rs_for_export(conn, date):
    """Get all RS ratings for a given date as a DataFrame, joined with sector/industry."""
    p = "%s" if _conn_is_pg(conn) else "?"
    query = (
        f"SELECT r.ticker, r.date, r.rs_raw, r.rs_rating, t.sector, t.industry "
        f"FROM rs r LEFT JOIN tickers t ON r.ticker = t.ticker "
        f"WHERE r.date = {p} AND r.rs_raw IS NOT NULL "
        f"ORDER BY r.rs_raw DESC"
    )
    return pd.read_sql_query(query, conn, params=(date,))
