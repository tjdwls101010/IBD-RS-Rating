"""Tests for database operations."""

import sqlite3
import pytest

from ibd_rs import db


@pytest.fixture
def conn():
    """In-memory SQLite connection with schema initialized."""
    c = db.get_connection(":memory:")
    db.init_db(c)
    yield c
    c.close()


def test_init_db_creates_tables(conn):
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    names = [t[0] for t in tables]
    assert "price" in names
    assert "rs" in names
    assert "meta" in names


def test_init_db_idempotent(conn):
    # Calling init_db again should not fail
    db.init_db(conn)


def test_upsert_prices(conn):
    records = [
        ("AAPL", "2026-01-01", 150.0),
        ("AAPL", "2026-01-02", 152.0),
        ("NVDA", "2026-01-01", 800.0),
    ]
    db.upsert_prices(conn, records)

    count = conn.execute("SELECT COUNT(*) FROM price").fetchone()[0]
    assert count == 3


def test_upsert_prices_replaces(conn):
    db.upsert_prices(conn, [("AAPL", "2026-01-01", 150.0)])
    db.upsert_prices(conn, [("AAPL", "2026-01-01", 155.0)])

    count = conn.execute("SELECT COUNT(*) FROM price").fetchone()[0]
    assert count == 1

    price = conn.execute("SELECT close FROM price WHERE ticker='AAPL'").fetchone()[0]
    assert price == 155.0


def test_upsert_rs(conn):
    records = [
        ("AAPL", "2026-01-01", 0.345, 72),
        ("SPY", "2026-01-01", 0.123, None),
    ]
    db.upsert_rs(conn, records)

    count = conn.execute("SELECT COUNT(*) FROM rs").fetchone()[0]
    assert count == 2

    spy = conn.execute("SELECT rs_rating FROM rs WHERE ticker='SPY'").fetchone()[0]
    assert spy is None


def test_get_prices_df(conn):
    records = [
        ("AAPL", "2026-01-01", 150.0),
        ("AAPL", "2026-01-02", 152.0),
        ("NVDA", "2026-01-01", 800.0),
        ("NVDA", "2026-01-02", 810.0),
    ]
    db.upsert_prices(conn, records)

    df = db.get_prices_df(conn)
    assert df.shape == (2, 2)  # 2 dates × 2 tickers
    assert "AAPL" in df.columns
    assert "NVDA" in df.columns


def test_get_latest_price_date(conn):
    assert db.get_latest_price_date(conn) is None

    db.upsert_prices(conn, [("AAPL", "2026-01-01", 150.0), ("AAPL", "2026-03-01", 160.0)])
    assert db.get_latest_price_date(conn) == "2026-03-01"


def test_meta(conn):
    assert db.get_meta(conn, "test_key") is None

    db.set_meta(conn, "test_key", "test_value")
    assert db.get_meta(conn, "test_key") == "test_value"

    db.set_meta(conn, "test_key", "updated")
    assert db.get_meta(conn, "test_key") == "updated"


def test_prune_old_prices(conn):
    records = [
        ("AAPL", "2020-01-01", 100.0),  # old, should be pruned
        ("AAPL", "2026-03-01", 200.0),   # recent, should remain
    ]
    db.upsert_prices(conn, records)

    deleted = db.prune_old_prices(conn)
    assert deleted == 1

    count = conn.execute("SELECT COUNT(*) FROM price").fetchone()[0]
    assert count == 1


def test_get_price_stats(conn):
    db.upsert_prices(conn, [("AAPL", "2026-01-01", 150.0)])
    db.upsert_rs(conn, [("AAPL", "2026-01-01", 0.3, 70)])

    stats = db.get_price_stats(conn)
    assert stats["price_rows"] == 1
    assert stats["price_tickers"] == 1
    assert stats["rs_rows"] == 1
