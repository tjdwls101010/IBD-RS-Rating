"""Tests for database operations."""

import pytest
from ibd_rs import db


@pytest.fixture
def conn():
    c = db.get_connection(":memory:")
    db.init_db(c)
    yield c
    c.close()


def test_init_db_creates_tables(conn):
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    names = [t[0] for t in tables]
    assert "rs" in names
    assert "tickers" in names
    assert "meta" in names


def test_init_db_idempotent(conn):
    db.init_db(conn)


def test_upsert_prices(conn):
    records = [
        ("AAPL", "2026-01-01", 150.0),
        ("AAPL", "2026-01-02", 152.0),
        ("NVDA", "2026-01-01", 800.0),
    ]
    db.upsert_prices(conn, records)
    count = conn.execute("SELECT COUNT(*) FROM rs WHERE close IS NOT NULL").fetchone()[0]
    assert count == 3


def test_upsert_prices_does_not_overwrite_rs(conn):
    """Upserting prices should not overwrite existing rs_raw/rs_rating."""
    db.upsert_rs(conn, [("AAPL", "2026-01-01", 0.345, 72)])
    db.upsert_prices(conn, [("AAPL", "2026-01-01", 155.0)])

    row = conn.execute(
        "SELECT close, rs_raw, rs_rating FROM rs WHERE ticker='AAPL' AND date='2026-01-01'"
    ).fetchone()
    assert row[0] == 155.0  # close updated
    assert row[1] == 0.345  # rs_raw preserved
    assert row[2] == 72     # rs_rating preserved


def test_upsert_rs(conn):
    records = [
        ("AAPL", "2026-01-01", 0.345, 72),
        ("SPY", "2026-01-01", 0.123, 55),
    ]
    db.upsert_rs(conn, records)
    count = conn.execute("SELECT COUNT(*) FROM rs").fetchone()[0]
    assert count == 2


def test_upsert_tickers(conn):
    records = [
        ("AAPL", "Technology", "Consumer Electronics"),
        ("NVDA", "Technology", "Semiconductors"),
        ("LLY", "Healthcare", "Drug Manufacturers"),
    ]
    db.upsert_tickers(conn, records)
    count = conn.execute("SELECT COUNT(*) FROM tickers").fetchone()[0]
    assert count == 3

    row = conn.execute("SELECT sector, industry FROM tickers WHERE ticker='NVDA'").fetchone()
    assert row[0] == "Technology"
    assert row[1] == "Semiconductors"


def test_get_prices_df(conn):
    db.upsert_prices(conn, [
        ("AAPL", "2026-01-01", 150.0),
        ("AAPL", "2026-01-02", 152.0),
        ("NVDA", "2026-01-01", 800.0),
        ("NVDA", "2026-01-02", 810.0),
    ])
    df = db.get_prices_df(conn)
    assert df.shape == (2, 2)
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


def test_get_price_stats(conn):
    db.upsert_prices(conn, [("AAPL", "2026-01-01", 150.0)])
    db.upsert_rs(conn, [("AAPL", "2026-01-01", 0.3, 70)])
    stats = db.get_price_stats(conn)
    assert stats["price_rows"] == 1
    assert stats["price_tickers"] == 1
    assert stats["rs_rows"] == 1
