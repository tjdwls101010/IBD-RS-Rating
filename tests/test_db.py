"""Tests for database operations."""

from datetime import datetime, timedelta

import pytest
from ibd_rs import db
from ibd_rs.config import PRICE_RETENTION_MONTHS


@pytest.fixture
def conn():
    c = db.get_connection(":memory:")
    db.init_db(c)
    yield c
    c.close()


def _set_retention_now(monkeypatch):
    fixed_now = datetime(2026, 6, 8)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls):
            return fixed_now

    monkeypatch.setattr(db, "datetime", FixedDateTime)
    return (
        fixed_now - timedelta(days=PRICE_RETENTION_MONTHS * 30)
    ).strftime("%Y-%m-%d")


def _offset_date(date, days):
    return (
        datetime.strptime(date, "%Y-%m-%d") + timedelta(days=days)
    ).strftime("%Y-%m-%d")


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


def test_check_latest_trading_day_completeness_detects_stalled_database(conn):
    universe = [f"T{i}" for i in range(10)]
    db.upsert_prices(
        conn,
        [(ticker, "2026-04-17", 10.0) for ticker in universe],
    )
    db.upsert_prices(
        conn,
        [(ticker, "2026-05-22", 20.0) for ticker in universe[:2]],
    )

    report = db.check_latest_trading_day_completeness(
        conn,
        universe,
        threshold=0.90,
    )

    assert report["latest_date"] == "2026-05-22"
    assert report["universe_size"] == 10
    assert report["close_coverage"] == 2
    assert report["missing_close_count"] == 8
    assert report["rating_coverage"] == 0
    assert report["missing_rating_count"] == 10
    assert report["coverage_ratio"] == 0.2
    assert report["threshold"] == 0.90
    assert report["is_complete"] is False
    assert report["reason"] == "close_coverage_below_threshold"


def test_classify_latest_trading_day_completeness_threshold_boundary():
    failed = db.classify_latest_trading_day_completeness(
        latest_date="2026-05-22",
        universe_size=100,
        close_coverage=89,
        rating_coverage=0,
        threshold=0.90,
    )
    passed = db.classify_latest_trading_day_completeness(
        latest_date="2026-05-22",
        universe_size=100,
        close_coverage=90,
        rating_coverage=0,
        threshold=0.90,
    )

    assert failed["is_complete"] is False
    assert failed["missing_close_count"] == 11
    assert passed["is_complete"] is True
    assert passed["missing_close_count"] == 10


def test_check_latest_trading_day_completeness_fails_without_price_data(conn):
    report = db.check_latest_trading_day_completeness(
        conn,
        ["AAPL", "NVDA"],
        threshold=0.90,
    )

    assert report["latest_date"] is None
    assert report["universe_size"] == 2
    assert report["close_coverage"] == 0
    assert report["missing_close_count"] == 2
    assert report["coverage_ratio"] == 0.0
    assert report["is_complete"] is False
    assert report["reason"] == "no_price_data"


def test_check_latest_trading_day_completeness_fails_without_universe(conn):
    report = db.check_latest_trading_day_completeness(
        conn,
        [],
        threshold=0.90,
    )

    assert report["latest_date"] is None
    assert report["universe_size"] == 0
    assert report["is_complete"] is False
    assert report["reason"] == "universe_unknown"


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


def test_prune_old_close_clears_only_old_close_and_preserves_rs(conn, monkeypatch):
    cutoff = _set_retention_now(monkeypatch)
    old_date = _offset_date(cutoff, -1)
    recent_date = _offset_date(cutoff, 1)

    db.upsert_prices(
        conn,
        [
            ("AAPL", old_date, 150.0),
            ("AAPL", recent_date, 160.0),
            ("MSFT", old_date, 250.0),
        ],
    )
    db.upsert_rs(conn, [("AAPL", old_date, 0.345, 72)])

    pruned = db.prune_old_close(conn)

    assert pruned == 2
    old_with_rs = conn.execute(
        "SELECT close, rs_raw, rs_rating FROM rs WHERE ticker = ? AND date = ?",
        ("AAPL", old_date),
    ).fetchone()
    recent = conn.execute(
        "SELECT close, rs_raw, rs_rating FROM rs WHERE ticker = ? AND date = ?",
        ("AAPL", recent_date),
    ).fetchone()
    old_close_only = conn.execute(
        "SELECT close, rs_raw, rs_rating FROM rs WHERE ticker = ? AND date = ?",
        ("MSFT", old_date),
    ).fetchone()

    assert old_with_rs == (None, 0.345, 72)
    assert recent == (160.0, None, None)
    assert old_close_only == (None, None, None)
    assert db.get_rs_history(conn, "AAPL", 10) == [(old_date, 0.345, 72)]


def test_prune_old_close_keeps_cutoff_boundary_and_is_idempotent(conn, monkeypatch):
    cutoff = _set_retention_now(monkeypatch)
    old_date = _offset_date(cutoff, -1)
    recent_date = _offset_date(cutoff, 1)

    assert db.prune_old_close(conn) == 0

    db.upsert_prices(
        conn,
        [
            ("AAPL", cutoff, 150.0),
            ("AAPL", recent_date, 160.0),
            ("NVDA", old_date, 800.0),
        ],
    )

    assert db.prune_old_close(conn) == 1
    assert db.prune_old_close(conn) == 0

    rows = conn.execute(
        "SELECT ticker, date, close FROM rs ORDER BY ticker, date"
    ).fetchall()
    assert rows == [
        ("AAPL", cutoff, 150.0),
        ("AAPL", recent_date, 160.0),
        ("NVDA", old_date, None),
    ]
