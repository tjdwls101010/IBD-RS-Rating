"""Tests for split detection."""

import pytest

from ibd_rs import db
from ibd_rs.splits import detect_anomalous_changes


@pytest.fixture
def conn():
    c = db.get_connection(":memory:")
    db.init_db(c)
    yield c
    c.close()


def test_detect_split_like_drop(conn):
    """A 50% price drop should be flagged."""
    records = [
        ("TSLA", "2026-03-17", 200.0),
        ("TSLA", "2026-03-18", 100.0),  # 50% drop — looks like a split
    ]
    db.upsert_prices(conn, records)

    flagged = detect_anomalous_changes(conn, threshold=0.40)
    assert "TSLA" in flagged


def test_no_false_positive_normal_move(conn):
    """A 10% move should NOT be flagged."""
    records = [
        ("AAPL", "2026-03-17", 200.0),
        ("AAPL", "2026-03-18", 220.0),  # 10% up — normal
    ]
    db.upsert_prices(conn, records)

    flagged = detect_anomalous_changes(conn, threshold=0.40)
    assert "AAPL" not in flagged


def test_detect_reverse_split(conn):
    """A 100% price jump should be flagged (reverse split)."""
    records = [
        ("XYZ", "2026-03-17", 50.0),
        ("XYZ", "2026-03-18", 100.0),  # 100% jump
    ]
    db.upsert_prices(conn, records)

    flagged = detect_anomalous_changes(conn, threshold=0.40)
    assert "XYZ" in flagged


def test_no_data_returns_empty(conn):
    flagged = detect_anomalous_changes(conn)
    assert flagged == []


def test_single_day_returns_empty(conn):
    """Only one data point — can't compute change."""
    db.upsert_prices(conn, [("AAPL", "2026-03-18", 200.0)])
    flagged = detect_anomalous_changes(conn)
    assert flagged == []
