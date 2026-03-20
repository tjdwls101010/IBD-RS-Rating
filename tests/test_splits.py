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
    db.upsert_prices(conn, [
        ("TSLA", "2026-03-17", 200.0),
        ("TSLA", "2026-03-18", 100.0),
    ])
    flagged = detect_anomalous_changes(conn, threshold=0.40)
    assert "TSLA" in flagged


def test_no_false_positive_normal_move(conn):
    db.upsert_prices(conn, [
        ("AAPL", "2026-03-17", 200.0),
        ("AAPL", "2026-03-18", 220.0),
    ])
    flagged = detect_anomalous_changes(conn, threshold=0.40)
    assert "AAPL" not in flagged


def test_detect_reverse_split(conn):
    db.upsert_prices(conn, [
        ("XYZ", "2026-03-17", 50.0),
        ("XYZ", "2026-03-18", 100.0),
    ])
    flagged = detect_anomalous_changes(conn, threshold=0.40)
    assert "XYZ" in flagged


def test_no_data_returns_empty(conn):
    assert detect_anomalous_changes(conn) == []


def test_single_day_returns_empty(conn):
    db.upsert_prices(conn, [("AAPL", "2026-03-18", 200.0)])
    assert detect_anomalous_changes(conn) == []
