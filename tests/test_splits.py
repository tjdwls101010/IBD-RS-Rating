"""Tests for split detection and repair."""

from datetime import datetime, timedelta
from types import SimpleNamespace

import pandas as pd
import pytest

from ibd_rs import db, splits
from ibd_rs.config import PRICE_RETENTION_MONTHS, SPLIT_REPAIR_MAX_TICKERS
from ibd_rs.splits import detect_anomalous_changes, verify_and_repair


@pytest.fixture
def conn():
    c = db.get_connection(":memory:")
    db.init_db(c)
    yield c
    c.close()


def _recent_splits():
    split_date = pd.Timestamp("2026-07-23", tz="UTC")
    return pd.Series([2.0], index=pd.DatetimeIndex([split_date]))


def _empty_splits():
    return pd.Series(dtype=float, index=pd.DatetimeIndex([], tz="UTC"))


def _freeze_split_clock(monkeypatch):
    fixed_now = datetime(2026, 7, 23, 12, 0, 0)

    class FixedDateTime:
        @classmethod
        def now(cls):
            return fixed_now

    monkeypatch.setattr(splits, "datetime", FixedDateTime)
    return fixed_now


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


def test_detect_catches_auto_adjust_seam_outside_the_old_7day_window(conn):
    end = pd.Timestamp("2026-03-18")
    dates = pd.bdate_range(end=end, periods=15)
    closes = [200.0] * 4 + [100.0] * 11
    db.upsert_prices(conn, [
        ("SEAM", date.strftime("%Y-%m-%d"), close)
        for date, close in zip(dates, closes)
    ])

    last_seven = [
        close
        for date, close in zip(dates, closes)
        if date >= end - pd.Timedelta(days=7)
    ]
    # The old seven-calendar-day scan saw only this smooth, adjusted segment.
    assert last_seven
    assert set(last_seven) == {100.0}
    db.upsert_prices(conn, [
        ("SMOOTH7", date.strftime("%Y-%m-%d"), close)
        for date, close in zip(dates, closes)
        if date >= end - pd.Timedelta(days=7)
    ])

    flagged = detect_anomalous_changes(conn, threshold=0.40)

    assert "SEAM" in flagged
    assert "SMOOTH7" not in flagged


def test_detect_clamps_future_dated_anchor_to_today(conn):
    today = pd.Timestamp.now().normalize()
    db.upsert_prices(conn, [
        ("ANCHOR", (today - pd.Timedelta(days=1)).strftime("%Y-%m-%d"), 200.0),
        ("ANCHOR", today.strftime("%Y-%m-%d"), 100.0),
        ("FUTURE", (today + pd.Timedelta(days=30)).strftime("%Y-%m-%d"), 50.0),
    ])

    flagged = detect_anomalous_changes(conn, threshold=0.40)

    assert "ANCHOR" in flagged
    assert "FUTURE" not in flagged


def test_verify_and_repair_bounds_download_to_retention_not_2y(conn, monkeypatch):
    fixed_now = _freeze_split_clock(monkeypatch)
    monkeypatch.setattr(
        splits.yf,
        "Ticker",
        lambda ticker: SimpleNamespace(splits=_recent_splits()),
    )
    calls = []

    def fake_download(tickers, **kwargs):
        calls.append((tickers, kwargs))
        return pd.DataFrame(
            {tickers[0]: [123.0]},
            index=pd.DatetimeIndex([pd.Timestamp.now().normalize()]),
        )

    monkeypatch.setattr(splits.prices, "_download_batch", fake_download)

    repaired = verify_and_repair(conn, ["AAPL"])

    retention_start = (
        fixed_now - timedelta(days=PRICE_RETENTION_MONTHS * 30)
    ).strftime("%Y-%m-%d")
    assert repaired == ["AAPL"]
    assert calls == [(["AAPL"], {"start": retention_start})]
    assert "period" not in calls[0][1]


def test_verify_and_repair_does_not_write_future_dated_rows(conn, monkeypatch):
    _freeze_split_clock(monkeypatch)
    monkeypatch.setattr(
        splits.yf,
        "Ticker",
        lambda ticker: SimpleNamespace(splits=_recent_splits()),
    )
    today = pd.Timestamp.now().normalize()
    close_df = pd.DataFrame(
        {"AAPL": [123.0, 456.0]},
        index=pd.DatetimeIndex([today, today + pd.Timedelta(days=5)]),
    )
    monkeypatch.setattr(
        splits.prices,
        "_download_batch",
        lambda tickers, **kwargs: close_df,
    )
    written = []
    monkeypatch.setattr(
        splits.db,
        "upsert_prices",
        lambda connection, records: written.extend(records),
    )

    repaired = verify_and_repair(conn, ["AAPL"])

    assert repaired == ["AAPL"]
    assert written == [("AAPL", today.strftime("%Y-%m-%d"), 123.0)]
    assert all(date <= today.strftime("%Y-%m-%d") for _, date, _ in written)


def test_verify_and_repair_caps_tickers_per_run(conn, monkeypatch):
    flagged = [
        f"TICKER{i}"
        for i in range(SPLIT_REPAIR_MAX_TICKERS + 3)
    ]
    checked = []

    def fake_ticker(ticker):
        checked.append(ticker)
        return SimpleNamespace(splits=_empty_splits())

    monkeypatch.setattr(splits.yf, "Ticker", fake_ticker)

    assert verify_and_repair(conn, flagged) == []
    assert checked == flagged[:SPLIT_REPAIR_MAX_TICKERS]
    assert len(checked) <= SPLIT_REPAIR_MAX_TICKERS


def test_verify_and_repair_no_confirmed_split_writes_nothing(conn, monkeypatch):
    monkeypatch.setattr(
        splits.yf,
        "Ticker",
        lambda ticker: SimpleNamespace(splits=_empty_splits()),
    )

    def unexpected_download(tickers, **kwargs):
        raise AssertionError("download must not run without a confirmed split")

    monkeypatch.setattr(splits.prices, "_download_batch", unexpected_download)

    assert verify_and_repair(conn, ["AAPL"]) == []
