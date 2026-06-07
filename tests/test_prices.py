"""Tests for price download failure detection."""

import numpy as np
import pandas as pd
import pytest

from ibd_rs import db
from ibd_rs import prices


@pytest.fixture
def conn():
    c = db.get_connection(":memory:")
    db.init_db(c)
    yield c
    c.close()


def _price_frame(data):
    return pd.DataFrame(data, index=pd.to_datetime(["2000-01-02", "2000-01-03"]))


def _stored_tickers(conn):
    rows = conn.execute(
        "SELECT DISTINCT ticker FROM rs WHERE close IS NOT NULL ORDER BY ticker"
    ).fetchall()
    return [row[0] for row in rows]


def _newer_price_count(conn):
    return conn.execute(
        "SELECT COUNT(*) FROM rs WHERE close IS NOT NULL AND date > '2000-01-01'"
    ).fetchone()[0]


def test_download_initial_stores_all_tickers_when_all_returned(monkeypatch, conn):
    monkeypatch.setattr(
        prices,
        "_download_batch",
        lambda tickers, **kwargs: _price_frame(
            {"A": [10.0, 11.0], "B": [20.0, 21.0], "C": [30.0, 31.0]}
        ),
    )

    failed = prices.download_initial(["A", "B", "C"], conn)

    assert failed == {}
    assert db.get_meta(conn, "failed_tickers") is None
    assert _stored_tickers(conn) == ["A", "B", "C"]


def test_download_initial_detects_ticker_missing_from_returned_columns(monkeypatch, conn):
    monkeypatch.setattr(
        prices,
        "_download_batch",
        lambda tickers, **kwargs: _price_frame({"A": [10.0, 11.0], "B": [20.0, 21.0]}),
    )

    failed = prices.download_initial(["A", "B", "C"], conn)

    assert set(failed) == {"C"}
    assert db.get_meta(conn, "failed_tickers") == "C"
    assert _stored_tickers(conn) == ["A", "B"]


def test_download_initial_treats_all_nan_column_as_not_returned(monkeypatch, conn):
    monkeypatch.setattr(
        prices,
        "_download_batch",
        lambda tickers, **kwargs: _price_frame(
            {"A": [10.0, 11.0], "B": [20.0, 21.0], "C": [np.nan, np.nan]}
        ),
    )

    failed = prices.download_initial(["A", "B", "C"], conn)

    assert set(failed) == {"C"}
    assert db.get_meta(conn, "failed_tickers") == "C"
    assert _stored_tickers(conn) == ["A", "B"]


def test_download_initial_does_not_depend_on_yfinance_shared(monkeypatch, conn):
    monkeypatch.delattr(prices.yf, "shared", raising=False)
    monkeypatch.setattr(
        prices,
        "_download_batch",
        lambda tickers, **kwargs: _price_frame({"A": [10.0, 11.0], "B": [20.0, 21.0]}),
    )

    failed = prices.download_initial(["A", "B", "C"], conn)

    assert set(failed) == {"C"}
    assert db.get_meta(conn, "failed_tickers") == "C"
    assert _stored_tickers(conn) == ["A", "B"]


def test_download_initial_marks_whole_batch_failed_when_download_raises(monkeypatch, conn):
    def raise_error(tickers, **kwargs):
        raise RuntimeError("rate-limit after retry")

    monkeypatch.setattr(prices, "_download_batch", raise_error)

    failed = prices.download_initial(["A", "B", "C"], conn)

    assert set(failed) == {"A", "B", "C"}
    assert db.get_meta(conn, "failed_tickers") == "A,B,C"
    assert _stored_tickers(conn) == []


def test_download_initial_marks_empty_dataframe_as_all_failed(monkeypatch, conn):
    monkeypatch.setattr(prices, "_download_batch", lambda tickers, **kwargs: pd.DataFrame())

    failed = prices.download_initial(["A", "B", "C"], conn)

    assert set(failed) == {"A", "B", "C"}
    assert db.get_meta(conn, "failed_tickers") == "A,B,C"
    assert _stored_tickers(conn) == []


def test_download_update_uses_return_coverage_for_failed_tickers(monkeypatch, conn):
    db.upsert_prices(conn, [("A", "2000-01-01", 9.0)])
    monkeypatch.setattr(
        prices,
        "_download_batch",
        lambda tickers, **kwargs: _price_frame({"A": [10.0, 11.0], "B": [20.0, 21.0]}),
    )

    failed = prices.download_update(["A", "B", "C"], conn)

    assert set(failed) == {"C"}
    assert db.get_meta(conn, "failed_tickers") == "C"
    assert _stored_tickers(conn) == ["A", "B"]


def test_download_update_marks_whole_batch_failed_when_download_raises(monkeypatch, conn):
    db.upsert_prices(conn, [("A", "2000-01-01", 9.0)])

    def raise_error(tickers, **kwargs):
        raise RuntimeError("rate-limit after retry")

    monkeypatch.setattr(prices, "_download_batch", raise_error)

    failed = prices.download_update(["A", "B", "C"], conn)

    assert set(failed) == {"A", "B", "C"}
    assert db.get_meta(conn, "failed_tickers") == "A,B,C"
    assert _newer_price_count(conn) == 0


def test_download_update_marks_empty_dataframe_as_all_failed(monkeypatch, conn):
    db.upsert_prices(conn, [("A", "2000-01-01", 9.0)])
    monkeypatch.setattr(prices, "_download_batch", lambda tickers, **kwargs: pd.DataFrame())

    failed = prices.download_update(["A", "B", "C"], conn)

    assert set(failed) == {"A", "B", "C"}
    assert db.get_meta(conn, "failed_tickers") == "A,B,C"
    assert _newer_price_count(conn) == 0
