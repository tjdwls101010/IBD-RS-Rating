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


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    """Every test in this file exercises retry/inter-batch backoff paths that
    call time.sleep; none should burn real wall-clock time."""
    monkeypatch.setattr(prices.time, "sleep", lambda *_: None)


def _price_frame(data):
    return pd.DataFrame(data, index=pd.to_datetime(["2000-01-02", "2000-01-03"]))


def _dated_price_frame(data, dates):
    return pd.DataFrame(data, index=pd.to_datetime(dates))


def _set_update_today(monkeypatch, today="2026-05-22"):
    monkeypatch.setattr(prices, "_today", lambda: pd.Timestamp(today))


def _expected_trailing_start(today="2026-05-22"):
    return (
        pd.Timestamp(today) - pd.Timedelta(days=prices.TRAILING_WINDOW_DAYS)
    ).strftime("%Y-%m-%d")


def _stored_tickers(conn):
    rows = conn.execute(
        "SELECT DISTINCT ticker FROM rs WHERE close IS NOT NULL ORDER BY ticker"
    ).fetchall()
    return [row[0] for row in rows]


def _newer_price_count(conn):
    return conn.execute(
        "SELECT COUNT(*) FROM rs WHERE close IS NOT NULL AND date > '2000-01-01'"
    ).fetchone()[0]


def _stored_dates(conn, ticker):
    rows = conn.execute(
        "SELECT date FROM rs WHERE ticker = ? AND close IS NOT NULL ORDER BY date",
        (ticker,),
    ).fetchall()
    return [row[0] for row in rows]


def _ticker_price_count(conn, ticker):
    return conn.execute(
        "SELECT COUNT(*) FROM rs WHERE ticker = ? AND close IS NOT NULL",
        (ticker,),
    ).fetchone()[0]


def _multiindex_close(tickers, dates, value=10.0):
    columns = pd.MultiIndex.from_product([["Close"], tickers])
    return pd.DataFrame(value, index=pd.to_datetime(dates), columns=columns)


def test__stale_tickers_flags_ticker_missing_latest_session():
    close_df = _dated_price_frame(
        {
            "FRESH": [10.0, 11.0],
            "STALE": [20.0, np.nan],
        },
        ["2026-05-21", "2026-05-22"],
    )

    assert prices._stale_tickers(close_df) == {
        "STALE": (
            "stale: newest close 2026-05-21 "
            "before latest session 2026-05-22"
        )
    }



def test_download_batch_recovers_after_a_transient_failure(monkeypatch):
    calls = {"n": 0}
    ok_data = _multiindex_close(["A", "B"], ["2026-05-20", "2026-05-21"])

    def flaky_download(tickers, **kwargs):
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("rate limited")
        return ok_data

    monkeypatch.setattr(prices.yf, "download", flaky_download)

    result = prices._download_batch(["A", "B"], start="2026-05-19")

    assert calls["n"] == 3
    assert list(result.columns) == ["A", "B"]


def test_download_batch_raises_after_exhausting_all_retries(monkeypatch):
    def always_fail(tickers, **kwargs):
        raise RuntimeError("still rate limited")

    monkeypatch.setattr(prices.yf, "download", always_fail)

    with pytest.raises(RuntimeError, match="still rate limited"):
        prices._download_batch(["A", "B"], start="2026-05-19")


def test_download_update_stores_data_after_a_batch_level_retry(monkeypatch, conn):
    """End-to-end version of the plan's Slice 3 scenario: a rate-limit error
    once, then success, and the data ultimately gets stored."""
    _set_update_today(monkeypatch)
    calls = {"n": 0}
    ok_data = _multiindex_close(["A"], ["2026-05-22"])

    def flaky_download(tickers, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("429 rate limited")
        return ok_data

    monkeypatch.setattr(prices.yf, "download", flaky_download)

    failed = prices.download_update(["A"], conn)

    assert failed == {}
    assert calls["n"] == 2
    assert _stored_tickers(conn) == ["A"]


def test_download_update_always_writes_failed_tickers_empty_on_clean_run(
    monkeypatch,
):
    _set_update_today(monkeypatch)
    close_df = _dated_price_frame(
        {"A": [10.0, 11.0], "B": [20.0, 21.0]},
        ["2026-05-21", "2026-05-22"],
    )
    upserts = []
    meta_writes = []
    monkeypatch.setattr(prices, "_download_batch", lambda tickers, **kwargs: close_df)
    monkeypatch.setattr(
        prices.db,
        "upsert_prices",
        lambda passed_conn, records: upserts.append((passed_conn, records)),
    )
    monkeypatch.setattr(
        prices.db,
        "set_meta",
        lambda passed_conn, key, value: meta_writes.append(
            (passed_conn, key, value)
        ),
    )
    conn = object()

    failed = prices.download_update(["A", "B"], conn)

    assert failed == {}
    assert upserts and upserts[0][0] is conn
    assert meta_writes == [
        (conn, "failed_tickers", ""),
        (conn, "last_update_date", "2026-05-22"),
    ]


def test_download_update_records_stale_ticker_in_failed_tickers(
    monkeypatch, conn
):
    _set_update_today(monkeypatch)
    close_df = _dated_price_frame(
        {
            "FRESH": [10.0, 11.0],
            "STALE": [20.0, np.nan],
        },
        ["2026-05-21", "2026-05-22"],
    )
    monkeypatch.setattr(prices, "_download_batch", lambda tickers, **kwargs: close_df)

    failed = prices.download_update(["FRESH", "STALE"], conn)

    assert failed == {
        "STALE": (
            "stale: newest close 2026-05-21 "
            "before latest session 2026-05-22"
        )
    }
    assert db.get_meta(conn, "failed_tickers") == "STALE"
    assert _stored_dates(conn, "STALE") == ["2026-05-21"]



def test_download_initial_stores_all_tickers_when_all_returned(monkeypatch, conn):
    monkeypatch.setattr(
        prices,
        "_download_batch",
        lambda tickers, **kwargs: _price_frame(
            {"A": [10.0, 11.0], "B": [20.0, 21.0], "C": [30.0, 31.0]}
        ),
    )
    db.set_meta(conn, "failed_tickers", "OLD_FAILURE")

    failed = prices.download_initial(["A", "B", "C"], conn)

    assert failed == {}
    assert db.get_meta(conn, "failed_tickers") == ""
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


def test_download_update_refills_lagging_ticker_with_trailing_window(monkeypatch, conn):
    db.upsert_prices(conn, [("A", "2026-05-12", 9.0), ("B", "2026-05-21", 20.0)])
    _set_update_today(monkeypatch)
    expected_start = _expected_trailing_start()
    calls = []

    def download_batch(tickers, **kwargs):
        calls.append((tickers, kwargs))
        return _dated_price_frame(
            {"A": [10.0, 11.0, 12.0], "B": [20.1, 20.2, 20.3]},
            ["2026-05-13", "2026-05-20", "2026-05-22"],
        )

    monkeypatch.setattr(prices, "_download_batch", download_batch)

    failed = prices.download_update(["A", "B"], conn)

    assert failed == {}
    assert calls == [(["A", "B"], {"start": expected_start})]
    assert "2026-05-20" in _stored_dates(conn, "A")
    assert "2026-05-22" in _stored_dates(conn, "A")


def test_download_update_start_ignores_global_latest_price_date(monkeypatch, conn):
    db.upsert_prices(conn, [("SLOW", "2026-05-12", 9.0), ("FAST", "2026-05-22", 20.0)])
    _set_update_today(monkeypatch)
    expected_start = _expected_trailing_start()
    starts = []

    def download_batch(tickers, **kwargs):
        starts.append(kwargs["start"])
        return _dated_price_frame(
            {"SLOW": [10.0], "FAST": [20.1]},
            [expected_start],
        )

    monkeypatch.setattr(prices, "_download_batch", download_batch)

    failed = prices.download_update(["SLOW", "FAST"], conn)

    assert failed == {}
    assert starts == [expected_start]
    assert starts[0] != "2026-05-23"


def test_download_update_uses_trailing_window_on_empty_database(monkeypatch, conn):
    _set_update_today(monkeypatch)
    expected_start = _expected_trailing_start()
    starts = []

    def download_batch(tickers, **kwargs):
        starts.append(kwargs["start"])
        return _dated_price_frame({"A": [10.0]}, [expected_start])

    monkeypatch.setattr(prices, "_download_batch", download_batch)

    failed = prices.download_update(["A"], conn)

    assert failed == {}
    assert starts == [expected_start]
    assert _stored_dates(conn, "A") == [expected_start]


def test_download_update_is_idempotent_when_trailing_window_overlaps(monkeypatch, conn):
    _set_update_today(monkeypatch)
    expected_start = _expected_trailing_start()
    monkeypatch.setattr(
        prices,
        "_download_batch",
        lambda tickers, **kwargs: _dated_price_frame(
            {"A": [10.0, 11.0]},
            [expected_start, "2026-05-22"],
        ),
    )

    assert prices.download_update(["A"], conn) == {}
    assert prices.download_update(["A"], conn) == {}

    assert _ticker_price_count(conn, "A") == 2


def test_download_update_retries_same_trailing_window_after_partial_failure(monkeypatch, conn):
    _set_update_today(monkeypatch)
    expected_start = _expected_trailing_start()
    starts = []
    responses = [
        _dated_price_frame({"A": [10.0]}, [expected_start]),
        _dated_price_frame({"A": [10.0], "B": [20.0]}, [expected_start]),
    ]

    def download_batch(tickers, **kwargs):
        starts.append(kwargs["start"])
        return responses.pop(0)

    monkeypatch.setattr(prices, "_download_batch", download_batch)

    first_failed = prices.download_update(["A", "B"], conn)
    second_failed = prices.download_update(["A", "B"], conn)

    assert set(first_failed) == {"B"}
    assert second_failed == {}
    assert starts == [expected_start, expected_start]
    assert _stored_dates(conn, "B") == [expected_start]


def test_download_update_empty_ticker_list_does_not_call_download_boundary(monkeypatch, conn):
    _set_update_today(monkeypatch)

    def download_batch(tickers, **kwargs):
        raise AssertionError("empty input should not call download boundary")

    monkeypatch.setattr(prices, "_download_batch", download_batch)

    failed = prices.download_update([], conn)

    assert failed == {}
    assert conn.execute("SELECT COUNT(*) FROM rs WHERE close IS NOT NULL").fetchone()[0] == 0


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


def test_download_update_recovers_from_a_db_write_failure_on_one_batch(monkeypatch, conn):
    """A transient DB error while storing one batch (e.g. a dropped
    connection) must not crash the whole run: that batch's tickers are
    recorded as failed, and the next batch still gets stored."""
    batch1 = [f"T{i}" for i in range(prices.BATCH_SIZE)]
    batch2 = ["U0", "U1"]

    monkeypatch.setattr(
        prices, "_download_batch",
        lambda tickers, **kwargs: _price_frame({t: [10.0, 11.0] for t in tickers}),
    )

    real_upsert = db.upsert_prices
    calls = {"n": 0}

    def flaky_upsert(passed_conn, records):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("SSL connection has been closed unexpectedly")
        return real_upsert(passed_conn, records)

    monkeypatch.setattr(prices.db, "upsert_prices", flaky_upsert)

    failed = prices.download_update(batch1 + batch2, conn)

    assert set(failed) == set(batch1)
    assert _stored_tickers(conn) == sorted(batch2)


def test_download_update_marks_empty_dataframe_as_all_failed(monkeypatch, conn):
    db.upsert_prices(conn, [("A", "2000-01-01", 9.0)])
    monkeypatch.setattr(prices, "_download_batch", lambda tickers, **kwargs: pd.DataFrame())

    failed = prices.download_update(["A", "B", "C"], conn)

    assert set(failed) == {"A", "B", "C"}
    assert db.get_meta(conn, "failed_tickers") == "A,B,C"
    assert _newer_price_count(conn) == 0
