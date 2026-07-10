"""Tests for ticker universe fetch hardening (docs/plans/2026-07-11-...-reliability.md Slice 1)."""

import pandas as pd
import pytest

from ibd_rs import db
from ibd_rs import tickers
from ibd_rs.config import UNIVERSE_FLOOR


@pytest.fixture
def conn():
    c = db.get_connection(":memory:")
    db.init_db(c)
    yield c
    c.close()


def _records(n, prefix="T"):
    return [{"ticker": f"{prefix}{i}", "sector": "Tech", "industry": "Software"} for i in range(n)]


def _no_sleep(monkeypatch):
    monkeypatch.setattr(tickers.time, "sleep", lambda *_: None)


# --- _validate_universe_size: pure logic, no mocking needed ---

def test_validate_rejects_below_absolute_floor():
    ok, reason = tickers._validate_universe_size(UNIVERSE_FLOOR - 1, None, None)
    assert ok is False
    assert "floor" in reason


def test_validate_accepts_at_or_above_floor_with_no_baseline():
    ok, reason = tickers._validate_universe_size(UNIVERSE_FLOOR, None, None)
    assert ok is True


def test_validate_rejects_drop_below_90pct_of_last_good():
    ok, reason = tickers._validate_universe_size(4000, last_good_count=4600, reported_total=None)
    assert ok is False
    assert "last-good" in reason


def test_validate_accepts_small_drop_within_90pct_of_last_good():
    ok, reason = tickers._validate_universe_size(4200, last_good_count=4600, reported_total=None)
    assert ok is True


def test_validate_rejects_below_98pct_of_reported_total():
    ok, reason = tickers._validate_universe_size(4000, last_good_count=None, reported_total=4600)
    assert ok is False
    assert "reported total" in reason


def test_validate_accepts_within_98pct_of_reported_total():
    ok, reason = tickers._validate_universe_size(4550, last_good_count=None, reported_total=4600)
    assert ok is True


# --- fetch_ticker_list orchestration: mock at the _fetch_with_retries seam ---

def test_good_fetch_is_cached_and_advances_timestamp(monkeypatch, conn):
    monkeypatch.setattr(tickers, "_fetch_with_retries", lambda: (_records(UNIVERSE_FLOOR), UNIVERSE_FLOOR))

    result = tickers.fetch_ticker_list(conn)

    assert result.trusted is True
    assert len(result.tickers) == UNIVERSE_FLOOR
    assert db.get_meta(conn, "ticker_list_date") is not None
    assert db.get_meta(conn, "last_successful_fetch") is not None
    assert len(db.get_meta(conn, "ticker_list").split(",")) == UNIVERSE_FLOOR


def test_truncated_fetch_is_rejected_and_serves_last_good(monkeypatch, conn):
    # Seed a last-good cache (simulating a prior healthy fetch), far enough in the
    # past that it would normally be stale, so the code path exercises a fresh attempt.
    good = _records(4600, prefix="G")
    db.set_meta(conn, "ticker_list", ",".join(r["ticker"] for r in good))
    db.set_meta(conn, "ticker_list_date", "2000-01-01")

    # Reproduces the 2026-07-08 incident: Finviz returns 55 tickers.
    monkeypatch.setattr(tickers, "_fetch_with_retries", lambda: (_records(55), 4600))

    result = tickers.fetch_ticker_list(conn)

    assert result.trusted is False
    assert "floor" in result.reason
    assert len(result.tickers) == 4600  # served last-good, not the truncated 55
    # The bad fetch must NOT have overwritten the cache.
    assert db.get_meta(conn, "ticker_list_date") == "2000-01-01"


def test_fetch_failure_after_retries_serves_last_good(monkeypatch, conn):
    good = _records(4600, prefix="G")
    db.set_meta(conn, "ticker_list", ",".join(r["ticker"] for r in good))
    db.set_meta(conn, "ticker_list_date", "2000-01-01")

    def always_fail():
        raise RuntimeError("blocked")

    monkeypatch.setattr(tickers, "_fetch_with_retries", always_fail)

    result = tickers.fetch_ticker_list(conn)

    assert result.trusted is False
    assert "blocked" in result.reason
    assert len(result.tickers) == 4600
    assert db.get_meta(conn, "ticker_list_date") == "2000-01-01"


def test_fetch_failure_with_no_cache_falls_back_to_nasdaq_trader(monkeypatch, conn):
    def always_fail():
        raise RuntimeError("blocked")

    monkeypatch.setattr(tickers, "_fetch_with_retries", always_fail)
    monkeypatch.setattr(
        tickers.nasdaq_trader, "fetch_common_stock_tickers",
        lambda: [{"ticker": "AAPL", "sector": None, "industry": None}],
    )

    result = tickers.fetch_ticker_list(conn)

    assert result.trusted is False
    assert "Nasdaq Trader fallback" in result.reason
    assert result.tickers == ["AAPL"]
    assert db.get_meta(conn, "ticker_list") is None  # fallback is never cached as the new baseline


def test_partial_fetch_below_completeness_ratio_is_rejected_as_truncated(monkeypatch, conn):
    # A modest last-good baseline means the 90% drop-guard alone would pass
    # a 4000-ticker fetch, but Finviz itself reports 4600 are available --
    # a plausible-looking but truncated fetch that only the completeness
    # cross-check against Finviz's own total catches.
    good = _records(3000, prefix="G")
    db.set_meta(conn, "ticker_list", ",".join(r["ticker"] for r in good))
    db.set_meta(conn, "ticker_list_date", "2000-01-01")

    monkeypatch.setattr(tickers, "_fetch_with_retries", lambda: (_records(4000), 4600))

    result = tickers.fetch_ticker_list(conn)

    assert result.trusted is False
    assert "reported total" in result.reason


def test_fresh_cache_is_reused_without_a_new_fetch(monkeypatch, conn):
    db.set_meta(conn, "ticker_list", "AAPL,MSFT")
    from datetime import date
    db.set_meta(conn, "ticker_list_date", date.today().isoformat())

    def fail_if_called():
        raise AssertionError("should not fetch when cache is fresh")

    monkeypatch.setattr(tickers, "_fetch_with_retries", fail_if_called)

    result = tickers.fetch_ticker_list(conn)

    assert result.trusted is True
    assert result.tickers == ["AAPL", "MSFT"]


def test_force_refresh_bypasses_a_fresh_cache(monkeypatch, conn):
    db.set_meta(conn, "ticker_list", "AAPL,MSFT")
    from datetime import date
    db.set_meta(conn, "ticker_list_date", date.today().isoformat())

    monkeypatch.setattr(tickers, "_fetch_with_retries", lambda: (_records(UNIVERSE_FLOOR), UNIVERSE_FLOOR))

    result = tickers.fetch_ticker_list(conn, force_refresh=True)

    assert result.trusted is True
    assert len(result.tickers) == UNIVERSE_FLOOR


# --- _fetch_universe_attempt: the finvizfinance integration boundary ---

class _FakeOverview:
    url = "https://finviz.com/screener.ashx"
    size = 20
    request_params = {}

    def __init__(self, df):
        self._df = df

    def set_filter(self, filters_dict):
        self.filters_dict = filters_dict

    def screener_view(self, verbose=0):
        return self._df


def test_fetch_universe_attempt_excludes_etfs_and_adds_reference_tickers(monkeypatch):
    df = pd.DataFrame([
        {"Ticker": "AAPL", "Sector": "Technology", "Industry": "Consumer Electronics"},
        {"Ticker": "FAKE", "Sector": "", "Industry": "Exchange Traded Fund"},
        {"Ticker": "MSFT", "Sector": "Technology", "Industry": "Software"},
    ])
    monkeypatch.setattr(tickers, "Overview", lambda: _FakeOverview(df))
    monkeypatch.setattr(tickers, "_reported_total", lambda foverview: 3)

    records, reported_total = tickers._fetch_universe_attempt()

    tickers_out = {r["ticker"] for r in records}
    assert tickers_out == {"AAPL", "MSFT", "SPY", "QQQ"}  # FAKE excluded; reference tickers added
    assert reported_total == 3
    assert records == sorted(records, key=lambda r: r["ticker"])


def test_fetch_universe_attempt_raises_when_screener_returns_none(monkeypatch):
    monkeypatch.setattr(tickers, "Overview", lambda: _FakeOverview(None))
    monkeypatch.setattr(tickers, "_reported_total", lambda foverview: None)

    with pytest.raises(RuntimeError):
        tickers._fetch_universe_attempt()


# --- retry/backoff behavior ---

def test_fetch_with_retries_recovers_after_transient_failures(monkeypatch):
    _no_sleep(monkeypatch)
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("rate limited")
        return (_records(UNIVERSE_FLOOR), UNIVERSE_FLOOR)

    monkeypatch.setattr(tickers, "_fetch_universe_attempt", flaky)

    records, reported_total = tickers._fetch_with_retries()

    assert calls["n"] == 3
    assert len(records) == UNIVERSE_FLOOR


def test_fetch_with_retries_raises_after_exhausting_all_attempts(monkeypatch):
    _no_sleep(monkeypatch)

    def always_fail():
        raise RuntimeError("blocked")

    monkeypatch.setattr(tickers, "_fetch_universe_attempt", always_fail)

    with pytest.raises(RuntimeError, match="blocked"):
        tickers._fetch_with_retries()
