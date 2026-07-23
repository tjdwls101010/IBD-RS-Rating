"""Tests for ticker universe fetching, validation, and cache isolation."""

from datetime import date

import pytest

from ibd_rs import db
from ibd_rs import tickers
from ibd_rs.config import ANCHOR_TICKERS, SCREENER_FILTERS, UNIVERSE_FLOOR


@pytest.fixture
def conn():
    connection = db.get_connection(":memory:")
    db.init_db(connection)
    yield connection
    connection.close()


def _generated_symbols(n, prefix="Z"):
    symbols = []
    for value in range(n):
        chars = []
        current = value
        for _ in range(4):
            chars.append(chr(ord("A") + current % 26))
            current //= 26
        symbols.append(prefix + "".join(reversed(chars)))
    return symbols


def _clean_tickers(n, prefix="Z"):
    assert n >= len(ANCHOR_TICKERS)
    tickers_out = list(ANCHOR_TICKERS)
    for symbol in _generated_symbols(n, prefix):
        if symbol not in tickers_out:
            tickers_out.append(symbol)
        if len(tickers_out) == n:
            break
    return tickers_out


def _records(n, prefix="Z"):
    return [
        {"ticker": ticker, "sector": "Technology", "industry": "Software"}
        for ticker in _clean_tickers(n, prefix)
    ]


def _no_sleep(monkeypatch):
    monkeypatch.setattr(tickers.time, "sleep", lambda *_: None)


# --- _validate_universe_size: pure logic, no mocking needed ---


def test_validate_rejects_below_absolute_floor():
    ok, reason = tickers._validate_universe_size(
        UNIVERSE_FLOOR - 1, None, None, None,
    )
    assert ok is False
    assert "floor" in reason


def test_validate_accepts_at_or_above_floor_with_no_baseline():
    ok, reason = tickers._validate_universe_size(
        UNIVERSE_FLOOR, None, None, None,
    )
    assert ok is True


def test_validate_rejects_drop_below_90pct_of_last_good():
    ok, reason = tickers._validate_universe_size(
        4000, last_good_count=4600, raw_count=None, reported_total=None,
    )
    assert ok is False
    assert "last-good" in reason


def test_validate_accepts_small_drop_within_90pct_of_last_good():
    ok, reason = tickers._validate_universe_size(
        4200, last_good_count=4600, raw_count=None, reported_total=None,
    )
    assert ok is True


def test_validate_rejects_below_98pct_of_reported_total():
    ok, reason = tickers._validate_universe_size(
        4000, last_good_count=None, raw_count=4000, reported_total=4600,
    )
    assert ok is False
    assert "reported total" in reason


def test_validate_accepts_within_98pct_of_reported_total():
    ok, reason = tickers._validate_universe_size(
        4550, last_good_count=None, raw_count=4550, reported_total=4600,
    )
    assert ok is True


def test_validate_uses_raw_count_not_filtered_count_for_completeness_check():
    ok, reason = tickers._validate_universe_size(
        4658, last_good_count=None, raw_count=4955, reported_total=4960,
    )
    assert ok is True


# --- Semantic universe validation ---


def test_validate_universe_rejects_missing_anchors():
    mangled_anchors = [ticker[0] + ticker for ticker in ANCHOR_TICKERS]
    other_count = UNIVERSE_FLOOR - len(mangled_anchors)
    records = [
        {"ticker": ticker, "sector": None, "industry": None}
        for ticker in mangled_anchors + _generated_symbols(other_count, "Q")
    ]

    size_ok, _ = tickers._validate_universe_size(
        len(records), None, len(records), len(records),
    )
    ok, reason = tickers._validate_universe(records, None)

    assert size_ok is True
    assert ok is False
    assert "missing anchor" in reason
    assert "AAPL" in reason


def test_validate_universe_rejects_mass_malformed_shapes():
    records = _records(100)
    records.extend(
        {"ticker": f"BAD{i}", "sector": None, "industry": None}
        for i in range(10)
    )

    ok, reason = tickers._validate_universe(records, None)

    assert ok is False
    assert "malformed symbol fraction" in reason


def test_validate_universe_rejects_low_jaccard_vs_last_good():
    records = _records(200, prefix="Z")
    last_good = _clean_tickers(200, prefix="Y")

    ok, reason = tickers._validate_universe(records, last_good)

    assert ok is False
    assert "Jaccard" in reason


def test_validate_universe_accepts_a_clean_fetch():
    records = _records(200)
    last_good = [record["ticker"] for record in records]

    ok, reason = tickers._validate_universe(records, last_good)

    assert ok is True
    assert reason == "ok"


def test_fetch_and_validate_calls_both_guards_when_size_fails(monkeypatch):
    calls = []
    monkeypatch.setattr(
        tickers,
        "_fetch_with_retries",
        lambda: (_records(20), 20, 20),
    )
    monkeypatch.setattr(
        tickers,
        "_validate_universe_size",
        lambda *_args: calls.append("size") or (False, "bad size"),
    )
    monkeypatch.setattr(
        tickers,
        "_validate_universe",
        lambda *_args: calls.append("semantic") or (True, "ok"),
    )

    records, trusted, reason = tickers._fetch_and_validate(None)

    assert calls == ["size", "semantic"]
    assert records is None
    assert trusted is False
    assert reason == "bad size"


# --- Weekly refresh orchestration ---


def test_good_refresh_is_cached_and_advances_timestamp(monkeypatch, conn):
    monkeypatch.setattr(
        tickers,
        "_fetch_with_retries",
        lambda: (_records(UNIVERSE_FLOOR), UNIVERSE_FLOOR, UNIVERSE_FLOOR),
    )

    result, conn = tickers.refresh_universe(conn)

    assert result.trusted is True
    assert len(result.tickers) == UNIVERSE_FLOOR
    assert db.get_meta(conn, "ticker_list_date") is not None
    assert db.get_meta(conn, "last_successful_fetch") is not None
    assert len(db.get_meta(conn, "ticker_list").split(",")) == UNIVERSE_FLOOR


def test_full_count_anchor_corruption_is_rejected_and_serves_last_good(
    monkeypatch, conn,
):
    good = _clean_tickers(UNIVERSE_FLOOR, prefix="G")
    db.set_meta(conn, "ticker_list", ",".join(good))
    db.set_meta(conn, "ticker_list_date", "2000-01-01")

    mangled = [ticker[0] + ticker for ticker in ANCHOR_TICKERS]
    mangled.extend(
        _generated_symbols(UNIVERSE_FLOOR - len(mangled), prefix="Q")
    )
    records = [
        {"ticker": ticker, "sector": None, "industry": None}
        for ticker in mangled
    ]
    monkeypatch.setattr(
        tickers,
        "_fetch_with_retries",
        lambda: (records, len(records), len(records)),
    )

    result, conn = tickers.refresh_universe(conn)

    assert result.trusted is False
    assert "missing anchor" in result.reason
    assert result.tickers == good
    assert db.get_meta(conn, "ticker_list_date") == "2000-01-01"


def test_truncated_refresh_is_rejected_and_serves_last_good(monkeypatch, conn):
    good = _clean_tickers(4600, prefix="G")
    db.set_meta(conn, "ticker_list", ",".join(good))
    db.set_meta(conn, "ticker_list_date", "2000-01-01")
    monkeypatch.setattr(
        tickers,
        "_fetch_with_retries",
        lambda: (_records(55), 55, 4600),
    )

    result, conn = tickers.refresh_universe(conn)

    assert result.trusted is False
    assert "floor" in result.reason
    assert result.tickers == good
    assert db.get_meta(conn, "ticker_list_date") == "2000-01-01"


def test_fetch_failure_after_retries_serves_last_good(monkeypatch, conn):
    good = _clean_tickers(4600, prefix="G")
    db.set_meta(conn, "ticker_list", ",".join(good))
    db.set_meta(conn, "ticker_list_date", "2000-01-01")

    def always_fail():
        raise RuntimeError("blocked")

    monkeypatch.setattr(tickers, "_fetch_with_retries", always_fail)

    result, conn = tickers.refresh_universe(conn)

    assert result.trusted is False
    assert "blocked" in result.reason
    assert result.tickers == good
    assert db.get_meta(conn, "ticker_list_date") == "2000-01-01"


def test_fetch_failure_with_no_cache_falls_back_to_nasdaq_trader(
    monkeypatch, conn,
):
    def always_fail():
        raise RuntimeError("blocked")

    monkeypatch.setattr(tickers, "_fetch_with_retries", always_fail)
    monkeypatch.setattr(
        tickers.nasdaq_trader,
        "fetch_common_stock_tickers",
        lambda: [{"ticker": "AAPL", "sector": None, "industry": None}],
    )

    result, conn = tickers.refresh_universe(conn)

    assert result.trusted is False
    assert "Nasdaq Trader fallback" in result.reason
    assert result.tickers == ["AAPL"]
    assert db.get_meta(conn, "ticker_list") is None


def test_partial_refresh_below_reported_total_is_rejected(monkeypatch, conn):
    monkeypatch.setattr(
        tickers,
        "_fetch_with_retries",
        lambda: (_records(4000), 4000, 4600),
    )
    monkeypatch.setattr(
        tickers.nasdaq_trader,
        "fetch_common_stock_tickers",
        lambda: [{"ticker": "AAPL", "sector": None, "industry": None}],
    )

    result, conn = tickers.refresh_universe(conn)

    assert result.trusted is False
    assert "reported total" in result.reason


def test_trusted_refresh_uses_reconnected_connection_for_writes(
    monkeypatch, conn,
):
    fresh_conn = db.get_connection(":memory:")
    db.init_db(fresh_conn)
    monkeypatch.setattr(db, "reconnect", lambda connection: fresh_conn)
    monkeypatch.setattr(
        tickers,
        "_fetch_with_retries",
        lambda: (_records(UNIVERSE_FLOOR), UNIVERSE_FLOOR, UNIVERSE_FLOOR),
    )

    result, returned_conn = tickers.refresh_universe(conn)

    assert result.trusted is True
    assert returned_conn is fresh_conn
    assert len(db.get_meta(fresh_conn, "ticker_list").split(",")) == UNIVERSE_FLOOR
    assert db.get_meta(conn, "ticker_list") is None
    fresh_conn.close()


def test_refresh_universe_always_fetches_even_with_current_cache(monkeypatch, conn):
    good = _clean_tickers(UNIVERSE_FLOOR)
    db.set_meta(conn, "ticker_list", ",".join(good))
    db.set_meta(conn, "ticker_list_date", date.today().isoformat())
    calls = []
    monkeypatch.setattr(
        tickers,
        "_fetch_with_retries",
        lambda: calls.append("fetch")
        or (_records(UNIVERSE_FLOOR), UNIVERSE_FLOOR, UNIVERSE_FLOOR),
    )

    result, conn = tickers.refresh_universe(conn)

    assert calls == ["fetch"]
    assert result.trusted is True


# --- Daily cache-only path ---


def test_get_cached_universe_serves_valid_cache(monkeypatch, conn):
    cached = _clean_tickers(UNIVERSE_FLOOR)
    db.set_meta(conn, "ticker_list", ",".join(cached))

    monkeypatch.setattr(
        tickers.finviz,
        "fetch_screener_records",
        lambda *_args, **_kwargs: pytest.fail("daily cache read must not call Finviz"),
    )
    monkeypatch.setattr(
        tickers.nasdaq_trader,
        "fetch_common_stock_tickers",
        lambda: pytest.fail("valid cache must not use fallback"),
    )

    result, returned_conn = tickers.get_cached_universe(conn)

    assert returned_conn is conn
    assert result.trusted is True
    assert result.tickers == cached
    assert result.reason == "cache-valid"


def test_get_cached_universe_rejects_poisoned_cache(monkeypatch, conn):
    poisoned = [ticker[0] + ticker for ticker in ANCHOR_TICKERS] + _generated_symbols(UNIVERSE_FLOOR)
    db.set_meta(conn, "ticker_list", ",".join(poisoned))
    monkeypatch.setattr(
        tickers.nasdaq_trader,
        "fetch_common_stock_tickers",
        lambda: [{"ticker": "AAPL", "sector": None, "industry": None}],
    )

    result, conn = tickers.get_cached_universe(conn)

    assert result.trusted is False
    assert "cached universe invalid" in result.reason
    assert "missing anchor" in result.reason
    assert result.tickers == ["AAPL"]
    assert db.get_meta(conn, "ticker_list") == ",".join(poisoned)


def test_get_cached_universe_rejects_undersized_cache(monkeypatch, conn):
    """A cache containing the anchors but below UNIVERSE_FLOOR must NOT be
    trusted on the daily path (it would also poison the refresh Jaccard
    baseline). It routes to the untrusted Nasdaq fallback, cache untouched."""
    undersized = _clean_tickers(len(ANCHOR_TICKERS) + 20)  # anchors present, << floor
    db.set_meta(conn, "ticker_list", ",".join(undersized))
    monkeypatch.setattr(
        tickers.nasdaq_trader,
        "fetch_common_stock_tickers",
        lambda: [{"ticker": "AAPL", "sector": None, "industry": None}],
    )

    result, conn = tickers.get_cached_universe(conn)

    assert result.trusted is False
    assert "below floor" in result.reason
    assert db.get_meta(conn, "ticker_list") == ",".join(undersized)


def test_refresh_universe_ignores_undersized_cache_as_baseline(monkeypatch, conn):
    """A clean full refresh must HEAL an undersized/poisoned cache rather than
    being Jaccard-rejected against it: the invalid cache is not used as the
    day-over-day baseline."""
    undersized = _clean_tickers(len(ANCHOR_TICKERS) + 5)
    db.set_meta(conn, "ticker_list", ",".join(undersized))
    clean = _records(UNIVERSE_FLOOR)
    monkeypatch.setattr(
        tickers,
        "_fetch_with_retries",
        lambda: (clean, UNIVERSE_FLOOR, UNIVERSE_FLOOR),
    )

    result, conn = tickers.refresh_universe(conn)

    assert result.trusted is True  # not Jaccard-deadlocked against the tiny cache
    healed = db.get_meta(conn, "ticker_list").split(",")
    assert len(healed) == UNIVERSE_FLOOR
    assert set(ANCHOR_TICKERS).issubset(healed)


def test_get_cached_universe_with_no_cache_uses_untrusted_fallback(
    monkeypatch, conn,
):
    monkeypatch.setattr(
        tickers.nasdaq_trader,
        "fetch_common_stock_tickers",
        lambda: [{"ticker": "MSFT", "sector": None, "industry": None}],
    )

    result, conn = tickers.get_cached_universe(conn)

    assert result.trusted is False
    assert "no cached universe" in result.reason
    assert result.tickers == ["MSFT"]


# --- Owned Finviz integration boundary ---


def test_fetch_universe_attempt_excludes_etfs_and_adds_reference_tickers(
    monkeypatch,
):
    seen_filters = []
    fetched = [
        {
            "ticker": "AAPL",
            "sector": "Technology",
            "industry": "Consumer Electronics",
        },
        {
            "ticker": "FAKE",
            "sector": None,
            "industry": "Exchange Traded Fund",
        },
        {"ticker": "MSFT", "sector": "Technology", "industry": "Software"},
    ]
    monkeypatch.setattr(
        tickers.finviz,
        "fetch_screener_records",
        lambda filters: seen_filters.append(filters) or (fetched, 3, 3),
    )

    records, raw_count, reported_total = tickers._fetch_universe_attempt()

    assert seen_filters == [SCREENER_FILTERS]
    assert {record["ticker"] for record in records} == {
        "AAPL", "MSFT", "SPY", "QQQ",
    }
    assert raw_count == 3
    assert reported_total == 3
    assert records == sorted(records, key=lambda record: record["ticker"])


def test_fetch_universe_attempt_raw_count_is_unaffected_by_exclusions(
    monkeypatch,
):
    fetched = [
        {
            "ticker": "AAPL",
            "sector": "Technology",
            "industry": "Consumer Electronics",
        },
        {"ticker": "ETF1", "sector": None, "industry": "Exchange Traded Fund"},
        {"ticker": "ETF2", "sector": None, "industry": "Exchange Traded Fund"},
    ]
    monkeypatch.setattr(
        tickers.finviz,
        "fetch_screener_records",
        lambda _filters: (fetched, 3, 3),
    )

    records, raw_count, reported_total = tickers._fetch_universe_attempt()

    assert raw_count == 3
    assert {record["ticker"] for record in records} == {"AAPL", "SPY", "QQQ"}


def test_fetch_universe_attempt_propagates_parser_failure(monkeypatch):
    def fail(_filters):
        raise RuntimeError("no results")

    monkeypatch.setattr(tickers.finviz, "fetch_screener_records", fail)

    with pytest.raises(RuntimeError, match="no results"):
        tickers._fetch_universe_attempt()


# --- Retry/backoff behavior ---


def test_fetch_with_retries_recovers_after_transient_failures(monkeypatch):
    _no_sleep(monkeypatch)
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("rate limited")
        return _records(UNIVERSE_FLOOR), UNIVERSE_FLOOR, UNIVERSE_FLOOR

    monkeypatch.setattr(tickers, "_fetch_universe_attempt", flaky)

    records, raw_count, reported_total = tickers._fetch_with_retries()

    assert calls["n"] == 3
    assert len(records) == UNIVERSE_FLOOR


def test_fetch_with_retries_raises_after_exhausting_all_attempts(monkeypatch):
    _no_sleep(monkeypatch)

    def always_fail():
        raise RuntimeError("blocked")

    monkeypatch.setattr(tickers, "_fetch_universe_attempt", always_fail)

    with pytest.raises(RuntimeError, match="blocked"):
        tickers._fetch_with_retries()


# --- Weekly enrichment corruption-resistance (D12) ---


def test_refresh_universe_does_not_upsert_sector_on_untrusted_fetch(monkeypatch, conn):
    """Corruption-resistant enrichment: a validity-failing fetch (e.g. the logo
    corruption where the anchors are mangled) must NOT write sector/industry
    rows, so the weekly job can never poison the tickers table (D12)."""
    db.set_meta(conn, "ticker_list", ",".join(_clean_tickers(UNIVERSE_FLOOR)))
    corrupt = [
        {"ticker": ticker[0] + ticker, "sector": "Junk", "industry": "Junk"}
        for ticker in ANCHOR_TICKERS
    ] + [
        {"ticker": symbol, "sector": "Junk", "industry": "Junk"}
        for symbol in _generated_symbols(UNIVERSE_FLOOR)
    ]
    monkeypatch.setattr(
        tickers,
        "_fetch_with_retries",
        lambda: (corrupt, len(corrupt), len(corrupt)),
    )
    upserts = []
    monkeypatch.setattr(
        tickers.db, "upsert_tickers", lambda c, records: upserts.append(records)
    )

    result, conn = tickers.refresh_universe(conn)

    assert result.trusted is False
    assert upserts == []  # a corrupt fetch never writes sector/industry rows
