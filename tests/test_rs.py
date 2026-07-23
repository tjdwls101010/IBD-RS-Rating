"""Tests for RS calculation engine."""

import struct
import numpy as np
import pandas as pd
import pytest

from ibd_rs import db, rs as rs_module
from ibd_rs.config import PRICE_RETENTION_MONTHS, RS_RECOMPUTE_WINDOW_DAYS, RS_WEIGHTS
from ibd_rs.rs import backfill_ratings, calculate_and_store, compute_rs_raw, compute_rs_rating


def _make_price_df(n_tickers=10, n_days=300, seed=42):
    """Create synthetic price data with known trends."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2024-01-01", periods=n_days)
    tickers = [f"T{i:03d}" for i in range(n_tickers)]

    # Each ticker gets a slightly different drift
    data = {}
    for i, ticker in enumerate(tickers):
        drift = 0.0002 * (i + 1)  # higher-numbered tickers trend up more
        returns = rng.normal(drift, 0.02, n_days)
        prices = 100 * np.cumprod(1 + returns)
        data[ticker] = prices

    return pd.DataFrame(data, index=dates)


def _manual_rs_raw_from_valid_prices(prices):
    """Compute the expected last RS Raw value on a non-NaN ticker series."""
    return (
        0.4 * (prices.iloc[-1] / prices.iloc[-64] - 1)
        + 0.2 * (prices.iloc[-1] / prices.iloc[-127] - 1)
        + 0.2 * (prices.iloc[-1] / prices.iloc[-190] - 1)
        + 0.2 * (prices.iloc[-1] / prices.iloc[-253] - 1)
    )


@pytest.fixture
def conn():
    c = db.get_connection(":memory:")
    db.init_db(c)
    yield c
    c.close()


@pytest.fixture(autouse=True)
def _neutralize_universe_floor(monkeypatch):
    """Small synthetic fixtures here would be swamped by the production
    UNIVERSE_FLOOR (3000). Neutralize it to 1 by default so the ranking/gate
    logic is exercised directly; floor-specific tests override it locally."""
    monkeypatch.setattr("ibd_rs.rs.UNIVERSE_FLOOR", 1)


def test_rs_raw_shape():
    price_df = _make_price_df(n_tickers=5, n_days=300)
    rs_raw = compute_rs_raw(price_df)
    assert rs_raw.shape == price_df.shape


def test_rs_raw_first_252_are_nan():
    price_df = _make_price_df(n_tickers=3, n_days=300)
    rs_raw = compute_rs_raw(price_df)
    # First 252 rows should have NaN (insufficient lookback)
    assert rs_raw.iloc[:252].isna().all().all()
    # After 252, should have valid values
    assert rs_raw.iloc[252:].notna().any().any()


def test_rs_raw_manual_calculation():
    """Verify RS Raw formula against manual computation for a single ticker."""
    price_df = _make_price_df(n_tickers=1, n_days=300)
    rs_raw = compute_rs_raw(price_df)

    ticker = price_df.columns[0]
    last_row = rs_raw.iloc[-1]

    expected = _manual_rs_raw_from_valid_prices(price_df[ticker])

    assert abs(last_row[ticker] - expected) < 1e-10


def test_rs_raw_uses_ticker_valid_trading_days_for_internal_gaps():
    """Internal NaNs should be skipped when counting n trading days back."""
    dates = pd.bdate_range("2024-01-01", periods=280)
    price_df = pd.DataFrame(
        {
            "GAP": np.linspace(50, 220, len(dates)),
            "FULL": np.linspace(90, 180, len(dates)),
        },
        index=dates,
    )
    price_df.loc[dates[[12, 38, 77, 121, 203]], "GAP"] = np.nan

    rs_raw = compute_rs_raw(price_df)

    valid_gap_prices = price_df["GAP"].dropna()
    expected = _manual_rs_raw_from_valid_prices(valid_gap_prices)
    assert abs(rs_raw.iloc[-1]["GAP"] - expected) < 1e-10


def test_rs_raw_warms_up_tickers_with_fewer_than_252_valid_trading_days():
    dates = pd.bdate_range("2024-01-01", periods=300)
    data = {}
    for i in range(10):
        data[f"OLD{i:02d}"] = np.linspace(100 + i, 200 + i, len(dates))

    new_listing = pd.Series(np.nan, index=dates)
    new_listing.iloc[-251:] = np.linspace(40, 80, 251)
    data["NEW"] = new_listing
    price_df = pd.DataFrame(data, index=dates)

    rs_raw = compute_rs_raw(price_df)
    assert rs_raw["NEW"].isna().all()
    assert rs_raw[[c for c in rs_raw.columns if c.startswith("OLD")]].iloc[-1].notna().all()

    rs_rating = compute_rs_rating(rs_raw.iloc[[-1]], active_universe=set(price_df.columns))
    assert pd.isna(rs_rating.iloc[-1]["NEW"])
    assert rs_rating.drop(columns=["NEW"]).iloc[-1].notna().all()


def test_rs_rating_range():
    """RS Rating should be between 1 and 99."""
    price_df = _make_price_df(n_tickers=100, n_days=300)
    rs_raw = compute_rs_raw(price_df)
    # Only use the last row (all tickers have values)
    rs_raw_valid = rs_raw.iloc[[-1]]
    rs_rating = compute_rs_rating(rs_raw_valid, active_universe=set(price_df.columns))

    ratings = rs_rating.iloc[0].dropna()
    assert ratings.min() >= 1
    assert ratings.max() <= 99


def test_rs_rating_ordering():
    """Higher RS Raw should get higher RS Rating."""
    price_df = _make_price_df(n_tickers=50, n_days=300)
    rs_raw = compute_rs_raw(price_df)
    rs_raw_last = rs_raw.iloc[[-1]]
    rs_rating = compute_rs_rating(rs_raw_last, active_universe=set(price_df.columns))

    last_raw = rs_raw_last.iloc[0]
    last_rating = rs_rating.iloc[0]

    # The ticker with highest RS Raw should have highest RS Rating
    best_raw_ticker = last_raw.idxmax()
    best_rating_ticker = last_rating.idxmax()
    assert best_raw_ticker == best_rating_ticker

    # Lowest too
    worst_raw_ticker = last_raw.idxmin()
    worst_rating_ticker = last_rating.idxmin()
    assert worst_raw_ticker == worst_rating_ticker


def test_rs_rating_includes_reference():
    """Reference tickers (SPY, QQQ) should also have RS ratings."""
    price_df = _make_price_df(n_tickers=10, n_days=300)
    price_df = price_df.rename(columns={"T000": "SPY", "T001": "QQQ"})

    rs_raw = compute_rs_raw(price_df)
    rs_raw_last = rs_raw.iloc[[-1]]
    rs_rating = compute_rs_rating(rs_raw_last, active_universe=set(price_df.columns))

    assert rs_rating.iloc[0]["SPY"] >= 1
    assert rs_rating.iloc[0]["QQQ"] >= 1
    assert rs_rating.iloc[0]["T002"] >= 1


def test_rs_rating_skips_dates_below_universe_threshold():
    dates = pd.to_datetime(["2026-01-02", "2026-01-05"])
    tickers = [f"T{i:03d}" for i in range(100)]
    rs_raw = pd.DataFrame(np.nan, index=dates, columns=tickers)
    rs_raw.iloc[0, :90] = np.arange(90)
    rs_raw.iloc[1, :54] = np.arange(54)

    rs_rating = compute_rs_rating(rs_raw, active_universe=set(tickers))

    assert rs_rating.iloc[0].notna().sum() == 90
    assert rs_rating.iloc[0, :90].min() >= 1
    assert rs_rating.iloc[0, :90].max() <= 99
    assert rs_rating.iloc[0].idxmax() == "T089"
    assert rs_rating.iloc[1].isna().all()


def test_empty_rs_inputs_return_empty_results():
    empty = pd.DataFrame()
    assert compute_rs_raw(empty).empty
    assert compute_rs_rating(empty, active_universe=set()).empty


def test_calculate_and_store_is_atomic_on_store_failure(conn, monkeypatch):
    dates = pd.bdate_range("2024-01-01", periods=254)
    tickers = ["AAPL", "MSFT", "NVDA"]
    db.upsert_prices(
        conn,
        [
            (ticker, date.strftime("%Y-%m-%d"), 100.0 + i + day_index)
            for i, ticker in enumerate(tickers)
            for day_index, date in enumerate(dates)
        ],
    )
    calculate_and_store(
        conn,
        recalc_all=True,
        active_universe=tickers,
    )
    before = conn.execute(
        "SELECT ticker, date, rs_raw, rs_rating FROM rs "
        "WHERE rs_raw IS NOT NULL OR rs_rating IS NOT NULL "
        "ORDER BY ticker, date"
    ).fetchall()
    last_rs_date_before = db.get_meta(conn, "last_rs_date")
    upsert_calls = 0

    def fail_first_upsert(_conn, _records, *, commit=True):
        nonlocal upsert_calls
        upsert_calls += 1
        assert commit is False
        raise RuntimeError("simulated RS store failure")

    monkeypatch.setattr(db, "upsert_rs", fail_first_upsert)

    with pytest.raises(RuntimeError, match="simulated RS store failure"):
        calculate_and_store(
            conn,
            recalc_all=True,
            active_universe=tickers,
            force_full=True,
        )

    after = conn.execute(
        "SELECT ticker, date, rs_raw, rs_rating FROM rs "
        "WHERE rs_raw IS NOT NULL OR rs_rating IS NOT NULL "
        "ORDER BY ticker, date"
    ).fetchall()
    assert upsert_calls == 1
    assert after == before
    assert db.get_meta(conn, "last_rs_date") == last_rs_date_before
    assert not conn.in_transaction


def test_calculate_and_store_happy_path_commits_ratings(conn):
    dates = pd.bdate_range("2024-01-01", periods=254)
    tickers = ["AAPL", "MSFT", "NVDA"]
    db.upsert_prices(
        conn,
        [
            (ticker, date.strftime("%Y-%m-%d"), 100.0 + i + day_index)
            for i, ticker in enumerate(tickers)
            for day_index, date in enumerate(dates)
        ],
    )

    written = calculate_and_store(
        conn,
        recalc_all=True,
        active_universe=tickers,
    )

    latest_date = dates[-1].strftime("%Y-%m-%d")
    ratings = conn.execute(
        "SELECT rs_rating FROM rs WHERE date = ? ORDER BY ticker",
        (latest_date,),
    ).fetchall()
    assert written == len(tickers) * 2
    assert len(ratings) == len(tickers)
    assert all(rating is not None for (rating,) in ratings)
    assert db.get_meta(conn, "last_rs_date") == latest_date
    assert not conn.in_transaction


def test_calculate_and_store_clears_rating_for_threshold_miss_dates(conn):
    dates = pd.bdate_range("2024-01-01", periods=254)
    tickers = [f"T{i:03d}" for i in range(100)]
    price_records = []
    stale_rating_records = []

    for i, ticker in enumerate(tickers):
        for day_index, date in enumerate(dates):
            if day_index == 253 and i >= 54:
                continue
            price_records.append((ticker, date.strftime("%Y-%m-%d"), 100 + i + day_index))
        if i < 54:
            stale_rating_records.append((ticker, dates[253].strftime("%Y-%m-%d"), 0.123, 77))

    db.upsert_prices(conn, price_records)
    db.upsert_rs(conn, stale_rating_records)
    db.set_meta(conn, "ticker_list", ",".join(tickers))

    calculate_and_store(conn, recalc_all=True, force_full=True)

    threshold_miss_count = conn.execute(
        "SELECT COUNT(*) FROM rs WHERE date = ? AND rs_rating IS NOT NULL",
        (dates[253].strftime("%Y-%m-%d"),),
    ).fetchone()[0]
    threshold_pass_count = conn.execute(
        "SELECT COUNT(*) FROM rs WHERE date = ? AND rs_rating IS NOT NULL",
        (dates[252].strftime("%Y-%m-%d"),),
    ).fetchone()[0]

    assert threshold_miss_count == 0
    assert threshold_pass_count == 100


def test_recalc_all_backfills_dates_before_global_rs_cursor(conn):
    dates = pd.bdate_range("2024-01-01", periods=254)
    tickers = [f"T{i:03d}" for i in range(10)]
    price_records = [
        (ticker, date.strftime("%Y-%m-%d"), 100 + i + day_index)
        for i, ticker in enumerate(tickers)
        for day_index, date in enumerate(dates)
    ]
    db.upsert_prices(conn, price_records)
    db.upsert_rs(conn, [("T000", dates[-1].strftime("%Y-%m-%d"), 0.5, 90)])
    db.set_meta(conn, "ticker_list", ",".join(tickers))

    calculate_and_store(conn, recalc_all=True, force_full=True)

    backfilled = conn.execute(
        "SELECT rs_raw, rs_rating FROM rs WHERE ticker = ? AND date = ?",
        ("T000", dates[252].strftime("%Y-%m-%d")),
    ).fetchone()
    assert backfilled is not None
    assert backfilled[0] is not None
    assert backfilled[1] is not None


def test_recalc_all_refuses_without_force_full(conn):
    dates = pd.bdate_range("2024-01-01", periods=300)
    tickers = ["AAPL", "MSFT", "NVDA"]
    db.upsert_prices(
        conn,
        [
            (ticker, date.strftime("%Y-%m-%d"), 100.0 + i + day_index)
            for i, ticker in enumerate(tickers)
            for day_index, date in enumerate(dates)
        ],
    )
    oldest = dates[-1].strftime("%Y-%m-%d")
    db.upsert_rs(conn, [("AAPL", oldest, 0.25, 75)])
    before = conn.execute(
        "SELECT close, rs_raw, rs_rating FROM rs WHERE ticker = ? AND date = ?",
        ("AAPL", oldest),
    ).fetchone()

    with pytest.raises(SystemExit, match="without --force-full"):
        calculate_and_store(
            conn,
            recalc_all=True,
            active_universe=tickers,
        )

    assert conn.execute(
        "SELECT close, rs_raw, rs_rating FROM rs WHERE ticker = ? AND date = ?",
        ("AAPL", oldest),
    ).fetchone() == before
    assert conn.execute("SELECT COUNT(*) FROM rs WHERE rs_raw IS NOT NULL").fetchone()[0] == 1


def test_recalc_all_refuses_when_one_ticker_has_pruned_lookback(conn):
    dates = pd.bdate_range("2024-01-01", periods=300)
    full_tickers = ["AAPL", "MSFT", "NVDA"]
    price_records = [
        (ticker, date.strftime("%Y-%m-%d"), 100.0 + i + day_index)
        for i, ticker in enumerate(full_tickers)
        for day_index, date in enumerate(dates)
    ]
    price_records.extend(
        ("SPARSE", date.strftime("%Y-%m-%d"), 50.0 + day_index)
        for day_index, date in enumerate(dates[-200:])
    )
    db.upsert_prices(conn, price_records)

    target_date = dates[-1].strftime("%Y-%m-%d")
    db.upsert_rs(
        conn,
        [
            ("AAPL", target_date, 0.25, 75),
            ("SPARSE", target_date, 0.44, 91),
        ],
    )
    before_counts = conn.execute(
        "SELECT COUNT(rs_raw), COUNT(rs_rating) FROM rs"
    ).fetchone()
    before_rows = conn.execute(
        "SELECT ticker, date, rs_raw, rs_rating FROM rs "
        "WHERE rs_raw IS NOT NULL OR rs_rating IS NOT NULL ORDER BY ticker, date"
    ).fetchall()
    sparse_before = conn.execute(
        "SELECT rs_raw, rs_rating FROM rs WHERE ticker = ? AND date = ?",
        ("SPARSE", target_date),
    ).fetchone()

    with pytest.raises(SystemExit, match="stored rs_raw cells would be cleared"):
        calculate_and_store(
            conn,
            recalc_all=True,
            active_universe=full_tickers + ["SPARSE"],
            force_full=True,
        )

    after_counts = conn.execute(
        "SELECT COUNT(rs_raw), COUNT(rs_rating) FROM rs"
    ).fetchone()
    after_rows = conn.execute(
        "SELECT ticker, date, rs_raw, rs_rating FROM rs "
        "WHERE rs_raw IS NOT NULL OR rs_rating IS NOT NULL ORDER BY ticker, date"
    ).fetchall()
    sparse_after = conn.execute(
        "SELECT rs_raw, rs_rating FROM rs WHERE ticker = ? AND date = ?",
        ("SPARSE", target_date),
    ).fetchone()

    assert after_counts == before_counts
    assert after_rows == before_rows
    assert sparse_before == (0.44, 91)
    assert sparse_after == sparse_before


def test_recalc_all_allowed_with_full_history(conn):
    dates = pd.bdate_range("2023-01-02", periods=505)
    tickers = ["AAPL", "MSFT", "NVDA"]
    db.upsert_prices(
        conn,
        [
            (ticker, date.strftime("%Y-%m-%d"), 100.0 + i + day_index)
            for i, ticker in enumerate(tickers)
            for day_index, date in enumerate(dates)
        ],
    )
    oldest = dates[max(RS_WEIGHTS)].strftime("%Y-%m-%d")
    db.upsert_rs(conn, [("AAPL", oldest, 0.25, 75)])

    written = calculate_and_store(
        conn,
        recalc_all=True,
        active_universe=tickers,
        force_full=True,
    )

    assert written == len(tickers) * (len(dates) - max(RS_WEIGHTS))
    assert conn.execute(
        "SELECT COUNT(*) FROM rs WHERE date = ? AND rs_rating IS NOT NULL",
        (oldest,),
    ).fetchone()[0] == len(tickers)
    assert conn.execute(
        "SELECT COUNT(*) FROM rs WHERE date = ? AND rs_rating IS NOT NULL",
        (dates[-1].strftime("%Y-%m-%d"),),
    ).fetchone()[0] == len(tickers)


def test_incremental_aborts_on_null_cursor(conn):
    dates = pd.bdate_range("2024-01-01", periods=260)
    tickers = ["AAPL", "MSFT", "NVDA"]
    price_records = [
        (ticker, date.strftime("%Y-%m-%d"), 100.0 + i + day_index)
        for i, ticker in enumerate(tickers)
        for day_index, date in enumerate(dates)
    ]
    db.upsert_prices(conn, price_records)

    with pytest.raises(SystemExit, match="No RS cursor"):
        calculate_and_store(
            conn,
            recalc_all=False,
            active_universe=tickers,
        )

    assert conn.execute("SELECT COUNT(*) FROM rs WHERE rs_raw IS NOT NULL").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM rs WHERE close IS NOT NULL").fetchone()[0] == len(
        price_records
    )


def test_backfill_ratings_reranks_without_touching_rs_raw_or_close(conn, monkeypatch):
    dates = pd.bdate_range("2024-01-01", periods=254)
    tickers = ["AAPL", "MSFT", "NVDA", "TSLA"]
    db.upsert_prices(
        conn,
        [
            (ticker, date.strftime("%Y-%m-%d"), 100.0 + i * 10 + day_index)
            for i, ticker in enumerate(tickers)
            for day_index, date in enumerate(dates)
        ],
    )
    db.set_meta(conn, "ticker_list", ",".join(tickers))
    calculate_and_store(conn, recalc_all=True)

    target_date = dates[-1].strftime("%Y-%m-%d")
    originally_rated = conn.execute(
        "SELECT ticker, close, rs_raw, rs_rating FROM rs "
        "WHERE date = ? ORDER BY ticker",
        (target_date,),
    ).fetchall()
    assert all(row[3] is not None for row in originally_rated)

    db.clear_rs_for_dates(conn, [target_date])
    db.upsert_rs(
        conn,
        [(ticker, target_date, raw, None) for ticker, _close, raw, _rating in originally_rated],
    )
    before = conn.execute(
        "SELECT ticker, close, rs_raw, rs_rating FROM rs "
        "WHERE date = ? ORDER BY ticker",
        (target_date,),
    ).fetchall()
    before_immutable_bytes = [
        (ticker, struct.pack("!d", close), struct.pack("!d", raw))
        for ticker, close, raw, _rating in before
    ]

    def forbidden_call(*_args, **_kwargs):
        pytest.fail("rating-only backfill called a destructive/recompute primitive")

    monkeypatch.setattr(db, "clear_rs_for_dates", forbidden_call)
    monkeypatch.setattr(db, "upsert_rs", forbidden_call)
    monkeypatch.setattr(db, "upsert_prices", forbidden_call)
    monkeypatch.setattr(rs_module, "compute_rs_raw", forbidden_call)

    assert backfill_ratings(conn, target_date, target_date) == len(tickers)

    after = conn.execute(
        "SELECT ticker, close, rs_raw, rs_rating FROM rs "
        "WHERE date = ? ORDER BY ticker",
        (target_date,),
    ).fetchall()
    after_immutable_bytes = [
        (ticker, struct.pack("!d", close), struct.pack("!d", raw))
        for ticker, close, raw, _rating in after
    ]
    assert after_immutable_bytes == before_immutable_bytes
    assert [row[3] for row in after] == [row[3] for row in originally_rated]


def test_backfill_ratings_is_idempotent(conn):
    date = "2026-06-30"
    tickers = ["AAPL", "MSFT", "NVDA", "TSLA"]
    db.upsert_prices(
        conn,
        [(ticker, date, 100.0 + i) for i, ticker in enumerate(tickers)],
    )
    db.upsert_rs(
        conn,
        [(ticker, date, -0.2 + i * 0.3, None) for i, ticker in enumerate(tickers)],
    )
    db.set_meta(conn, "ticker_list", ",".join(tickers))

    assert backfill_ratings(conn, date, date) == len(tickers)
    after_first = conn.execute(
        "SELECT ticker, close, rs_raw, rs_rating FROM rs "
        "WHERE date = ? ORDER BY ticker",
        (date,),
    ).fetchall()

    assert backfill_ratings(conn, date, date) == len(tickers)
    after_second = conn.execute(
        "SELECT ticker, close, rs_raw, rs_rating FROM rs "
        "WHERE date = ? ORDER BY ticker",
        (date,),
    ).fetchall()

    assert after_second == after_first
    assert all(row[3] is not None for row in after_second)


def test_backfill_writes_null_for_gate_rejected_dates(conn):
    date = "2026-06-30"
    active_tickers = [f"T{i:02d}" for i in range(10)]
    stored_tickers = active_tickers[:8]
    db.upsert_rs(
        conn,
        [(ticker, date, 0.1 + i, 77) for i, ticker in enumerate(stored_tickers)],
    )
    db.set_meta(conn, "ticker_list", ",".join(active_tickers))

    before = conn.execute(
        "SELECT rs_raw, rs_rating FROM rs WHERE date = ? ORDER BY ticker",
        (date,),
    ).fetchall()
    assert all(raw is not None and rating == 77 for raw, rating in before)

    assert backfill_ratings(conn, date, date) == len(stored_tickers)

    after = conn.execute(
        "SELECT rs_raw, rs_rating FROM rs WHERE date = ? ORDER BY ticker",
        (date,),
    ).fetchall()
    assert [raw for raw, _rating in after] == [raw for raw, _rating in before]
    assert all(rating is None for _raw, rating in after)


def test_incremental_recompute_reraises_recent_day_left_unrated_by_a_prior_low_coverage_run(conn):
    """Reproduces the 2026-07-08 'permanent hole' bug: a day stored with rs_raw
    but NULL rating (because an earlier run had too few tickers) must still
    get re-rated by a later incremental run once full data is available --
    the old cursor (MAX(date) WHERE rs_raw IS NOT NULL) would otherwise never
    revisit it, since rs_raw was already non-NULL for that date."""
    dates = pd.bdate_range("2024-01-01", periods=280)
    tickers = [f"T{i:03d}" for i in range(100)]
    price_records = [
        (ticker, date.strftime("%Y-%m-%d"), 100 + i + day_index)
        for i, ticker in enumerate(tickers)
        for day_index, date in enumerate(dates)
    ]
    db.upsert_prices(conn, price_records)
    db.set_meta(conn, "ticker_list", ",".join(tickers))
    calculate_and_store(conn, recalc_all=True)

    # Simulate a low-coverage run: rs_raw got stored for the latest day (from
    # whatever few tickers had data) but it was left unrated.
    last_date = dates[-1].strftime("%Y-%m-%d")
    db.clear_rs_for_dates(conn, [last_date])
    db.upsert_rs(conn, [("T000", last_date, 0.01, None)])
    unrated_before = conn.execute(
        "SELECT COUNT(*) FROM rs WHERE date = ? AND rs_rating IS NOT NULL", (last_date,)
    ).fetchone()[0]
    assert unrated_before == 0

    # Full price data for that date is already present (upserted above), so a
    # later incremental run should re-rate it now.
    calculate_and_store(conn, recalc_all=False)

    rated_after = conn.execute(
        "SELECT COUNT(*) FROM rs WHERE date = ? AND rs_rating IS NOT NULL", (last_date,)
    ).fetchone()[0]
    assert rated_after == 100


def test_incremental_recompute_leaves_history_outside_the_trailing_window_untouched(conn):
    dates = pd.bdate_range("2024-01-01", periods=280)
    tickers = [f"T{i:03d}" for i in range(100)]
    price_records = [
        (ticker, date.strftime("%Y-%m-%d"), 100 + i + day_index)
        for i, ticker in enumerate(tickers)
        for day_index, date in enumerate(dates)
    ]
    db.upsert_prices(conn, price_records)
    db.set_meta(conn, "ticker_list", ",".join(tickers))
    calculate_and_store(conn, recalc_all=True)

    old_date = dates[252].strftime("%Y-%m-%d")  # well outside the trailing window
    before = conn.execute(
        "SELECT ticker, rs_raw, rs_rating FROM rs WHERE date = ? ORDER BY ticker", (old_date,)
    ).fetchall()

    # A later price "correction" for that old date must not leak into a
    # recompute -- proves the date is genuinely skipped, not just unchanged
    # by coincidence.
    db.upsert_prices(conn, [(tickers[0], old_date, 99999.0)])
    calculate_and_store(conn, recalc_all=False)

    after = conn.execute(
        "SELECT ticker, rs_raw, rs_rating FROM rs WHERE date = ? ORDER BY ticker", (old_date,)
    ).fetchall()
    assert after == before


def test_incremental_recompute_still_backfills_dates_newer_than_the_cursor(conn):
    """The trailing window is additive, not a replacement for catch-up: a
    multi-day gap larger than the window must still be fully recomputed."""
    dates = pd.bdate_range("2024-01-01", periods=280)
    tickers = [f"T{i:03d}" for i in range(100)]
    price_records = [
        (ticker, date.strftime("%Y-%m-%d"), 100 + i + day_index)
        for i, ticker in enumerate(tickers)
        for day_index, date in enumerate(dates)
    ]
    # Seed RS only through a cursor well before the trailing window, leaving
    # a gap bigger than RS_RECOMPUTE_WINDOW_DAYS with prices but no RS at all.
    cutoff_index = 280 - RS_RECOMPUTE_WINDOW_DAYS - 5
    db.upsert_prices(conn, [r for r in price_records if r[1] <= dates[cutoff_index].strftime("%Y-%m-%d")])
    db.set_meta(conn, "ticker_list", ",".join(tickers))
    calculate_and_store(conn, recalc_all=True)
    db.upsert_prices(conn, [r for r in price_records if r[1] > dates[cutoff_index].strftime("%Y-%m-%d")])

    calculate_and_store(conn, recalc_all=False)

    first_gap_date = dates[cutoff_index + 1].strftime("%Y-%m-%d")
    rated = conn.execute(
        "SELECT COUNT(*) FROM rs WHERE date = ? AND rs_rating IS NOT NULL", (first_gap_date,)
    ).fetchone()[0]
    assert rated == 100


def test_recompute_window_leaves_enough_lookback_margin_in_retention_window():
    # Retention keeps `now - PRICE_RETENTION_MONTHS*30` CALENDAR days; convert to
    # trading days (~252/365) rather than the old 21-per-month proxy, which
    # overstated the margin and hid a ~2-day knife-edge. Every recomputed date
    # (the trailing window) must still have >=252 trading days of in-window
    # lookback, with an explicit positive buffer.
    retained_calendar_days = PRICE_RETENTION_MONTHS * 30
    retained_trading_days = retained_calendar_days * 252 / 365
    max_lookback = max(RS_WEIGHTS)
    margin = retained_trading_days - (RS_RECOMPUTE_WINDOW_DAYS + max_lookback)
    assert margin >= 20, f"lookback margin {margin:.1f} trading days is too thin"


def test_recompute_margin_survives_a_holiday_dense_window():
    # A holiday-dense stretch yields fewer trading days per calendar day; the
    # retention window must still cover 252 + the recompute window.
    retained_calendar_days = PRICE_RETENTION_MONTHS * 30
    holiday_dense_ratio = 245 / 365  # ~245 trading days/yr instead of ~252
    retained_trading_days = retained_calendar_days * holiday_dense_ratio
    max_lookback = max(RS_WEIGHTS)
    assert retained_trading_days - (RS_RECOMPUTE_WINDOW_DAYS + max_lookback) > 0


def test_rs_raw_higher_ticker_has_higher_raw():
    """In our synthetic data, higher-numbered tickers have higher drift,
    so they should have higher RS Raw on average."""
    price_df = _make_price_df(n_tickers=10, n_days=300)
    rs_raw = compute_rs_raw(price_df)
    last_raw = rs_raw.iloc[-1]

    # T009 (highest drift) should generally have higher RS than T000 (lowest)
    assert last_raw["T009"] > last_raw["T000"]


def test_gate_ignores_inflated_union_of_delisted_ghost_columns():
    """BUG1 regression: a stored price union bloated with delisted 'ghost'
    columns must not push a well-covered active universe below the gate. The
    old denominator len(price_df.columns) NULLed the whole date once the union
    grew; the active-universe denominator keeps rating it."""
    date = pd.to_datetime(["2026-06-30"])
    active = [f"A{i:03d}" for i in range(100)]
    ghosts = [f"G{i:03d}" for i in range(400)]  # delisted: all-NaN columns
    rs_raw = pd.DataFrame(np.nan, index=date, columns=active + ghosts)
    rs_raw.iloc[0, :100] = np.arange(100)  # all 100 active have valid RS Raw

    rated = compute_rs_rating(rs_raw, active_universe=set(active))
    assert rated.iloc[0][active].notna().all()  # active fully rated

    # Contrast the pre-fix behavior: counting the ghosts in the denominator
    # (the old union) is 100/500 = 0.2 < 0.90 -> the entire date is NULLed.
    old_union = compute_rs_rating(rs_raw, active_universe=set(active + ghosts))
    assert old_union.iloc[0].isna().all()


def test_gate_denominator_floored_at_universe_floor(monkeypatch):
    """A collapsed active universe cannot masquerade as full coverage: even a
    fully-covered tiny active set is left unrated because the denominator is
    floored at UNIVERSE_FLOOR, the same guard the watchdog applies."""
    monkeypatch.setattr("ibd_rs.rs.UNIVERSE_FLOOR", 3000)
    date = pd.to_datetime(["2026-06-30"])
    active = [f"A{i:03d}" for i in range(50)]
    rs_raw = pd.DataFrame(
        np.arange(50, dtype=float).reshape(1, 50), index=date, columns=active
    )

    rated = compute_rs_rating(rs_raw, active_universe=set(active))
    # 50 valid / max(50, 3000) = 0.017 < 0.90 -> unrated.
    assert rated.iloc[0].isna().all()


def test_calculate_and_store_gate_uses_cached_active_universe(conn):
    """calculate_and_store sizes the rating gate against the cached ticker_list
    membership, not the raw stored price union, so delisted ghost columns in the
    union can't dilute a well-covered day below the gate (BUG1, full pipeline)."""
    dates = pd.bdate_range("2024-01-01", periods=254)
    active = [f"A{i:03d}" for i in range(100)]
    ghosts = [f"G{i:03d}" for i in range(300)]  # present in the union, delisted

    records = []
    for i, ticker in enumerate(active):
        for day_index, date in enumerate(dates):
            records.append((ticker, date.strftime("%Y-%m-%d"), 100.0 + i + day_index))
    # Ghosts have only an early stretch of closes, then go dark: no recent RS
    # Raw, but they still appear as columns in the stored price union.
    for i, ticker in enumerate(ghosts):
        for date in dates[:120]:
            records.append((ticker, date.strftime("%Y-%m-%d"), 50.0 + i))
    db.upsert_prices(conn, records)
    db.set_meta(conn, "ticker_list", ",".join(active))

    calculate_and_store(conn, recalc_all=True)

    last = dates[-1].strftime("%Y-%m-%d")
    rated = conn.execute(
        "SELECT COUNT(*) FROM rs WHERE date = ? AND rs_rating IS NOT NULL", (last,)
    ).fetchone()[0]
    assert rated == 100  # active fully rated; ghosts don't dilute the denominator


def test_calculate_and_store_gates_off_when_no_active_universe(conn):
    """Fail-closed (architect blocker fix): with no cached ticker_list and no
    explicit universe, the gate must NOT fall back to the delisting-inflated
    price union. It rates nothing (leaving the watchdog to flag the run) while
    RS Raw is still computed."""
    dates = pd.bdate_range("2024-01-01", periods=254)
    tickers = [f"T{i:03d}" for i in range(100)]
    records = [
        (ticker, date.strftime("%Y-%m-%d"), 100.0 + i + day_index)
        for i, ticker in enumerate(tickers)
        for day_index, date in enumerate(dates)
    ]
    db.upsert_prices(conn, records)
    # Deliberately set NO ticker_list meta and pass no active_universe.

    calculate_and_store(conn, recalc_all=True)

    last = dates[-1].strftime("%Y-%m-%d")
    rated = conn.execute(
        "SELECT COUNT(*) FROM rs WHERE date = ? AND rs_rating IS NOT NULL", (last,)
    ).fetchone()[0]
    rs_raw_count = conn.execute(
        "SELECT COUNT(*) FROM rs WHERE date = ? AND rs_raw IS NOT NULL", (last,)
    ).fetchone()[0]
    assert rated == 0  # gated off: no union fallback
    assert rs_raw_count == 100  # RS Raw still computed; only the rating is gated
