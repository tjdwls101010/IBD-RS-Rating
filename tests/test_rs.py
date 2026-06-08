"""Tests for RS calculation engine."""

import numpy as np
import pandas as pd
import pytest

from ibd_rs import db
from ibd_rs.rs import calculate_and_store, compute_rs_raw, compute_rs_rating


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

    rs_rating = compute_rs_rating(rs_raw.iloc[[-1]], universe_size=len(price_df.columns))
    assert pd.isna(rs_rating.iloc[-1]["NEW"])
    assert rs_rating.drop(columns=["NEW"]).iloc[-1].notna().all()


def test_rs_rating_range():
    """RS Rating should be between 1 and 99."""
    price_df = _make_price_df(n_tickers=100, n_days=300)
    rs_raw = compute_rs_raw(price_df)
    # Only use the last row (all tickers have values)
    rs_raw_valid = rs_raw.iloc[[-1]]
    rs_rating = compute_rs_rating(rs_raw_valid, universe_size=len(price_df.columns))

    ratings = rs_rating.iloc[0].dropna()
    assert ratings.min() >= 1
    assert ratings.max() <= 99


def test_rs_rating_ordering():
    """Higher RS Raw should get higher RS Rating."""
    price_df = _make_price_df(n_tickers=50, n_days=300)
    rs_raw = compute_rs_raw(price_df)
    rs_raw_last = rs_raw.iloc[[-1]]
    rs_rating = compute_rs_rating(rs_raw_last, universe_size=len(price_df.columns))

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
    rs_rating = compute_rs_rating(rs_raw_last, universe_size=len(price_df.columns))

    assert rs_rating.iloc[0]["SPY"] >= 1
    assert rs_rating.iloc[0]["QQQ"] >= 1
    assert rs_rating.iloc[0]["T002"] >= 1


def test_rs_rating_skips_dates_below_universe_threshold():
    dates = pd.to_datetime(["2026-01-02", "2026-01-05"])
    tickers = [f"T{i:03d}" for i in range(100)]
    rs_raw = pd.DataFrame(np.nan, index=dates, columns=tickers)
    rs_raw.iloc[0, :90] = np.arange(90)
    rs_raw.iloc[1, :54] = np.arange(54)

    rs_rating = compute_rs_rating(rs_raw, universe_size=100)

    assert rs_rating.iloc[0].notna().sum() == 90
    assert rs_rating.iloc[0, :90].min() >= 1
    assert rs_rating.iloc[0, :90].max() <= 99
    assert rs_rating.iloc[0].idxmax() == "T089"
    assert rs_rating.iloc[1].isna().all()


def test_empty_rs_inputs_return_empty_results():
    empty = pd.DataFrame()
    assert compute_rs_raw(empty).empty
    assert compute_rs_rating(empty, universe_size=0).empty


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
        stale_rating_records.append((ticker, dates[253].strftime("%Y-%m-%d"), 0.123, 77))

    db.upsert_prices(conn, price_records)
    db.upsert_rs(conn, stale_rating_records)

    calculate_and_store(conn, recalc_all=True)

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
    db.upsert_rs(conn, [("CURSOR", dates[-1].strftime("%Y-%m-%d"), 0.5, 90)])

    calculate_and_store(conn, recalc_all=True)

    backfilled = conn.execute(
        "SELECT rs_raw, rs_rating FROM rs WHERE ticker = ? AND date = ?",
        ("T000", dates[252].strftime("%Y-%m-%d")),
    ).fetchone()
    assert backfilled is not None
    assert backfilled[0] is not None
    assert backfilled[1] is not None


def test_rs_raw_higher_ticker_has_higher_raw():
    """In our synthetic data, higher-numbered tickers have higher drift,
    so they should have higher RS Raw on average."""
    price_df = _make_price_df(n_tickers=10, n_days=300)
    rs_raw = compute_rs_raw(price_df)
    last_raw = rs_raw.iloc[-1]

    # T009 (highest drift) should generally have higher RS than T000 (lowest)
    assert last_raw["T009"] > last_raw["T000"]
