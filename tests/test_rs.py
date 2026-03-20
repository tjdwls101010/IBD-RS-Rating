"""Tests for RS calculation engine."""

import numpy as np
import pandas as pd
import pytest

from ibd_rs.rs import compute_rs_raw, compute_rs_rating


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

    # Manual calculation for the last date
    prices = price_df[ticker]
    roc_63 = prices.iloc[-1] / prices.iloc[-64] - 1
    roc_126 = prices.iloc[-1] / prices.iloc[-127] - 1
    roc_189 = prices.iloc[-1] / prices.iloc[-190] - 1
    roc_252 = prices.iloc[-1] / prices.iloc[-253] - 1
    expected = 0.4 * roc_63 + 0.2 * roc_126 + 0.2 * roc_189 + 0.2 * roc_252

    assert abs(last_row[ticker] - expected) < 1e-10


def test_rs_rating_range():
    """RS Rating should be between 1 and 99."""
    price_df = _make_price_df(n_tickers=100, n_days=300)
    rs_raw = compute_rs_raw(price_df)
    # Only use the last row (all tickers have values)
    rs_raw_valid = rs_raw.iloc[[-1]]
    rs_rating = compute_rs_rating(rs_raw_valid, reference_tickers=[])

    ratings = rs_rating.iloc[0].dropna()
    assert ratings.min() >= 1
    assert ratings.max() <= 99


def test_rs_rating_ordering():
    """Higher RS Raw should get higher RS Rating."""
    price_df = _make_price_df(n_tickers=50, n_days=300)
    rs_raw = compute_rs_raw(price_df)
    rs_raw_last = rs_raw.iloc[[-1]]
    rs_rating = compute_rs_rating(rs_raw_last, reference_tickers=[])

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
    rs_rating = compute_rs_rating(rs_raw_last)

    assert rs_rating.iloc[0]["SPY"] >= 1
    assert rs_rating.iloc[0]["QQQ"] >= 1
    assert rs_rating.iloc[0]["T002"] >= 1


def test_rs_raw_higher_ticker_has_higher_raw():
    """In our synthetic data, higher-numbered tickers have higher drift,
    so they should have higher RS Raw on average."""
    price_df = _make_price_df(n_tickers=10, n_days=300)
    rs_raw = compute_rs_raw(price_df)
    last_raw = rs_raw.iloc[-1]

    # T009 (highest drift) should generally have higher RS than T000 (lowest)
    assert last_raw["T009"] > last_raw["T000"]
