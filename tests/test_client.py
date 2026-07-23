"""Unit tests for rs_rating.client.RS.

All HTTP calls are mocked so the test suite runs offline and fast.
"""

import json
import time
import warnings
from unittest.mock import MagicMock, patch

import pytest

from rs_rating import RS
from rs_rating.client import _TOKEN_REFRESH_MARGIN_SECONDS

# Captured before any test monkeypatches RS._get_token, so token-specific
# tests can restore the real implementation.
_ORIGINAL_GET_TOKEN = RS._get_token


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def mock_response(data, status=200):
    """Return a context-manager-compatible mock for urllib.request.urlopen."""
    resp = MagicMock()
    resp.read.return_value = json.dumps(data).encode()
    resp.__enter__ = MagicMock(return_value=resp)
    resp.__exit__ = MagicMock(return_value=False)
    return resp


@pytest.fixture(autouse=True)
def _stub_token(monkeypatch):
    """Every test in this file exercises data methods, not the token
    round-trip itself -- stub it out so existing single/multi-call
    urlopen mocks don't also need to account for a token fetch.
    Token-caching behavior itself is tested separately below, where the
    real implementation is restored."""
    monkeypatch.setattr(RS, "_get_token", lambda self: "test-token")


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------

@patch("urllib.request.urlopen")
def test_get_latest(mock_urlopen):
    mock_urlopen.return_value = mock_response([
        {"ticker": "NVDA", "date": "2026-03-19", "rs_raw": 0.17, "rs_rating": 70}
    ])
    rs = RS()
    result = rs.get("NVDA")
    assert result["ticker"] == "NVDA"
    assert result["rs_rating"] == 70


@patch("urllib.request.urlopen")
def test_get_with_date(mock_urlopen):
    mock_urlopen.return_value = mock_response([
        {"ticker": "AAPL", "date": "2026-03-15", "rs_raw": 0.30, "rs_rating": 75}
    ])
    rs = RS()
    result = rs.get("AAPL", date="2026-03-15")
    assert result["date"] == "2026-03-15"


@patch("urllib.request.urlopen")
def test_get_not_found(mock_urlopen):
    mock_urlopen.return_value = mock_response([])
    rs = RS()
    result = rs.get("INVALID")
    assert result is None


@patch("urllib.request.urlopen")
def test_history(mock_urlopen):
    mock_urlopen.return_value = mock_response([
        {"date": "2026-03-19", "rs_raw": 0.17, "rs_rating": 70},
        {"date": "2026-03-18", "rs_raw": 0.16, "rs_rating": 68},
    ])
    rs = RS()
    result = rs.history("NVDA", days=2)
    assert len(result) == 2


@patch("urllib.request.urlopen")
def test_history_with_date_range(mock_urlopen):
    mock_urlopen.return_value = mock_response([
        {"date": "2026-03-15", "rs_raw": 0.14, "rs_rating": 65},
        {"date": "2026-03-16", "rs_raw": 0.15, "rs_rating": 67},
        {"date": "2026-03-17", "rs_raw": 0.16, "rs_rating": 68},
    ])
    rs = RS()
    result = rs.history("NVDA", start="2026-03-15", end="2026-03-17")
    assert len(result) == 3


@patch("urllib.request.urlopen")
def test_top(mock_urlopen):
    # First call: get latest date; second call: get top stocks
    mock_urlopen.side_effect = [
        mock_response([{"date": "2026-03-19"}]),
        mock_response([
            {"ticker": "MU", "rs_rating": 99, "rs_raw": 1.99},
            {"ticker": "AAOI", "rs_rating": 98, "rs_raw": 3.10},
        ]),
    ]
    rs = RS()
    result = rs.top(2)
    assert len(result) == 2
    assert result[0]["rs_rating"] >= result[1]["rs_rating"]


@patch("urllib.request.urlopen")
def test_top_with_date(mock_urlopen):
    """When a date is provided, only one HTTP call should be made."""
    mock_urlopen.return_value = mock_response([
        {"ticker": "MU", "rs_rating": 99, "rs_raw": 1.99},
    ])
    rs = RS()
    result = rs.top(1, date="2026-03-19")
    assert len(result) == 1
    # Only one call -- no need to fetch the latest date
    assert mock_urlopen.call_count == 1


@patch("urllib.request.urlopen")
def test_bottom(mock_urlopen):
    mock_urlopen.side_effect = [
        mock_response([{"date": "2026-03-19"}]),
        mock_response([
            {"ticker": "XYZ", "rs_rating": 1, "rs_raw": 0.001},
            {"ticker": "ABC", "rs_rating": 2, "rs_raw": 0.002},
        ]),
    ]
    rs = RS()
    result = rs.bottom(2)
    assert len(result) == 2
    assert result[0]["rs_rating"] <= result[1]["rs_rating"]


@patch("urllib.request.urlopen")
def test_compare(mock_urlopen):
    mock_urlopen.side_effect = [
        mock_response([{"date": "2026-03-19"}]),
        mock_response([
            {"ticker": "NVDA", "rs_rating": 70, "rs_raw": 0.17},
            {"ticker": "AAPL", "rs_rating": 65, "rs_raw": 0.12},
        ]),
    ]
    rs = RS()
    result = rs.compare(["NVDA", "AAPL"])
    assert len(result) == 2


@patch("urllib.request.urlopen")
def test_filter(mock_urlopen):
    mock_urlopen.side_effect = [
        mock_response([{"date": "2026-03-19"}]),
        mock_response([
            {"ticker": "MU", "rs_rating": 99, "rs_raw": 1.99},
        ]),
    ]
    rs = RS()
    result = rs.filter(min_rating=90)
    assert all(r["rs_rating"] >= 90 for r in result)


@patch("urllib.request.urlopen")
def test_filter_with_max(mock_urlopen):
    mock_urlopen.side_effect = [
        mock_response([{"date": "2026-03-19"}]),
        mock_response([
            {"ticker": "LOW", "rs_rating": 10, "rs_raw": 0.01},
        ]),
    ]
    rs = RS()
    result = rs.filter(max_rating=20)
    assert all(r["rs_rating"] <= 20 for r in result)


@patch("urllib.request.urlopen")
def test_case_insensitive(mock_urlopen):
    mock_urlopen.return_value = mock_response([
        {"ticker": "NVDA", "date": "2026-03-19", "rs_raw": 0.17, "rs_rating": 70}
    ])
    rs = RS()
    result = rs.get("nvda")  # lowercase should work
    assert result["ticker"] == "NVDA"


@patch("urllib.request.urlopen")
def test_reference(mock_urlopen):
    mock_urlopen.side_effect = [
        mock_response([{"date": "2026-03-19"}]),
        mock_response([
            {"ticker": "SPY", "rs_raw": 0.05, "rs_rating": 55, "date": "2026-03-19"},
            {"ticker": "QQQ", "rs_raw": 0.06, "rs_rating": 60, "date": "2026-03-19"},
        ]),
    ]
    rs = RS()
    result = rs.reference()
    assert len(result) == 2
    assert result[0]["rs_rating"] is not None


@patch("urllib.request.urlopen")
def test_custom_url_and_auth_url(mock_urlopen):
    mock_urlopen.return_value = mock_response([
        {"ticker": "TSLA", "date": "2026-03-19", "rs_raw": 0.50, "rs_rating": 85}
    ])
    rs = RS(url="https://custom.example.com/rest/v1", auth_url="https://custom.example.com/auth")
    assert rs._base == "https://custom.example.com/rest/v1"
    assert rs._auth_base == "https://custom.example.com/auth"
    result = rs.get("TSLA")
    assert result["ticker"] == "TSLA"


def test_default_endpoints():
    rs = RS()
    assert "neon.tech" in rs._base
    assert "neon.tech" in rs._auth_base


@patch("urllib.request.urlopen")
def test_http_error(mock_urlopen):
    """RuntimeError should be raised on HTTP errors."""
    import urllib.error
    error = urllib.error.HTTPError(
        url="http://example.com",
        code=400,
        msg="Bad Request",
        hdrs=None,
        fp=MagicMock(read=MagicMock(return_value=b'{"message":"bad"}'))
    )
    mock_urlopen.side_effect = error
    rs = RS()
    with pytest.raises(RuntimeError, match="Neon Data API error 400"):
        rs.get("NVDA")


@patch("urllib.request.urlopen")
def test_connection_error(mock_urlopen):
    """ConnectionError should be raised on URL errors."""
    import urllib.error
    mock_urlopen.side_effect = urllib.error.URLError("no host")
    rs = RS()
    with pytest.raises(ConnectionError, match="Failed to connect"):
        rs.get("NVDA")


@patch("urllib.request.urlopen")
def test_top_empty_database(mock_urlopen):
    """top() should return [] when no data is available."""
    mock_urlopen.return_value = mock_response([])
    rs = RS()
    result = rs.top()
    assert result == []


@patch("urllib.request.urlopen")
def test_movers_up(mock_urlopen):
    mock_urlopen.side_effect = [
        # _latest_date
        mock_response([{"date": "2026-03-19"}]),
        # current ratings
        mock_response([
            {"ticker": "NVDA", "rs_rating": 70},
            {"ticker": "AAPL", "rs_rating": 80},
            {"ticker": "TSLA", "rs_rating": 50},
        ]),
        # dates lookup (5 days back)
        mock_response([
            {"date": "2026-03-19"}, {"date": "2026-03-18"},
            {"date": "2026-03-17"}, {"date": "2026-03-14"},
            {"date": "2026-03-13"}, {"date": "2026-03-12"},
        ]),
        # previous ratings
        mock_response([
            {"ticker": "NVDA", "rs_rating": 60},
            {"ticker": "AAPL", "rs_rating": 82},
            {"ticker": "TSLA", "rs_rating": 55},
        ]),
    ]
    rs = RS()
    result = rs.movers(days=5, n=2, direction="up")
    assert len(result) == 2
    assert result[0]["ticker"] == "NVDA"  # +10, biggest gainer
    assert result[0]["change"] == 10


@patch("urllib.request.urlopen")
def test_movers_down(mock_urlopen):
    mock_urlopen.side_effect = [
        mock_response([{"date": "2026-03-19"}]),
        mock_response([
            {"ticker": "NVDA", "rs_rating": 70},
            {"ticker": "TSLA", "rs_rating": 50},
        ]),
        mock_response([
            {"date": "2026-03-19"}, {"date": "2026-03-18"},
            {"date": "2026-03-17"}, {"date": "2026-03-14"},
            {"date": "2026-03-13"}, {"date": "2026-03-12"},
        ]),
        mock_response([
            {"ticker": "NVDA", "rs_rating": 60},
            {"ticker": "TSLA", "rs_rating": 55},
        ]),
    ]
    rs = RS()
    result = rs.movers(days=5, n=2, direction="down")
    assert result[0]["change"] == -5  # TSLA dropped most


@patch("urllib.request.urlopen")
def test_dates(mock_urlopen):
    mock_urlopen.side_effect = [
        mock_response([{"date": "2025-03-21"}]),
        mock_response([{"date": "2026-03-19"}]),
    ]
    rs = RS()
    result = rs.dates()
    assert result["first"] == "2025-03-21"
    assert result["last"] == "2026-03-19"


@patch("urllib.request.urlopen")
def test_dates_empty(mock_urlopen):
    mock_urlopen.side_effect = [
        mock_response([]),
        mock_response([]),
    ]
    rs = RS()
    result = rs.dates()
    assert result["first"] is None
    assert result["last"] is None


@patch("urllib.request.urlopen")
def test_staleness_warns_when_ratings_lag_closes(mock_urlopen):
    mock_urlopen.side_effect = [
        mock_response([{"date": "2026-07-18"}]),
        mock_response([{"date": "2026-07-22"}]),
    ]

    with pytest.warns(
        UserWarning,
        match=(
            "RS ratings are stale: latest rated 2026-07-18 "
            "lags latest close 2026-07-22"
        ),
    ):
        result = RS().staleness()

    assert result == {
        "latest_rated_date": "2026-07-18",
        "latest_close_date": "2026-07-22",
        "is_stale": True,
        "lag_days": 4,
    }
    rated_url, close_url = [
        call.args[0].full_url for call in mock_urlopen.call_args_list
    ]
    assert "rs_rating=not.is.null" in rated_url
    assert "close=not.is.null" in close_url


@patch("urllib.request.urlopen")
def test_staleness_not_stale_when_current(mock_urlopen):
    mock_urlopen.side_effect = [
        mock_response([{"date": "2026-07-22"}]),
        mock_response([{"date": "2026-07-22"}]),
    ]

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = RS().staleness()

    assert result == {
        "latest_rated_date": "2026-07-22",
        "latest_close_date": "2026-07-22",
        "is_stale": False,
        "lag_days": 0,
    }
    assert caught == []



@patch("urllib.request.urlopen")
def test_sectors(mock_urlopen):
    mock_urlopen.return_value = mock_response([
        {"sector": "Technology"},
        {"sector": "Healthcare"},
        {"sector": "Technology"},
    ])
    rs = RS()
    result = rs.sectors()
    assert "Technology" in result
    assert "Healthcare" in result
    assert len(result) == 2  # deduplicated


@patch("urllib.request.urlopen")
def test_industries(mock_urlopen):
    mock_urlopen.return_value = mock_response([
        {"industry": "Semiconductors"},
        {"industry": "Software"},
    ])
    rs = RS()
    result = rs.industries(sector="Technology")
    assert "Semiconductors" in result


@patch("urllib.request.urlopen")
def test_sector_ranking(mock_urlopen):
    mock_urlopen.side_effect = [
        mock_response([{"date": "2026-03-19"}]),  # _latest_date
        mock_response([  # rs ratings
            {"ticker": "NVDA", "rs_rating": 90},
            {"ticker": "AAPL", "rs_rating": 70},
            {"ticker": "LLY", "rs_rating": 60},
        ]),
        mock_response([  # tickers info
            {"ticker": "NVDA", "sector": "Technology"},
            {"ticker": "AAPL", "sector": "Technology"},
            {"ticker": "LLY", "sector": "Healthcare"},
        ]),
    ]
    rs = RS()
    result = rs.sector_ranking()
    assert len(result) == 2
    assert result[0]["sector"] == "Technology"  # avg 80
    assert result[0]["avg_rs"] == 80.0


@patch("urllib.request.urlopen")
def test_sector_top(mock_urlopen):
    mock_urlopen.side_effect = [
        mock_response([{"date": "2026-03-19"}]),
        mock_response([{"ticker": "NVDA", "industry": "Semiconductors"}]),
        mock_response([{"ticker": "NVDA", "rs_rating": 90, "rs_raw": 0.5}]),
    ]
    rs = RS()
    result = rs.sector_top("Technology", n=5)
    assert len(result) == 1
    assert result[0]["industry"] == "Semiconductors"


@patch("urllib.request.urlopen")
def test_industry_top(mock_urlopen):
    mock_urlopen.side_effect = [
        mock_response([{"date": "2026-03-19"}]),
        mock_response([{"ticker": "NVDA"}, {"ticker": "AMD"}]),
        mock_response([
            {"ticker": "NVDA", "rs_rating": 90, "rs_raw": 0.5},
            {"ticker": "AMD", "rs_rating": 80, "rs_raw": 0.3},
        ]),
    ]
    rs = RS()
    result = rs.industry_top("Semiconductors", n=5)
    assert len(result) == 2


@patch("urllib.request.urlopen")
def test_sectors_empty_tickers_table_returns_empty(mock_urlopen):
    mock_urlopen.return_value = mock_response([])
    assert RS().sectors() == []


@patch("urllib.request.urlopen")
def test_industries_empty_tickers_table_returns_empty(mock_urlopen):
    mock_urlopen.return_value = mock_response([])
    assert RS().industries() == []


@patch("urllib.request.urlopen")
def test_sector_ranking_degrades_when_tickers_table_empty(mock_urlopen):
    # Ratings exist but the tickers table carries no sector mapping (the exact
    # state right after Finviz is demoted / before the weekly enrichment runs).
    mock_urlopen.side_effect = [
        mock_response([{"date": "2026-03-19"}]),          # _latest_date
        mock_response([{"ticker": "NVDA", "rs_rating": 90}]),  # rs ratings
        mock_response([]),                                 # tickers info (empty)
    ]
    assert RS().sector_ranking() == []  # sensible empty, not a crash


@patch("urllib.request.urlopen")
def test_sector_ranking_partial_sector_data_returns_partial(mock_urlopen):
    # Only some rated tickers carry a sector; the ranking aggregates just those.
    mock_urlopen.side_effect = [
        mock_response([{"date": "2026-03-19"}]),
        mock_response([
            {"ticker": "NVDA", "rs_rating": 90},
            {"ticker": "AAPL", "rs_rating": 70},
            {"ticker": "GHOST", "rs_rating": 50},  # no sector row -> excluded
        ]),
        mock_response([
            {"ticker": "NVDA", "sector": "Technology"},
            {"ticker": "AAPL", "sector": "Technology"},
        ]),
    ]
    result = RS().sector_ranking()
    assert result == [{"sector": "Technology", "avg_rs": 80.0, "count": 2}]


@patch("urllib.request.urlopen")
def test_industry_ranking_degrades_when_tickers_table_empty(mock_urlopen):
    mock_urlopen.side_effect = [
        mock_response([{"date": "2026-03-19"}]),
        mock_response([{"ticker": "NVDA", "rs_rating": 90}]),
        mock_response([]),
    ]
    assert RS().industry_ranking() == []


@patch("urllib.request.urlopen")
def test_sector_top_unknown_sector_returns_empty(mock_urlopen):
    mock_urlopen.side_effect = [
        mock_response([{"date": "2026-03-19"}]),
        mock_response([]),  # no tickers in this sector
    ]
    assert RS().sector_top("Nonexistent") == []


@patch("urllib.request.urlopen")
def test_industry_top_unknown_industry_returns_empty(mock_urlopen):
    mock_urlopen.side_effect = [
        mock_response([{"date": "2026-03-19"}]),
        mock_response([]),
    ]
    assert RS().industry_top("Nonexistent") == []


def test_version_matches_pyproject_across_packages():
    import tomllib
    from pathlib import Path
    import ibd_rs
    import rs_rating

    pyproject = tomllib.loads(
        (Path(__file__).resolve().parents[1] / "pyproject.toml").read_text(encoding="utf-8")
    )
    expected = pyproject["project"]["version"]

    assert rs_rating.__version__ == expected
    assert ibd_rs.__version__ == expected


# ------------------------------------------------------------------
# Token caching (real _get_token restored; urlopen mocked directly)
# ------------------------------------------------------------------

def test_token_is_fetched_and_sent_as_bearer_auth(monkeypatch):
    monkeypatch.setattr(RS, "_get_token", _ORIGINAL_GET_TOKEN)
    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_urlopen.side_effect = [
            mock_response({"token": "abc123", "expires_at": time.time() + 3600}),
            mock_response([{"ticker": "NVDA", "date": "2026-03-19", "rs_raw": 0.17, "rs_rating": 70}]),
        ]
        rs = RS()
        result = rs.get("NVDA")

    assert result["ticker"] == "NVDA"
    token_call, data_call = mock_urlopen.call_args_list
    assert "/token/anonymous" in token_call.args[0].full_url
    assert data_call.args[0].headers["Authorization"] == "Bearer abc123"


def test_token_is_cached_across_multiple_requests(monkeypatch):
    monkeypatch.setattr(RS, "_get_token", _ORIGINAL_GET_TOKEN)
    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_urlopen.side_effect = [
            mock_response({"token": "abc123", "expires_at": time.time() + 3600}),
            mock_response([{"ticker": "NVDA", "rs_rating": 70}]),
            mock_response([{"ticker": "AAPL", "rs_rating": 80}]),
        ]
        rs = RS()
        rs.get("NVDA")
        rs.get("AAPL")

    token_calls = [c for c in mock_urlopen.call_args_list if "/token/anonymous" in c.args[0].full_url]
    assert len(token_calls) == 1


def test_token_is_refreshed_once_near_expiry(monkeypatch):
    monkeypatch.setattr(RS, "_get_token", _ORIGINAL_GET_TOKEN)
    rs = RS()
    rs._token = "stale-token"
    rs._token_expires_at = time.time() + _TOKEN_REFRESH_MARGIN_SECONDS - 1  # inside the refresh margin

    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_urlopen.side_effect = [
            mock_response({"token": "fresh-token", "expires_at": time.time() + 3600}),
            mock_response([{"ticker": "NVDA", "rs_rating": 70}]),
        ]
        rs.get("NVDA")

    assert rs._token == "fresh-token"
