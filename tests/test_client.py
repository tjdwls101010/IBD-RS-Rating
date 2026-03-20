"""Unit tests for rs_rating.client.RS.

All HTTP calls are mocked so the test suite runs offline and fast.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from rs_rating import RS


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
def test_custom_url_and_key(mock_urlopen):
    mock_urlopen.return_value = mock_response([
        {"ticker": "TSLA", "date": "2026-03-19", "rs_raw": 0.50, "rs_rating": 85}
    ])
    rs = RS(url="https://custom.supabase.co", key="custom-key")
    assert rs.url == "https://custom.supabase.co"
    assert rs.key == "custom-key"
    result = rs.get("TSLA")
    assert result["ticker"] == "TSLA"


def test_default_credentials():
    rs = RS()
    assert "supabase.co" in rs.url
    assert rs.key.startswith("eyJ")


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
    with pytest.raises(RuntimeError, match="Supabase API error 400"):
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
            {"date": "2026-03-19"}, {"date": "2026-03-12"},
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


def test_version():
    import rs_rating
    assert rs_rating.__version__ == "0.1.0"
