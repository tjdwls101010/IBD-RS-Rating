"""Tests for the Nasdaq Trader fallback universe source."""

from ibd_rs import nasdaq_trader


class _FakeResponse:
    def __init__(self, text):
        self.text = text


NASDAQ_LISTED_FIXTURE = "\n".join([
    "Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares",
    "AAPL|Apple Inc. Common Stock|Q|N|N|100|N|N",
    "QQQ|Invesco QQQ Trust|Q|N|N|100|Y|N",
    "ZZZT|Test Issue Stock|Q|Y|N|100|N|N",
    "File Creation Time: 0101202512:00|||||||",
])

OTHER_LISTED_FIXTURE = "\n".join([
    "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol",
    "SPY|SPDR S&P 500 ETF|P|SPY|Y|100|N|SPY",
    "BRK.A|Berkshire Hathaway|N|BRK.A|N|100|N|BRK.A",
    "File Creation Time: 0101202512:00||||||",
])


def test_fetch_common_stock_tickers_excludes_etfs_and_test_issues(monkeypatch):
    def fake_get(url, timeout):
        if url == nasdaq_trader.NASDAQ_LISTED_URL:
            return _FakeResponse(NASDAQ_LISTED_FIXTURE)
        if url == nasdaq_trader.OTHER_LISTED_URL:
            return _FakeResponse(OTHER_LISTED_FIXTURE)
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr(nasdaq_trader.requests, "get", fake_get)

    result = nasdaq_trader.fetch_common_stock_tickers()

    tickers = {r["ticker"] for r in result}
    assert tickers == {"AAPL", "BRK.A"}
    assert all(r["sector"] is None and r["industry"] is None for r in result)


def test_fetch_common_stock_tickers_deduplicates_across_both_directories(monkeypatch):
    listed = "\n".join([
        "Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares",
        "AAPL|Apple Inc.|Q|N|N|100|N|N",
        "File Creation Time: x|||||||",
    ])
    other = "\n".join([
        "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol",
        "AAPL|Apple Inc.|N|AAPL|N|100|N|AAPL",
        "File Creation Time: x||||||",
    ])

    def fake_get(url, timeout):
        return _FakeResponse(listed if url == nasdaq_trader.NASDAQ_LISTED_URL else other)

    monkeypatch.setattr(nasdaq_trader.requests, "get", fake_get)

    result = nasdaq_trader.fetch_common_stock_tickers()

    assert [r["ticker"] for r in result] == ["AAPL"]


def test_parse_directory_rejects_unexpected_header():
    import pytest

    with pytest.raises(ValueError):
        nasdaq_trader._parse_directory("Wrong|Header\nrow\nfooter", "Symbol", "ETF", "Test Issue")
