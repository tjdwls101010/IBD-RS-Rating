"""Tests for the owned Finviz screener parser."""

import pytest
import finvizfinance.screener.overview as overview_module
import finvizfinance.util as finviz_util
from bs4 import BeautifulSoup

from ibd_rs import finviz


def _soup(html):
    return BeautifulSoup(html, "html.parser")


def test_parser_extracts_clean_ticker_from_logo_dom():
    soup = _soup(
        """
        <table class="screener_table">
          <tr><th>No.</th><th>Ticker</th><th>Sector</th><th>Industry</th></tr>
          <tr>
            <td>1</td>
            <td><a href="quote.ashx?t=AAPL"><span>A</span>AAPL</a></td>
            <td>Technology</td>
            <td>Consumer Electronics</td>
          </tr>
        </table>
        """
    )

    ticker_cell = soup.find_all("td")[1]
    assert ticker_cell.text == "AAAPL"
    assert finviz._parse_screener_table(soup) == [
        {
            "ticker": "AAPL",
            "sector": "Technology",
            "industry": "Consumer Electronics",
        }
    ]


@pytest.mark.parametrize(
    ("href", "expected"),
    [
        ("quote.ashx?v=111&t=aapl&ty=c", "AAPL"),
        ("quote.ashx?t=AAC-U", "AAC-U"),
        ("quote.ashx?v=111&o=ticker", None),
        (None, None),
    ],
)
def test_ticker_from_href_variants(href, expected):
    assert finviz._ticker_from_href(href) == expected


def test_parse_skips_rows_without_ticker_anchor():
    soup = _soup(
        """
        <table class="screener_table">
          <tr><th>No.</th><th>Ticker</th><th>Sector</th><th>Industry</th></tr>
          <tr>
            <td>1</td><td>MSFT</td><td>Technology</td><td>Software</td>
          </tr>
          <tr>
            <td>2</td><td><a href="quote.ashx?v=111">NVDA</a></td>
            <td>Technology</td><td>Semiconductors</td>
          </tr>
          <tr>
            <td>3</td><td><a href="quote.ashx?t=AAPL">AAPL</a></td>
            <td>Technology</td><td>Consumer Electronics</td>
          </tr>
        </table>
        """
    )

    assert finviz._parse_screener_table(soup) == [
        {
            "ticker": "AAPL",
            "sector": "Technology",
            "industry": "Consumer Electronics",
        }
    ]


class _FakeOverview:
    size = 20
    url = "https://finviz.com/screener.ashx"

    def __init__(self):
        self.request_params = {"v": 111}
        self.filters = None

    def set_filter(self, filters_dict):
        self.filters = filters_dict


def _page(ticker, page_count=None):
    options = ""
    if page_count is not None:
        options = '<select id="pageSelect">' + (
            "<option></option>" * page_count
        ) + "</select>"
    return _soup(
        f"""
        {options}
        <table class="screener_table">
          <tr><th>No.</th><th>Ticker</th><th>Sector</th><th>Industry</th></tr>
          <tr>
            <td>1</td><td><a href="quote.ashx?t={ticker}">{ticker}</a></td>
            <td>Technology</td><td>Software</td>
          </tr>
        </table>
        """
    )


def test_fetch_screener_records_paginates_with_owned_parser(monkeypatch):
    overview = _FakeOverview()
    pages = {1: _page("AAPL", page_count=2), 21: _page("MSFT")}
    requests = []
    sleeps = []

    monkeypatch.setattr(overview_module, "Overview", lambda: overview)
    monkeypatch.setattr(
        finviz_util,
        "web_scrap",
        lambda url, params: requests.append((url, dict(params)))
        or pages[params.get("r", 1)],
    )
    monkeypatch.setattr(finviz.time, "sleep", lambda seconds: sleeps.append(seconds))

    records, raw_count, reported_total = finviz.fetch_screener_records(
        {"Market Cap.": "+Micro (over $50mln)"},
    )

    assert overview.filters == {"Market Cap.": "+Micro (over $50mln)"}
    assert [record["ticker"] for record in records] == ["AAPL", "MSFT"]
    assert raw_count == 2
    assert reported_total == 40
    assert requests[0][1] == {"v": 111, "o": "ticker"}
    assert requests[1][1]["r"] == 21
    assert sleeps == [1]


def test_fetch_screener_records_raises_for_missing_results(monkeypatch):
    monkeypatch.setattr(overview_module, "Overview", _FakeOverview)
    monkeypatch.setattr(
        finviz_util,
        "web_scrap",
        lambda url, params: _soup("<html></html>"),
    )

    with pytest.raises(RuntimeError, match="Finviz screener returned no results"):
        finviz.fetch_screener_records({})
