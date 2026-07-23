"""Owned Finviz screener parser.

Finviz's ticker cells contain presentation markup whose visible text is not a
stable symbol source. Tickers are extracted from the quote link's ``t`` query
parameter instead.
"""

import re
import time


_TICKER_QUERY_PATTERN = re.compile(r"[?&]t=([^&]+)")


def _ticker_from_href(href):
    """Return the uppercased ``t`` query parameter from a Finviz link."""
    if not href:
        return None
    match = _TICKER_QUERY_PATTERN.search(href)
    return match.group(1).upper() if match else None


def _parse_screener_table(soup):
    """Parse ticker, sector, and industry records from a screener page."""
    table = soup.find("table", class_="screener_table")
    if table is None:
        return []

    rows = table.find_all("tr")
    if not rows:
        return []

    headers = [header.text.strip() for header in rows[0].find_all("th")][1:]
    indexes = {name: index for index, name in enumerate(headers)}
    required = {"Ticker", "Sector", "Industry"}
    if not required.issubset(indexes):
        return []

    records = []
    for row in rows[1:]:
        cols = row.find_all("td")[1:]
        if len(cols) <= max(indexes[name] for name in required):
            continue

        anchor = cols[indexes["Ticker"]].find("a", href=True)
        ticker = _ticker_from_href(anchor["href"]) if anchor else None
        if not ticker:
            continue

        sector = cols[indexes["Sector"]].text.strip() or None
        industry = cols[indexes["Industry"]].text.strip() or None
        records.append({"ticker": ticker, "sector": sector, "industry": industry})

    return records


def _page_count(soup):
    """Return the screener's page count, including a one-page fallback."""
    page_select = soup.find(id="pageSelect")
    if page_select is not None:
        count = len(page_select.find_all("option"))
        if count:
            return count
    return 1 if soup.find("table", class_="screener_table") is not None else 0


def fetch_screener_records(filters_dict):
    """Fetch and parse all Finviz screener pages for ``filters_dict``."""
    # Keep the optional engine dependency off import-only/client code paths.
    from finvizfinance.screener.overview import Overview
    from finvizfinance.util import web_scrap

    overview = Overview()
    overview.set_filter(filters_dict=filters_dict)
    overview.request_params["o"] = "ticker"

    soup = web_scrap(overview.url, overview.request_params)
    page_count = _page_count(soup)
    if page_count == 0:
        raise RuntimeError("Finviz screener returned no results")

    records = _parse_screener_table(soup)
    for page_index in range(1, page_count):
        overview.request_params["r"] = page_index * overview.size + 1
        time.sleep(1)
        soup = web_scrap(overview.url, overview.request_params)
        records.extend(_parse_screener_table(soup))

    return records, len(records), page_count * overview.size
