"""Fallback ticker universe from the Nasdaq Trader symbol directory.

Scrape-resistant (plain HTTPS, pipe-delimited, no anti-bot defenses), used only
when Finviz is unavailable and no cached universe exists. Carries no sector,
industry, or market-cap data.
"""

import logging

import requests

logger = logging.getLogger(__name__)

NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"


def _parse_directory(text, symbol_col, etf_col, test_col):
    """Parse a pipe-delimited Nasdaq Trader directory, keyed by header names.

    Drops the header row and the trailing "File Creation Time" footer row,
    and excludes ETFs and test issues.
    """
    lines = text.strip("\n").split("\n")
    if len(lines) < 2:
        return []
    header = lines[0].split("|")
    idx = {name: i for i, name in enumerate(header)}
    if symbol_col not in idx or etf_col not in idx or test_col not in idx:
        raise ValueError(f"unexpected Nasdaq Trader directory header: {header}")

    tickers = []
    for line in lines[1:-1]:
        fields = line.split("|")
        if len(fields) != len(header):
            continue
        if fields[idx[test_col]] != "N" or fields[idx[etf_col]] != "N":
            continue
        symbol = fields[idx[symbol_col]].strip()
        if symbol:
            tickers.append(symbol)
    return tickers


def fetch_common_stock_tickers():
    """Fetch a broad common-stock universe from Nasdaq Trader.

    Excludes ETFs and test issues but applies no cap/sector filter (Nasdaq
    Trader doesn't carry that data), so this is a broader superset than the
    normal Finviz-filtered universe. Returns
    [{"ticker": ..., "sector": None, "industry": None}, ...].
    """
    nasdaq_text = requests.get(NASDAQ_LISTED_URL, timeout=30).text
    other_text = requests.get(OTHER_LISTED_URL, timeout=30).text

    tickers = set(_parse_directory(nasdaq_text, "Symbol", "ETF", "Test Issue"))
    tickers |= set(_parse_directory(other_text, "ACT Symbol", "ETF", "Test Issue"))

    logger.info("Nasdaq Trader fallback universe: %d common stocks", len(tickers))
    return [{"ticker": t, "sector": None, "industry": None} for t in sorted(tickers)]
