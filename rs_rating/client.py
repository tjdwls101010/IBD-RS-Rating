"""IBD-style Relative Strength Rating client.

Uses Supabase REST API (PostgREST) to query pre-calculated RS ratings.
Zero external dependencies -- uses only the Python standard library.
"""

import json
import urllib.error
import urllib.parse
import urllib.request


class RS:
    """IBD-style Relative Strength Rating client.

    Uses Supabase REST API to query pre-calculated RS ratings
    for ~4600 US stocks, updated daily.

    Example::

        >>> from rs_rating import RS
        >>> rs = RS()
        >>> rs.get("NVDA")
        {'ticker': 'NVDA', 'date': '2026-03-19', 'rs_raw': 0.1666, 'rs_rating': 70}
    """

    # Default public Supabase endpoint (read-only via RLS)
    DEFAULT_URL = "https://qgoytloruyjtyasypesv.supabase.co"
    DEFAULT_KEY = (
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
        "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFnb3l0bG9ydXlqdHlhc3lwZXN2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM5MzExNjgsImV4cCI6MjA4OTUwNzE2OH0."
        "OjnyBGly1Xyqcb0GIk9wymoLrnsocUoodWkazt1_HyQ"
    )

    def __init__(self, url=None, key=None):
        self.url = (url or self.DEFAULT_URL).rstrip("/")
        self.key = key or self.DEFAULT_KEY
        self._base = f"{self.url}/rest/v1"
        self._headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
        }

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    def get(self, ticker, date=None):
        """Get RS rating for a ticker.

        Args:
            ticker: Stock symbol (e.g., ``"NVDA"``).
            date: Optional date string ``"YYYY-MM-DD"``.  If *None*, returns
                the latest available rating.

        Returns:
            A *dict* with keys ``ticker``, ``date``, ``rs_raw``, ``rs_rating``,
            or *None* if the ticker is not found.
        """
        if date:
            params = {
                "ticker": f"eq.{ticker.upper()}",
                "date": f"eq.{date}",
            }
        else:
            params = {
                "ticker": f"eq.{ticker.upper()}",
                "order": "date.desc",
                "limit": "1",
            }
        resp = self._request("rs", params)
        return resp[0] if resp else None

    def history(self, ticker, start=None, end=None, days=30):
        """Get RS rating history for a ticker.

        Args:
            ticker: Stock symbol.
            start: Start date ``"YYYY-MM-DD"``.
            end: End date ``"YYYY-MM-DD"``.
            days: Number of recent days (used when *start*/*end* are not given).

        Returns:
            A *list* of dicts with ``date``, ``rs_raw``, ``rs_rating``.
        """
        params = {
            "ticker": f"eq.{ticker.upper()}",
            "select": "date,rs_raw,rs_rating",
            "order": "date.desc",
        }
        if start:
            params["order"] = "date.asc"
            if end:
                params["and"] = f"(date.gte.{start},date.lte.{end})"
            else:
                params["date"] = f"gte.{start}"
        else:
            params["limit"] = str(days)
        return self._request("rs", params)

    def top(self, n=20, date=None):
        """Get top *n* stocks by RS Rating.

        Args:
            n: Number of stocks to return (default 20).
            date: Date string.  If *None*, uses the latest available date.

        Returns:
            A *list* of dicts with ``ticker``, ``rs_rating``, ``rs_raw``.
        """
        date = date or self._latest_date()
        if not date:
            return []

        params = {
            "select": "ticker,rs_rating,rs_raw",
            "date": f"eq.{date}",
            "rs_rating": "not.is.null",
            "order": "rs_rating.desc,rs_raw.desc",
            "limit": str(n),
        }
        return self._request("rs", params)

    def bottom(self, n=20, date=None):
        """Get bottom *n* stocks by RS Rating."""
        date = date or self._latest_date()
        if not date:
            return []

        params = {
            "select": "ticker,rs_rating,rs_raw",
            "date": f"eq.{date}",
            "rs_rating": "not.is.null",
            "order": "rs_rating.asc,rs_raw.asc",
            "limit": str(n),
        }
        return self._request("rs", params)

    def filter(self, min_rating=None, max_rating=None, date=None):
        """Filter stocks by RS Rating range.

        Args:
            min_rating: Minimum RS Rating (inclusive).
            max_rating: Maximum RS Rating (inclusive).
            date: Date string.  If *None*, uses the latest available date.

        Returns:
            A *list* of dicts sorted by ``rs_rating`` descending.
        """
        date = date or self._latest_date()
        if not date:
            return []

        params = {
            "select": "ticker,rs_rating,rs_raw",
            "date": f"eq.{date}",
            "rs_rating": "not.is.null",
            "order": "rs_rating.desc",
        }

        conditions = []
        if min_rating is not None:
            conditions.append(f"rs_rating.gte.{min_rating}")
        if max_rating is not None:
            conditions.append(f"rs_rating.lte.{max_rating}")
        if conditions:
            params["and"] = f"({','.join(conditions)})"

        return self._request("rs", params)

    def compare(self, tickers, date=None):
        """Compare RS ratings for multiple tickers.

        Args:
            tickers: A list of ticker symbols.
            date: Date string.  If *None*, uses the latest available date.

        Returns:
            A *list* of dicts sorted by ``rs_rating`` descending.
        """
        date = date or self._latest_date()
        if not date:
            return []

        tickers_str = ",".join(t.upper() for t in tickers)
        params = {
            "select": "ticker,rs_rating,rs_raw",
            "date": f"eq.{date}",
            "ticker": f"in.({tickers_str})",
            "order": "rs_rating.desc.nullslast",
        }
        return self._request("rs", params)

    def reference(self, date=None):
        """Get reference tickers (SPY, QQQ) RS scores.

        Returns:
            A *list* of dicts with ``ticker``, ``rs_raw``, ``rs_rating``, ``date``.
        """
        date = date or self._latest_date()
        if not date:
            return []

        params = {
            "select": "ticker,rs_raw,rs_rating,date",
            "date": f"eq.{date}",
            "ticker": "in.(SPY,QQQ)",
            "order": "rs_rating.desc",
        }
        return self._request("rs", params)

    def movers(self, days=5, n=20, direction="up"):
        """Get stocks with the biggest RS Rating change over recent days.

        Args:
            days: Lookback period in trading days (default 5).
            n: Number of results (default 20).
            direction: ``"up"`` for biggest gainers, ``"down"`` for biggest losers.

        Returns:
            A *list* of dicts with ``ticker``, ``rs_rating``, ``prev_rating``,
            ``change``, sorted by change magnitude.
        """
        # Get all ratings for latest date
        today = self._latest_date()
        if not today:
            return []

        current = self._request("rs", {
            "select": "ticker,rs_rating",
            "date": f"eq.{today}",
            "rs_rating": "not.is.null",
        })

        # Use SPY history to get distinct trading dates efficiently
        spy_dates = self._request("rs", {
            "select": "date",
            "ticker": "eq.SPY",
            "rs_rating": "not.is.null",
            "order": "date.desc",
            "limit": str(days + 1),
        })
        if len(spy_dates) <= days:
            return []
        prev_date = spy_dates[-1]["date"]

        previous = self._request("rs", {
            "select": "ticker,rs_rating",
            "date": f"eq.{prev_date}",
            "rs_rating": "not.is.null",
        })

        # Build lookup and compute changes
        prev_map = {r["ticker"]: r["rs_rating"] for r in previous}
        results = []
        for r in current:
            ticker = r["ticker"]
            if ticker in prev_map:
                change = r["rs_rating"] - prev_map[ticker]
                results.append({
                    "ticker": ticker,
                    "rs_rating": r["rs_rating"],
                    "prev_rating": prev_map[ticker],
                    "change": change,
                })

        reverse = direction.lower() != "down"
        results.sort(key=lambda x: x["change"], reverse=reverse)
        return results[:n]

    def dates(self):
        """Get the available date range for RS data.

        Returns:
            A *dict* with ``first`` (earliest date), ``last`` (latest date).
        """
        first = self._request("rs", {
            "select": "date",
            "rs_rating": "not.is.null",
            "order": "date.asc",
            "limit": "1",
        })
        last = self._request("rs", {
            "select": "date",
            "rs_rating": "not.is.null",
            "order": "date.desc",
            "limit": "1",
        })
        if not first or not last:
            return {"first": None, "last": None}
        return {"first": first[0]["date"], "last": last[0]["date"]}

    # ------------------------------------------------------------------
    # Sector / Industry analysis
    # ------------------------------------------------------------------

    def sectors(self):
        """List all available sectors.

        Returns:
            A *list* of sector name strings.
        """
        rows = self._request("tickers", {
            "select": "sector",
            "sector": "not.is.null",
            "order": "sector.asc",
            "limit": "10000",
        })
        return sorted(set(r["sector"] for r in rows if r["sector"]))

    def industries(self, sector=None):
        """List all available industries.

        Args:
            sector: Optional sector to filter by.

        Returns:
            A *list* of industry name strings.
        """
        params = {
            "select": "industry",
            "industry": "not.is.null",
            "order": "industry.asc",
            "limit": "10000",
        }
        if sector:
            params["sector"] = f"eq.{sector}"
        rows = self._request("tickers", params)
        return sorted(set(r["industry"] for r in rows if r["industry"]))

    def sector_ranking(self, date=None):
        """Rank sectors by average RS Rating.

        Args:
            date: Date string. If *None*, uses latest.

        Returns:
            A *list* of dicts with ``sector``, ``avg_rs``, ``count``,
            sorted by avg_rs descending.
        """
        date = date or self._latest_date()
        if not date:
            return []

        # Get all RS ratings for the date
        ratings = self._request("rs", {
            "select": "ticker,rs_rating",
            "date": f"eq.{date}",
            "rs_rating": "not.is.null",
        })

        # Get ticker→sector mapping
        tickers_info = self._request("tickers", {
            "select": "ticker,sector",
            "sector": "not.is.null",
        })
        sector_map = {t["ticker"]: t["sector"] for t in tickers_info}

        # Aggregate by sector
        sector_totals = {}
        for r in ratings:
            sector = sector_map.get(r["ticker"])
            if sector:
                if sector not in sector_totals:
                    sector_totals[sector] = {"sum": 0, "count": 0}
                sector_totals[sector]["sum"] += r["rs_rating"]
                sector_totals[sector]["count"] += 1

        result = [
            {
                "sector": s,
                "avg_rs": round(d["sum"] / d["count"], 1),
                "count": d["count"],
            }
            for s, d in sector_totals.items()
        ]
        result.sort(key=lambda x: x["avg_rs"], reverse=True)
        return result

    def industry_ranking(self, date=None, sector=None):
        """Rank industries by average RS Rating.

        Args:
            date: Date string. If *None*, uses latest.
            sector: Optional sector to filter by.

        Returns:
            A *list* of dicts with ``industry``, ``sector``, ``avg_rs``, ``count``,
            sorted by avg_rs descending.
        """
        date = date or self._latest_date()
        if not date:
            return []

        ratings = self._request("rs", {
            "select": "ticker,rs_rating",
            "date": f"eq.{date}",
            "rs_rating": "not.is.null",
        })

        params = {"select": "ticker,sector,industry", "industry": "not.is.null"}
        if sector:
            params["sector"] = f"eq.{sector}"
        tickers_info = self._request("tickers", params)
        info_map = {t["ticker"]: t for t in tickers_info}

        industry_totals = {}
        for r in ratings:
            info = info_map.get(r["ticker"])
            if info:
                key = info["industry"]
                if key not in industry_totals:
                    industry_totals[key] = {"sector": info["sector"], "sum": 0, "count": 0}
                industry_totals[key]["sum"] += r["rs_rating"]
                industry_totals[key]["count"] += 1

        result = [
            {
                "industry": ind,
                "sector": d["sector"],
                "avg_rs": round(d["sum"] / d["count"], 1),
                "count": d["count"],
            }
            for ind, d in industry_totals.items()
        ]
        result.sort(key=lambda x: x["avg_rs"], reverse=True)
        return result

    def sector_top(self, sector, n=20, date=None):
        """Get top N stocks within a specific sector.

        Args:
            sector: Sector name (e.g., ``"Technology"``).
            n: Number of results.
            date: Date string. If *None*, uses latest.

        Returns:
            A *list* of dicts with ``ticker``, ``rs_rating``, ``rs_raw``, ``industry``.
        """
        date = date or self._latest_date()
        if not date:
            return []

        # Get tickers in this sector
        sector_tickers = self._request("tickers", {
            "select": "ticker,industry",
            "sector": f"eq.{sector}",
        })
        if not sector_tickers:
            return []

        ticker_industry = {t["ticker"]: t["industry"] for t in sector_tickers}
        tickers_str = ",".join(ticker_industry.keys())

        ratings = self._request("rs", {
            "select": "ticker,rs_rating,rs_raw",
            "date": f"eq.{date}",
            "ticker": f"in.({tickers_str})",
            "rs_rating": "not.is.null",
            "order": "rs_rating.desc,rs_raw.desc",
            "limit": str(n),
        })

        for r in ratings:
            r["industry"] = ticker_industry.get(r["ticker"])
        return ratings

    def industry_top(self, industry, n=20, date=None):
        """Get top N stocks within a specific industry.

        Args:
            industry: Industry name (e.g., ``"Semiconductors"``).
            n: Number of results.
            date: Date string. If *None*, uses latest.

        Returns:
            A *list* of dicts with ``ticker``, ``rs_rating``, ``rs_raw``.
        """
        date = date or self._latest_date()
        if not date:
            return []

        industry_tickers = self._request("tickers", {
            "select": "ticker",
            "industry": f"eq.{industry}",
        })
        if not industry_tickers:
            return []

        tickers_str = ",".join(t["ticker"] for t in industry_tickers)

        return self._request("rs", {
            "select": "ticker,rs_rating,rs_raw",
            "date": f"eq.{date}",
            "ticker": f"in.({tickers_str})",
            "rs_rating": "not.is.null",
            "order": "rs_rating.desc,rs_raw.desc",
            "limit": str(n),
        })

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _latest_date(self):
        """Return the latest date that has non-null rs_rating data."""
        latest = self._request("rs", {
            "select": "date",
            "order": "date.desc",
            "limit": "1",
            "rs_rating": "not.is.null",
        })
        return latest[0]["date"] if latest else None

    def _request(self, table, params):
        """Make a GET request to the Supabase REST API."""
        query = urllib.parse.urlencode(params)
        url = f"{self._base}/{table}?{query}"

        req = urllib.request.Request(url, headers=self._headers)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            raise RuntimeError(f"Supabase API error {e.code}: {body}") from e
        except urllib.error.URLError as e:
            raise ConnectionError(f"Failed to connect to Supabase: {e}") from e
