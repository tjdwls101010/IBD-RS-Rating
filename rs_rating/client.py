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
        """Get reference tickers (SPY, QQQ) RS Raw scores.

        Reference tickers have ``rs_rating`` set to *null*.

        Returns:
            A *list* of dicts with ``ticker``, ``rs_raw``, ``date``.
        """
        if not date:
            latest = self._request("rs", {
                "select": "date",
                "order": "date.desc",
                "limit": "1",
                "rs_rating": "is.null",
            })
            if not latest:
                return []
            date = latest[0]["date"]

        params = {
            "select": "ticker,rs_raw,date",
            "date": f"eq.{date}",
            "rs_rating": "is.null",
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

        # Get all available dates to find the date N trading days ago
        dates = self._request("rs", {
            "select": "date",
            "date": f"lte.{today}",
            "rs_rating": "not.is.null",
            "order": "date.desc",
            "limit": str(days + 1),
        })
        if len(dates) < 2:
            return []
        prev_date = dates[-1]["date"]

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
