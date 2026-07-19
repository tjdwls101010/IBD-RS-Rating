# API Reference

Complete reference for the `rs_rating` Python client — all 15 public methods of the `RS` class. For a guided introduction, start with [Getting Started](Getting-Started.md).

```python
from rs_rating import RS
rs = RS()
```

## Conventions

These hold across every method, so they aren't repeated below.

- **Return types are plain `list` and `dict`.** No custom classes. Pass results straight to `pandas.DataFrame(...)`.
- **Ticker symbols are case-insensitive.** `"nvda"` and `"NVDA"` are equivalent.
- **Dates are `"YYYY-MM-DD"` strings.**
- **`date=None` means "the latest date that has ratings"**, resolved with an extra request. Pass an explicit date in loops to avoid repeating that lookup.
- **`rs_rating` may be `None`** on rows where a rating was withheld. See [Concepts](Concepts.md#universe-threshold).
- **No results is an empty list**, not an error. The exception is `get()`, which returns `None`.

---

## Constructor

### `RS(url=None, auth_url=None)`

Create a client. Both arguments default to the public hosted endpoint, so `RS()` is the normal usage.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `url` | `str` | Public Neon Data API | PostgREST-compatible base URL |
| `auth_url` | `str` | Public Neon Auth | Base URL exposing `GET /token/anonymous` |

```python
rs = RS()                                              # public endpoint
rs = RS(url="https://my-host/rest/v1",
        auth_url="https://my-auth/auth")               # self-hosted
```

The constructor makes no network calls — the first actual request triggers token acquisition. Tokens last one hour, are cached on the instance, and refresh automatically 60 seconds before expiry. Reuse one `RS` instance rather than constructing per call, so the token cache is shared.

> **Changed in 0.4.0.** The signature was `RS(url=None, key=None)` against Supabase. Neon issues short-lived tokens automatically and there is no static key to supply, so `key` was replaced by `auth_url`. Code passing `key=` must be updated.

---

## Single stock

### `get(ticker, date=None) → dict | None`

The rating for one stock on one date.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ticker` | `str` | required | Stock symbol |
| `date` | `str` | `None` | Specific date; `None` gives that ticker's most recent row |

```python
rs.get("NVDA")
# {'ticker': 'NVDA', 'date': '2026-03-19', 'close': 121.4, 'rs_raw': 0.1666, 'rs_rating': 70}

rs.get("NVDA", date="2026-03-01")
rs.get("NOTATICKER")
# None
```

Returns `None` when the ticker isn't tracked, or when it has no row on the requested date.

Note this returns the ticker's latest row, which is not necessarily the market's latest date — a ticker that stopped updating returns its last available row. Compare `result["date"]` against `dates()["last"]` if that distinction matters.

### `history(ticker, start=None, end=None, days=30) → list[dict]`

A ticker's rating over time.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ticker` | `str` | required | Stock symbol |
| `start` | `str` | `None` | Start date; when set, results come back oldest-first |
| `end` | `str` | `None` | End date; only meaningful with `start` |
| `days` | `int` | `30` | Most recent N rows. **Ignored when `start` is set** |

```python
rs.history("NVDA")                                       # last 30, newest first
rs.history("NVDA", days=90)
rs.history("NVDA", start="2026-01-01")                   # from date, oldest first
rs.history("NVDA", start="2026-01-01", end="2026-03-01")
```

```python
[{'date': '2026-03-19', 'rs_raw': 0.17, 'rs_rating': 70}, ...]
```

**Ordering flips depending on arguments** — newest-first with `days`, oldest-first with `start`. Sort explicitly if you need certainty:

```python
h = sorted(rs.history("NVDA", days=90), key=lambda r: r["date"])
```

---

## Market-wide queries

### `top(n=20, date=None) → list[dict]`

The highest-rated stocks. Ties on `rs_rating` are broken by `rs_raw` descending.

```python
rs.top(10)
# [{'ticker': 'MU', 'rs_rating': 99, 'rs_raw': 1.99}, ...]
```

Because ratings are integers 1–99 and the universe is ~4,600 stocks, roughly 46 stocks share rating 99 — hence the `rs_raw` tiebreak, which gives a strict ordering within the top bucket.

### `bottom(n=20, date=None) → list[dict]`

The lowest-rated stocks, ascending. Same shape as `top()`.

### `filter(min_rating=None, max_rating=None, date=None) → list[dict]`

Every stock within a rating range, sorted by rating descending. Both bounds are inclusive and either may be omitted.

```python
rs.filter(min_rating=90)                    # top decile — around 460 stocks
rs.filter(min_rating=80, max_rating=95)
rs.filter(max_rating=10)                    # weakest
```

This is unpaginated — `min_rating=1` returns the entire universe in one response. Keep the range narrow.

### `compare(tickers, date=None) → list[dict]`

Several named stocks side by side on one date, sorted by rating descending, nulls last.

```python
rs.compare(["NVDA", "AMD", "AVGO", "INTC"])
# [{'ticker': 'AVGO', 'rs_rating': 85, 'rs_raw': 0.51}, ...]
```

Tickers with no row on that date are simply absent — the result can be shorter than the input, so don't zip them positionally.

### `reference(date=None) → list[dict]`

SPY and QQQ, which are ranked alongside individual stocks rather than excluded.

```python
rs.reference()
# [{'ticker': 'QQQ', 'rs_raw': 0.063, 'rs_rating': 58, 'date': '2026-03-19'},
#  {'ticker': 'SPY', 'rs_raw': 0.049, 'rs_rating': 46, 'date': '2026-03-19'}]
```

Useful as a market-regime read: a stock at RS 60 while SPY sits at 46 is meaningfully ahead of the index. See [Concepts](Concepts.md#reference-ticker).

### `movers(days=5, n=20, direction="up") → list[dict]`

Stocks whose *rating* moved most over recent trading days — change in relative position, not in price.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `days` | `int` | `5` | Lookback in trading days |
| `n` | `int` | `20` | Results to return |
| `direction` | `str` | `"up"` | `"up"` for gainers, `"down"` for losers |

```python
rs.movers(days=5, n=10)
# [{'ticker': 'XYZ', 'rs_rating': 85, 'prev_rating': 60, 'change': 25}, ...]

rs.movers(days=20, n=10, direction="down")
```

Often more actionable than `top()`: a stock climbing 60 → 85 is gaining strength, while one sitting at 99 for months may be late in its move.

Trading days are resolved from SPY's history, so `days=5` means five actual sessions, correctly skipping weekends and holidays. Returns `[]` if the database doesn't hold `days + 1` sessions yet. Any `direction` value other than `"down"` (case-insensitive) is treated as `"up"`.

This method makes three requests and does the diff client-side. Cache the result rather than calling it repeatedly.

### `dates() → dict`

The available range.

```python
rs.dates()
# {'first': '2025-03-21', 'last': '2026-03-19'}
```

`{'first': None, 'last': None}` means the database has no ratings at all. `first` reflects the end of the initial [warm-up](Concepts.md#warm-up) period.

Use this as a freshness check — if `last` is well behind the most recent weekday, the pipeline is behind.

---

## Sector and industry

Sector and industry come from the Finviz screener and live in a separate `tickers` table, so these methods join client-side across two requests.

### `sectors() → list[str]`

Every sector name, sorted.

```python
rs.sectors()
# ['Basic Materials', 'Communication Services', 'Consumer Cyclical', ...]
```

### `industries(sector=None) → list[str]`

Every industry name, optionally within one sector.

```python
rs.industries()
rs.industries("Technology")
# ['Communication Equipment', 'Computer Hardware', 'Consumer Electronics', ...]
```

Pass a sector name exactly as `sectors()` returns it — matching is exact.

### `sector_ranking(date=None) → list[dict]`

Sectors ranked by mean RS Rating.

```python
rs.sector_ranking()
# [{'sector': 'Energy', 'avg_rs': 78.7, 'count': 210}, ...]
```

`count` is how many rated stocks went into the average — worth checking, since a sector with 12 constituents produces a much noisier mean than one with 400.

### `industry_ranking(date=None, sector=None) → list[dict]`

The same, at industry granularity, optionally scoped to one sector.

```python
rs.industry_ranking()
# [{'industry': 'Oil & Gas Drilling', 'sector': 'Energy', 'avg_rs': 92.0, 'count': 15}, ...]

rs.industry_ranking(sector="Technology")
```

Industries are narrow, so small `count` values are common and averages are correspondingly noisy. O'Neil's work put roughly half of a stock's move down to its industry group, which is what makes this worth looking at despite the noise.

### `sector_top(sector, n=20, date=None) → list[dict]`

The strongest stocks inside one sector. Each result carries its `industry`.

```python
rs.sector_top("Technology", n=5)
# [{'ticker': 'AXTI', 'rs_rating': 99, 'rs_raw': 14.79, 'industry': 'Semiconductor Equipment & Materials'}, ...]
```

Returns `[]` for an unknown sector name.

### `industry_top(industry, n=20, date=None) → list[dict]`

The strongest stocks inside one industry.

```python
rs.industry_top("Semiconductors", n=5)
# [{'ticker': 'MU', 'rs_rating': 98, 'rs_raw': 1.99}, ...]
```

Returns `[]` for an unknown industry name. Combined with `industry_ranking()`, this is the standard two-step: find the leading group, then the leaders within it.

---

## Errors

The client raises two exception types and does not define custom ones.

| Exception | Raised when | Typical cause |
|---|---|---|
| `RuntimeError` | The API returned an HTTP error status | Malformed query, endpoint misconfiguration, server-side failure. Message includes status code and response body. |
| `ConnectionError` | The request never completed | No network, DNS failure, timeout, unreachable endpoint |

```python
from rs_rating import RS

try:
    data = RS().top(10)
except ConnectionError:
    ...      # network problem — retrying may help
except RuntimeError as e:
    ...      # API rejected the request — retrying probably won't
```

Timeouts are 15 seconds for token acquisition and 30 seconds for data requests. There is no built-in retry; wrap calls yourself if you need one.

---

## Performance notes

Each method is one or more HTTP round trips with no client-side caching beyond the auth token.

**Pass an explicit date in loops.** Any method with `date=None` spends an extra request resolving the latest date:

```python
latest = rs.dates()["last"]
for sector in rs.sectors():
    rs.sector_top(sector, date=latest)     # 1 lookup instead of N
```

**Fetch broadly, then filter locally**, rather than calling per ticker:

```python
# Instead of: [rs.get(t) for t in watchlist]      → N requests
snapshot = {r["ticker"]: r for r in rs.filter(min_rating=1)}   # 1 request
```

**Reuse the instance.** A new `RS()` starts with an empty token cache and pays for a fresh token on its first call.

**Methods with multiple round trips:** `movers()` (3), `sector_ranking()` / `industry_ranking()` / `sector_top()` / `industry_top()` (2 each, plus 1 if `date` is omitted).

## Next

- [Concepts](Concepts.md) — what these numbers mean
- [Getting Started](Getting-Started.md) — worked examples
- [FAQ](FAQ.md) — nulls, gaps, and accuracy
- [CLI Reference](CLI-Reference.md) — the engine side

[← Back to index](README.md)
