# Troubleshooting

Symptoms mapped to causes and fixes. Client problems first, then engine problems.

For "why is this number like that?" questions where nothing is actually broken, see [FAQ](FAQ.md).

---

## Client problems

### `ModuleNotFoundError: No module named 'rs_rating'`

The package is `ibd-rs-rating` on PyPI but imports as `rs_rating`:

```bash
pip install ibd-rs-rating
```

```python
from rs_rating import RS      # not "import ibd_rs_rating"
```

If it's installed and still not found, you likely have multiple Python environments:

```bash
which python && python -c "import rs_rating; print(rs_rating.__file__)"
```

### `TypeError: __init__() got an unexpected keyword argument 'key'`

Code written for 0.3.x. The 0.4.0 backend migration replaced the static key with automatic token acquisition:

```python
rs = RS(url=..., key=...)        # 0.3.x — no longer valid
rs = RS(url=..., auth_url=...)   # 0.4.0+
```

Most code just needs `RS()`. See [API Reference](API-Reference.md#constructor).

### `ConnectionError: Failed to connect to the Neon Data API`

The request never completed — network, DNS, firewall, or the endpoint being down.

```bash
curl -sI https://ep-shiny-bread-ato0bxsg.apirest.c-9.us-east-1.aws.neon.tech/neondb/rest/v1/
```

Behind a corporate proxy, `urllib` respects `HTTPS_PROXY`:

```bash
export HTTPS_PROXY=http://proxy.example.com:8080
```

Retrying may help; this is a transport failure, not a rejection.

### `RuntimeError: Neon Data API error 4xx: ...`

The API rejected the request; the message carries the status and body.

| Status | Cause |
|---|---|
| 400 | Malformed query — usually an unusual character in a sector or industry name |
| 401 / 403 | Token problem. On the public endpoint, likely transient; construct a fresh `RS()` to force a new token |
| 404 | Wrong base URL, or a self-hosted endpoint missing the `rs` / `tickers` tables |

Retrying usually won't help — the request itself was rejected.

### Data looks stale

```python
rs.dates()["last"]
```

If that's more than a couple of days behind the last weekday, the upstream pipeline is behind, not your client. There's no client-side cache to clear. Check [the workflow runs](https://github.com/tjdwls101010/IBD-RS-Rating/actions/workflows/daily_update.yml) — a red run explains it.

If it persists for several days, open an issue. If you need availability guarantees, [self-host](Operations.md).

### `get()` returns `None`

In order of likelihood:

1. **Not in the universe** — under $50M market cap, an ETF, or a shell company. Confirm: `"XYZ" in {r["ticker"] for r in rs.filter(min_rating=1)}`.
2. **Still in warm-up** — under 252 trading days of history, so no rating exists yet.
3. **Wrong date** — `rs.get("NVDA", date="2026-03-21")` on a weekend or holiday returns `None`.
4. **Delisted or renamed** — the ticker exists historically but has no recent rows.

### `rs_rating` is `None` but `rs_raw` has a value

Not a bug — that exact combination is the signature of a date whose coverage fell below the 90% universe threshold. RS Raw is per-stock and remains valid; the rating depends on the population and was withheld. See [Concepts](Concepts.md#universe-threshold).

Handle it explicitly:

```python
rated = [r for r in results if r["rs_rating"] is not None]
```

### Calls are slow

Each method is one or more HTTP round trips with no caching. The usual culprit is a per-ticker loop:

```python
# Slow: N requests
[rs.get(t) for t in watchlist]

# Fast: 1 request
snapshot = {r["ticker"]: r for r in rs.filter(min_rating=1)}
[snapshot.get(t) for t in watchlist]
```

Also pass an explicit `date=` inside loops to skip the repeated latest-date lookup, and reuse one `RS` instance so the token cache is shared. See [API Reference](API-Reference.md#performance-notes).

---

## Engine problems

### `ModuleNotFoundError: No module named 'pandas'` (or `yfinance`, `finvizfinance`)

The engine extras aren't installed. The base package is deliberately dependency-free:

```bash
pip install -e ".[engine]"        # add ",pg" for Postgres
```

### `ModuleNotFoundError: No module named 'psycopg2'`

`DATABASE_URL` is set but the Postgres driver isn't installed:

```bash
pip install -e ".[engine,pg]"
```

Or unset `DATABASE_URL` to fall back to SQLite.

### `init` exits 1: "Refusing to init from an untrusted universe"

The screener fetch failed validation, and `init` won't build a database on a bad universe. The reason line says which guard tripped:

| Message | Cause | Fix |
|---|---|---|
| `below absolute floor 3000` | Fetch returned almost nothing | Finviz is blocking or down — wait and retry |
| `below 90% of last-good count` | Meaningful shrinkage | Retry; if it persists, the universe may genuinely have changed |
| `below 98% of Finviz's reported total (likely truncated)` | Partial pagination | Retry — usually transient |

Retry after a delay first; scrapers get blocked temporarily. If it persists for hours, Finviz may have changed its page structure — worth an issue.

### `update` exits 1 with `Result: FAIL`

The completeness check rejected the run. The report says which:

```
Close coverage: 2103/4642 (45.3%)
Threshold: 90.0%
Result: FAIL (close_coverage_below_threshold)
```

| Reason | Meaning | Fix |
|---|---|---|
| `close_coverage_below_threshold` | Too many tickers missing prices | Usually rate limiting — see below. Often self-heals next run via the trailing window. |
| `no_price_data` | No prices at all on the latest date | Download stage failed entirely; check logs above |
| `universe_unknown` | Universe size resolved to zero | Cache is empty and the fetch failed |

**This is the system working.** It declined to publish ratings computed against a partial population, and it told you. Run again after the underlying issue clears; the trailing window backfills up to 10 days automatically.

### Many tickers failing to download

Almost always yfinance rate limiting. It's a free unofficial endpoint with no published quota.

```python
# ibd_rs/config.py
BATCH_SIZE = 50                  # from 75
INTER_BATCH_SLEEP_SECONDS = 5    # from 2
DOWNLOAD_THREADS = 2             # from 4
```

Run with `-v` to see which stage is failing. A slower run that completes beats a fast one that gets blocked.

### `SSL connection has been closed unexpectedly`

The database session was terminated mid-run — typically a serverless instance auto-suspending during a long gap between writes.

`db.reconnect()` guards the known long gap (the Finviz scrape). If you hit this somewhere else, that's a genuine bug worth reporting with the traceback, since it means a gap exists that isn't guarded.

Workaround: disable auto-suspend on your database, or shorten the run by raising `BATCH_SIZE` if rate limits allow.

### `update` times out

The daily workflow allows 30 minutes; a healthy run takes about 3.

Check in order:

1. **Retention running?** Step 5 should report pruned records. If it's always 0 and the price range spans years, retention isn't working and every pivot is oversized.
2. **`status` price range** — should be ~13 months. Much wider means unbounded growth.
3. **Rate limiting** — retries with backoff can stretch a run substantially.

```sql
SELECT MIN(date), MAX(date), COUNT(*) FROM rs WHERE close IS NOT NULL;
```

### Ratings are missing for recent dates

Query what's actually stored:

```sql
SELECT date,
       COUNT(*) FILTER (WHERE rs_raw IS NOT NULL)    AS raw,
       COUNT(*) FILTER (WHERE rs_rating IS NOT NULL) AS rated
FROM rs
WHERE date > '2026-03-01'
GROUP BY date ORDER BY date DESC;
```

| Pattern | Meaning |
|---|---|
| `raw` high, `rated` 0 | Universe threshold rejected the date — coverage was below 90% |
| Both 0 | RS computation didn't run, or prices are missing |
| Both high | Ratings exist; the problem is in your query |

For the first case, fix the price gap and the [recompute window](Concepts.md#self-healing-recompute-window) re-rates it within 15 trading days. Beyond that window, `recalc` rebuilds everything.

### Ratings look wrong after a config change

Changing `RS_WEIGHTS` or `RS_UNIVERSE_THRESHOLD` doesn't retroactively update stored data — old ratings keep their old meaning, so the history becomes internally inconsistent.

```bash
python -m ibd_rs recalc
```

This rebuilds every rating from stored prices without downloading. Back up first.

### A ticker's prices look wrong after a split

Split repair runs every update, but it needs the split to appear in yfinance's calendar within 7 days (`SPLIT_LOOKBACK_DAYS`).

Manual repair:

```python
from ibd_rs import db, splits
conn = db.get_connection()
splits.verify_and_repair(conn, ["XYZ"])
```

Then `recalc`, since the bad prices already fed into ratings.

If the split is older than the lookback window, force a full re-download of that ticker by clearing its prices (`db.delete_ticker_prices`) and running `init`.

---

## Diagnostic checklist

When something is wrong and you don't know where:

```bash
# 1. What does the database actually contain?
python -m ibd_rs status

# 2. Are recent dates rated?
python -m ibd_rs lookup SPY --days 10

# 3. Does a verbose run reveal the failing stage?
python -m ibd_rs -v update

# 4. Is the universe intact?
python -c "from ibd_rs import db; c=db.get_connection(); print(len(db.get_meta(c,'ticker_list').split(',')))"
```

Step 4 should print roughly 4,600. A much smaller number means the universe cache is poisoned — clear it and let the next run refetch:

```sql
DELETE FROM meta WHERE key IN ('ticker_list', 'ticker_list_date');
```

## Still stuck

Open an [issue](https://github.com/tjdwls101010/IBD-RS-Rating/issues) with:

- The command or code you ran, and its full output (`-v` for engine problems)
- `python -m ibd_rs status` output, for engine problems
- Python version, package version, and whether you're self-hosting
- Ticker and date, for data problems — that makes it directly checkable

Security issues go to [SECURITY.md](../../SECURITY.md), not the issue tracker.

## Next

- [FAQ](FAQ.md) — questions where nothing is broken
- [Operations](Operations.md) — monitoring to catch these earlier
- [Concepts](Concepts.md) — why nulls and gaps are intentional

[← Back to index](README.md)
