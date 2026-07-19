# Getting Started

From nothing to a real result. There are two ways to use this project, and they have almost nothing in common — pick the one that matches what you want.

- **[Reading ratings](#path-1-reading-ratings)** — install a package, call a method, get data. Two minutes.
- **[Running the engine](#path-2-running-the-engine)** — build your own rating database from scratch. Thirty minutes, mostly waiting.

Most people want the first.

---

## Path 1: Reading ratings

### Prerequisites

Python 3.10 or newer, and an internet connection. That's the whole list — the client has no dependencies and needs no account, API key, or configuration.

```bash
python --version    # must be 3.10+
```

### Install

```bash
pip install ibd-rs-rating
```

The package is named `ibd-rs-rating`; the module you import is `rs_rating`. (The repository also contains an `ibd_rs` package — that's the engine, covered in Path 2, and it is not installed by this command's dependencies.)

### First result

```python
from rs_rating import RS

rs = RS()
print(rs.get("NVDA"))
```

Expected output, with different numbers and a more recent date:

```python
{'ticker': 'NVDA', 'date': '2026-03-19', 'close': 121.4, 'rs_raw': 0.1666, 'rs_rating': 70}
```

If you got a dict like that, everything works. The first call fetched an anonymous access token behind the scenes and cached it for the session; subsequent calls reuse it.

**Reading the result:** `rs_rating: 70` means NVDA outperformed 70% of the roughly 4,600 stocks in the universe over the trailing year, weighted toward recent months. `rs_raw: 0.1666` is the underlying weighted return, about 16.7% — useful for fine-grained comparison, but the rating is the number that means something on its own. See [Concepts](Concepts.md).

### Verify it's live

Data should be current to the last completed US trading day:

```python
rs.dates()
# {'first': '2025-03-21', 'last': '2026-03-19'}
```

`last` should be within a day or two of the most recent weekday. If it's weeks stale, see [Troubleshooting](Troubleshooting.md).

`first` is when ratings begin — the point where enough price history had accumulated to clear the 252-trading-day warm-up.

### A real question

Finding the strongest stocks in the market right now:

```python
from rs_rating import RS

rs = RS()

for stock in rs.top(10):
    print(f"{stock['ticker']:6} RS {stock['rs_rating']:2}  raw {stock['rs_raw']:+.2%}")
```

```
MU     RS 99  raw +199.31%
AXTI   RS 99  raw +1479.00%
...
```

Narrowing to one industry, since group strength tends to matter:

```python
# Which sectors are leading?
for s in rs.sector_ranking()[:5]:
    print(f"{s['sector']:24} avg RS {s['avg_rs']:5.1f}  ({s['count']} stocks)")

# The strongest names inside one
for stock in rs.industry_top("Semiconductors", n=5):
    print(stock["ticker"], stock["rs_rating"])
```

Checking whether a stock's strength is improving or fading:

```python
history = rs.history("NVDA", days=30)
print(f"30 days ago: RS {history[-1]['rs_rating']}")
print(f"Today:       RS {history[0]['rs_rating']}")
```

`history` returns newest-first by default, which is why the oldest entry is at index `-1`.

Finding stocks whose ratings are climbing fastest — often more interesting than the ones already at 99:

```python
for m in rs.movers(days=5, n=10):
    print(f"{m['ticker']:6} {m['prev_rating']:2} → {m['rs_rating']:2}  (+{m['change']})")
```

### Into pandas

Every method returns plain lists of dicts, so:

```python
import pandas as pd
from rs_rating import RS

df = pd.DataFrame(RS().filter(min_rating=90))
print(df.describe())
```

### What next

- [API Reference](API-Reference.md) — all 15 methods with full parameters
- [Concepts](Concepts.md) — what the numbers actually mean, and why some are missing
- [FAQ](FAQ.md) — accuracy versus official IBD ratings

---

## Path 2: Running the engine

Build your own rating database instead of reading the hosted one. Worth doing if you need guaranteed availability, want to change the universe or the formula, or don't want a dependency on someone else's free endpoint.

### Prerequisites

- Python 3.10+ (CI runs 3.12)
- ~30 minutes for the initial load, most of it waiting on downloads
- A stable connection — the initial load makes several thousand requests
- Optionally, a Postgres database. Without one, everything runs on local SQLite.

### Install

```bash
git clone https://github.com/tjdwls101010/IBD-RS-Rating.git
cd IBD-RS-Rating

python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate

pip install -e ".[engine]"       # add ",pg" if you'll use Postgres: ".[engine,pg]"
```

`engine` brings in pandas, yfinance, finvizfinance, and requests. `pg` adds the Postgres driver. Neither is needed for the client in Path 1 — that separation is why the published client stays dependency-free.

Confirm the CLI is available:

```bash
ibd-rs --help
```

`python -m ibd_rs` does the same thing and is what the CI workflows use.

### Choose a backend

**SQLite (default).** Do nothing. Data goes to `data/rs.db`. Good for trying it out and for local analysis.

**Postgres.** Set the connection string:

```bash
export DATABASE_URL="postgresql://user:password@host/dbname"
```

Whenever `DATABASE_URL` is set, it wins. To fall back to SQLite, unset it. Schema creation is automatic on both backends — there is no separate migration step.

### Initial load

```bash
python -m ibd_rs init
```

Four steps, roughly 20–30 minutes:

```
Step 1/4: Fetching ticker list from Finviz...
  Found 4642 tickers
Step 2/4: Downloading 2-year price history (4642 tickers)...
  This may take 20-30 minutes.
Step 3/4: Checking for stock splits...
Step 4/4: Calculating RS ratings...
  Computed 1284102 RS records
```

Two years of history is downloaded because one year of *ratings* requires one year of history plus the 252-trading-day lookback that produces them.

`init` refuses to run if the ticker universe fails validation — it exits with status 1 rather than building a database on a bad universe. Building on 55 tickers instead of 4,600 would produce a fully-populated database of meaningless ratings, which is much harder to notice than a failure. See [Concepts](Concepts.md#universe-validation).

Some tickers will fail to download; a warning is normal and the run continues.

### Verify

```bash
python -m ibd_rs status
```

```
Database Status
========================================
Price records:    2,341,208
Price tickers:         4,598
Price range:     2024-03-20 — 2026-03-19
RS records:       1,284,102
RS tickers:            4,591
RS range:        2025-03-21 — 2026-03-19
Last update:     2026-03-19
```

Sanity checks:

- **Price tickers** near 4,600. Far below means the universe fetch was degraded.
- **RS range start** roughly a year after the price range start — that gap is warm-up, and its absence means something is wrong.
- **RS tickers** close to price tickers. A large shortfall means many stocks lack sufficient history.

Then look at actual output:

```bash
python -m ibd_rs top 20
python -m ibd_rs lookup NVDA
```

### Daily updates

```bash
python -m ibd_rs update
```

Six steps, around 3 minutes. It re-downloads a trailing 10-day window for every ticker, repairs splits, recomputes recent ratings, applies retention, and finally checks data completeness.

**Exit code matters here.** `update` exits 1 when the universe fetch was untrusted or when coverage on the latest trading day is below 90%. That non-zero exit is the entire stall-detection mechanism — if you schedule this, make sure the failure surfaces somewhere you'll see. See [Operations](Operations.md).

Run it after the US close (roughly 21:00 UTC), on weekdays.

### Exporting

```bash
python -m ibd_rs export
```

Writes `data/rs_ratings_<date>.csv` and refreshes `data/tickers.csv` — the full latest-date snapshot with sector and industry.

### Pointing the client at your own database

To have `rs_rating` read from your instance instead of the public one, expose it over a PostgREST-compatible endpoint and pass the URLs in:

```python
rs = RS(url="https://your-endpoint/rest/v1", auth_url="https://your-auth/auth")
```

The client expects PostgREST query syntax and a `GET /token/anonymous` endpoint returning `{"token": ..., "expires_at": ...}`. If your setup doesn't need auth, query the database directly — for self-hosting, reading Postgres with `psycopg2` is usually simpler than replicating the HTTP layer.

### What next

- [CLI Reference](CLI-Reference.md) — every command in detail
- [Operations](Operations.md) — scheduling, monitoring, reliability guards
- [Data Pipeline](Data-Pipeline.md) — what each step does internally
- [Architecture](Architecture.md) — how it all fits together

---

## Next

- [Concepts](Concepts.md) — the vocabulary behind every number here
- [API Reference](API-Reference.md) — the complete client surface
- [Troubleshooting](Troubleshooting.md) — when the above didn't go as described

[← Back to index](README.md)
