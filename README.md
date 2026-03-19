<div align="center">

<img src="https://investors.com/wp-content/themes/ibd/dist/images/ibd-placeholder.png" width="120" alt="RS Rating">

# IBD-Style Relative Strength Rating

**Unofficial IBD-style RS Rating for 4,600+ US stocks, updated daily.**

The only open-source project that provides **true percentile-ranked RS Ratings (1-99)** — not just weighted returns.

[![Daily Update](https://img.shields.io/badge/updated-daily-brightgreen)](#)
[![Stocks](https://img.shields.io/badge/stocks-4%2C600%2B-blue)](#)
[![Python](https://img.shields.io/badge/python-3.10%2B-yellow)](#)
[![License: MIT](https://img.shields.io/badge/license-MIT-lightgrey)](#)

[Installation](#installation) · [Quick Start](#quick-start) · [API Reference](#api-reference) · [How It Works](#how-it-works)

</div>

---

## Why This Project?

IBD's Relative Strength (RS) Rating is one of the most powerful tools for momentum investing — used by William O'Neil, Mark Minervini, and thousands of growth investors. **But IBD doesn't provide it for free**, and existing open-source alternatives only calculate weighted returns without the crucial **percentile ranking** step.

**This project solves that.** We calculate true RS Ratings (1-99) for 4,600+ US stocks daily:

- **RS 99** = outperforming 99% of all stocks over the past year
- **RS 50** = median performer
- **RS 1** = bottom 1%

```python
from rs_rating import RS

rs = RS()
rs.get("NVDA")
# {'ticker': 'NVDA', 'date': '2026-03-19', 'rs_raw': 0.1666, 'rs_rating': 70}
```

No API key needed. No rate limits. Just `pip install` and go.

---

## Installation

```bash
pip install ibd-rs-rating
```

**Zero dependencies** — uses only Python standard library (`urllib`, `json`).

---

## Quick Start

```python
from rs_rating import RS

rs = RS()

# Get latest RS Rating for a stock
rs.get("AAPL")
# {'ticker': 'AAPL', 'date': '2026-03-19', 'rs_raw': 0.2841, 'rs_rating': 72}

# Get RS Rating for a specific date
rs.get("AAPL", date="2026-03-01")

# Top 10 stocks by RS Rating
rs.top(10)
# [{'ticker': 'MU', 'rs_rating': 99, 'rs_raw': 1.99}, ...]

# RS Rating history (last 30 days)
rs.history("NVDA")
# [{'date': '2026-03-19', 'rs_raw': 0.17, 'rs_rating': 70}, ...]

# Compare multiple stocks
rs.compare(["NVDA", "AMD", "AVGO", "INTC"])
# [{'ticker': 'AVGO', 'rs_rating': 85}, {'ticker': 'NVDA', 'rs_rating': 70}, ...]

# Filter: stocks with RS ≥ 90
rs.filter(min_rating=90)
# Returns all stocks in the top 10%

# SPY & QQQ benchmark RS (raw score, not ranked)
rs.reference()
# [{'ticker': 'SPY', 'rs_raw': 0.049}, {'ticker': 'QQQ', 'rs_raw': 0.063}]

# Stocks with biggest RS Rating improvement (last 5 trading days)
rs.movers(days=5, n=10)
# [{'ticker': 'XYZ', 'rs_rating': 85, 'prev_rating': 60, 'change': 25}, ...]

# Biggest RS losers
rs.movers(days=5, n=10, direction="down")

# Available date range
rs.dates()
# {'first': '2025-03-21', 'last': '2026-03-19'}
```

---

## API Reference

### `RS(url=None, key=None)`

Create a client instance. No arguments needed — connects to the public API by default.

### `.get(ticker, date=None) → dict | None`

Get RS rating for a single ticker. Returns latest if no date specified.

| Parameter | Type | Description |
|-----------|------|-------------|
| `ticker` | str | Stock symbol (case-insensitive) |
| `date` | str | Optional. `"YYYY-MM-DD"` format |

### `.history(ticker, start=None, end=None, days=30) → list`

Get RS rating history for a ticker.

| Parameter | Type | Description |
|-----------|------|-------------|
| `ticker` | str | Stock symbol |
| `start` | str | Start date `"YYYY-MM-DD"` |
| `end` | str | End date `"YYYY-MM-DD"` |
| `days` | int | Recent days (default: 30, ignored if start is set) |

### `.top(n=20, date=None) → list`

Get top N stocks ranked by RS Rating.

### `.bottom(n=20, date=None) → list`

Get bottom N stocks ranked by RS Rating.

### `.filter(min_rating=None, max_rating=None, date=None) → list`

Filter stocks by RS Rating range.

```python
# Stocks with RS between 80 and 95
rs.filter(min_rating=80, max_rating=95)
```

### `.compare(tickers, date=None) → list`

Compare RS ratings for a list of tickers, sorted by rating descending.

```python
rs.compare(["AAPL", "MSFT", "GOOG", "AMZN", "META"])
```

### `.reference(date=None) → list`

Get RS raw scores for benchmark indices (SPY, QQQ). These are not percentile-ranked — they provide a baseline to compare individual stocks against the market.

### `.movers(days=5, n=20, direction="up") → list`

Get stocks with the biggest RS Rating change over recent trading days. Perfect for finding emerging momentum leaders.

| Parameter | Type | Description |
|-----------|------|-------------|
| `days` | int | Lookback period in trading days (default: 5) |
| `n` | int | Number of results (default: 20) |
| `direction` | str | `"up"` for gainers, `"down"` for losers |

```python
rs.movers(days=5, n=10, direction="up")
# [{'ticker': 'XYZ', 'rs_rating': 85, 'prev_rating': 60, 'change': 25}, ...]
```

### `.dates() → dict`

Get the available date range for RS data.

```python
rs.dates()
# {'first': '2025-03-21', 'last': '2026-03-19'}
```

---

## How It Works

### The Formula

RS Rating follows IBD's reverse-engineered methodology:

```
RS Raw = 0.4 × ROC(63) + 0.2 × ROC(126) + 0.2 × ROC(189) + 0.2 × ROC(252)
```

Where `ROC(n)` = cumulative price return over the last `n` trading days.

This gives **5x more weight to the most recent quarter** compared to the oldest quarter — designed to catch stocks with accelerating momentum.

| Quarter | Effective Weight |
|---------|-----------------|
| Most recent (0-3 months) | 100% |
| 2nd quarter (3-6 months) | 60% |
| 3rd quarter (6-9 months) | 40% |
| Oldest (9-12 months) | 20% |

The raw score is then **percentile-ranked across all ~4,600 stocks** to produce a rating from 1 to 99.

### Data Pipeline

```
Finviz Screener → Ticker list (~4,600 stocks)
       ↓
yfinance → Daily close prices (2 years history)
       ↓
RS calculation → Vectorized pandas computation
       ↓
Supabase PostgreSQL → Stored & served via REST API
       ↓
GitHub Actions → Automated daily update (weekdays, after market close)
```

### Universe

- **~4,600 US-listed stocks** (NYSE, NASDAQ, AMEX)
- Market cap > $50M (micro-cap and above)
- Excludes ETFs and shell companies (SPACs)
- Includes ADRs (BABA, TSM, etc.)
- SPY & QQQ tracked as reference benchmarks

---

## Self-Hosting

Want to run your own instance? The calculation engine is included.

```bash
git clone https://github.com/your-username/IBD-RS-Rating.git
cd IBD-RS-Rating
pip install -e ".[pg]"

# Local mode (SQLite)
python -m ibd_rs init      # Download 2yr data + calculate RS (~30 min)
python -m ibd_rs update    # Daily update (~3 min)
python -m ibd_rs top 20    # View top stocks

# Cloud mode (Supabase)
export DATABASE_URL="postgresql://..."
python -m ibd_rs init      # Loads data into Supabase
```

### CLI Commands

| Command | Description |
|---------|-------------|
| `python -m ibd_rs init` | Initial setup: download data + compute RS |
| `python -m ibd_rs update` | Daily update: new prices + RS recalc |
| `python -m ibd_rs top [N]` | Top N stocks by RS Rating |
| `python -m ibd_rs lookup TICKER` | RS history for a ticker |
| `python -m ibd_rs status` | Database statistics |
| `python -m ibd_rs export` | Export to CSV |

---

## Accuracy

Compared against actual IBD MarketSmith RS Ratings:

| Range | Accuracy | Notes |
|-------|----------|-------|
| RS 90+ | ±1-3 points | Near-exact match for top performers |
| RS 60-90 | ±5-10 points | Systematic offset due to universe size difference |
| RS < 30 | ±3-6 points | Both agree stock is weak |

**Ranking order is consistent** — the same stocks appear at the top. The absolute values may differ slightly because IBD's exact formula and universe are proprietary.

---

## Disclaimer

This project is **not affiliated with Investor's Business Daily (IBD)** or William O'Neil + Co. RS Ratings are calculated using a reverse-engineered approximation of IBD's methodology. For official ratings, subscribe to [IBD MarketSmith](https://marketsmith.investors.com/).

This tool is for **educational and research purposes**. It is not financial advice.

---

## License

MIT
