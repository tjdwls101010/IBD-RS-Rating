<p align="center">
  <img src="https://github.com/tjdwls101010/tjdwls101010/blob/main/Images/IBD%20RS%20Rating.png?raw=true" width="640" alt="IBD RS Rating">
</p>

<h1 align="center">IBD RS Rating</h1>

<p align="center">
  <strong>Percentile-ranked relative strength ratings (1&ndash;99) for ~4,600 US stocks, recalculated every trading day.</strong>
</p>

<p align="center">
  <a href="https://pypi.org/project/ibd-rs-rating/"><img src="https://img.shields.io/pypi/v/ibd-rs-rating" alt="PyPI"></a>
  <a href="https://pypi.org/project/ibd-rs-rating/"><img src="https://img.shields.io/pypi/pyversions/ibd-rs-rating" alt="Python versions"></a>
  <a href="LICENSE"><img src="https://img.shields.io/pypi/l/ibd-rs-rating" alt="License: MIT"></a>
  <a href="https://github.com/tjdwls101010/IBD-RS-Rating/actions/workflows/daily_update.yml"><img src="https://github.com/tjdwls101010/IBD-RS-Rating/actions/workflows/daily_update.yml/badge.svg" alt="Daily RS Update"></a>
</p>

<p align="center">
  <a href="#quick-start">Quick start</a> ·
  <a href="docs/wiki/README.md">Documentation</a> ·
  <a href="docs/wiki/API-Reference.md">API reference</a> ·
  <a href="docs/wiki/Concepts.md">How it works</a>
</p>

---

## What this is

Relative Strength (RS) Rating answers one question: **over the past year, did this stock outperform more of the market than that one?** A stock rated 90 outpaced 90% of the roughly 4,600 stocks it was measured against. A stock rated 50 was average. It is the metric William O'Neil built a strategy around, and it is the first filter most momentum investors apply.

Investor's Business Daily publishes RS Ratings behind a paid subscription. Open-source alternatives usually stop halfway — they compute a weighted return and call it a rating, skipping the step that gives the number meaning. **A weighted return tells you a stock rose 18%. A rating tells you that 18% put it in the top 3% of the market.** Only the second one is comparable across stocks, across sectors, and across time.

This project does the full computation: it collects daily closes for the whole US common-stock universe, computes each stock's weighted momentum score, and then ranks every score against every other score on that same trading day to produce a 1–99 rating.

```python
from rs_rating import RS

rs = RS()
rs.get("NVDA")
# {'ticker': 'NVDA', 'date': '2026-03-19', 'close': 121.4, 'rs_raw': 0.1666, 'rs_rating': 70}
```

No account, no API key, no rate limit. The reading client is pure Python standard library — installing it pulls in nothing else.

## Highlights

- **True percentile ranking.** Every rating is a stock's position within the full universe on that date, not a rescaled return.
- **Zero-dependency client.** `rs_rating` uses only `urllib` and `json`, so it drops into any environment without dependency conflicts.
- **Sector and industry analysis.** Rank sectors by average RS, or find the strongest names inside one industry — O'Neil's research attributes roughly half of a stock's move to its industry group.
- **Honest gaps.** A stock with under 252 trading days of history gets no rating rather than a rating built on thin data, and a trading day whose coverage falls below 90% of the universe is left unrated rather than published with a distorted denominator.
- **Self-hostable.** The full calculation engine ships in the same repository. Point it at SQLite for a laptop or any Postgres for production.

## Installation

```bash
pip install ibd-rs-rating
```

Requires Python 3.10 or newer.

## Quick start

```python
from rs_rating import RS

rs = RS()

# One stock, latest rating
rs.get("AAPL")

# The strongest names in the market right now
rs.top(10)

# Everything in the top decile
rs.filter(min_rating=90)

# Head-to-head
rs.compare(["NVDA", "AMD", "AVGO", "INTC"])

# Momentum that is accelerating: biggest rating gains over 5 trading days
rs.movers(days=5, n=10)

# Which sectors are leading?
rs.sector_ranking()
```

Every call returns plain dicts and lists — no custom types to learn, and the output drops straight into `pandas.DataFrame(...)` if you want it there.

The first call fetches a short-lived anonymous access token and caches it for the session, so there is nothing to configure. See [Getting Started](docs/wiki/Getting-Started.md) for a full walkthrough and [API Reference](docs/wiki/API-Reference.md) for all 15 methods.

## How a rating is built

```
RS Raw = 0.4 × ROC(63) + 0.2 × ROC(126) + 0.2 × ROC(189) + 0.2 × ROC(252)
```

`ROC(n)` is the return over that stock's last `n` **valid trading days**. The most recent quarter carries five times the weight of the oldest, which is what makes the score respond to accelerating momentum rather than to a rally that ended nine months ago.

That raw score is then percentile-ranked against every other stock rated on the same date and scaled to 1–99. The ranking step is what turns a private number into a comparable one. [Concepts](docs/wiki/Concepts.md) explains each term; [Architecture](docs/wiki/Architecture.md) explains how the pipeline produces them.

## Data universe

Roughly 4,600 US-listed common stocks (NYSE, NASDAQ, AMEX) with a market cap above $50M, excluding ETFs and shell companies, including ADRs. SPY and QQQ are tracked and ranked alongside individual stocks so you can see where the index itself falls in the distribution.

A snapshot of the latest ratings is committed to [`data/tickers.csv`](data/tickers.csv).

## Self-hosting

The engine that produces the data is in the same repository, and it runs on your own database:

```bash
git clone https://github.com/tjdwls101010/IBD-RS-Rating.git
cd IBD-RS-Rating
pip install -e ".[engine,pg]"

python -m ibd_rs init      # download 2y of history and compute RS (20-30 min)
python -m ibd_rs update    # daily incremental update (~3 min)
python -m ibd_rs top 20    # inspect results
```

Without `DATABASE_URL` set, everything runs against a local SQLite file. Set it to any Postgres connection string to use that instead. [Operations](docs/wiki/Operations.md) covers running it as a scheduled job.

## Documentation

Full documentation lives in **[`docs/wiki/`](docs/wiki/README.md)**:

| Page | What it covers |
|---|---|
| [Overview](docs/wiki/Overview.md) | The problem, the approach, who it's for, what it deliberately doesn't do |
| [Getting Started](docs/wiki/Getting-Started.md) | First working result, both as a library user and as a self-hoster |
| [Concepts](docs/wiki/Concepts.md) | RS Raw, RS Rating, universe, warm-up, trailing window |
| [Architecture](docs/wiki/Architecture.md) | Components, data flow, schema, design decisions and why |
| [Data Pipeline](docs/wiki/Data-Pipeline.md) | Ticker sourcing, price download, split repair, retention |
| [API Reference](docs/wiki/API-Reference.md) | All 15 client methods with parameters and return shapes |
| [CLI Reference](docs/wiki/CLI-Reference.md) | Every `ibd-rs` command and its behaviour |
| [Operations](docs/wiki/Operations.md) | Self-hosting, scheduling, reliability guards, monitoring |
| [Troubleshooting](docs/wiki/Troubleshooting.md) | Symptoms mapped to causes and fixes |
| [FAQ](docs/wiki/FAQ.md) | Accuracy vs. real IBD, missing ratings, and other recurring questions |

## Project status

Beta, maintained by one person. The public data pipeline runs automatically on weekdays and the client API is stable — the last breaking change was the 0.4.0 backend migration, recorded in [CHANGELOG.md](CHANGELOG.md). Treat the hosted endpoint as best-effort: if you depend on this data operationally, self-host.

## Contributing

Issues and pull requests are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md) for setup and the test commands. To report a security issue, follow [SECURITY.md](SECURITY.md) instead of opening a public issue.

## Disclaimer

Not affiliated with Investor's Business Daily or William O'Neil + Co. RS Ratings here are a reverse-engineered approximation of IBD's published methodology; the official formula and universe are proprietary. For official ratings, use [IBD MarketSmith](https://marketsmith.investors.com/).

This is a research and educational tool. It is not financial advice, and nothing it outputs is a recommendation to buy or sell any security.

## License

MIT — see [LICENSE](LICENSE).
