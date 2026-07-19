# FAQ

Recurring questions about what the ratings mean and how far to trust them. For things that are actually broken, see [Troubleshooting](Troubleshooting.md).

---

## Accuracy and methodology

### How close is this to a real IBD RS Rating?

Close at the extremes, looser in the middle. Compared against IBD MarketSmith ratings:

| Range | Typical difference | Interpretation |
|---|---|---|
| RS 90+ | ±1–3 points | Near-exact for the strongest names |
| RS 60–90 | ±5–10 points | Systematic offset, mostly from universe-size differences |
| Below RS 30 | ±3–6 points | Both agree the stock is weak |

**Ranking order is consistent** — the same names show up at the top in both. Absolute values differ because IBD's exact formula, universe construction, and adjustments are proprietary.

Practically: a screen for "RS ≥ 90" gives substantially the same list. Treating a rating of 73 as precisely IBD's 73 is over-reading it.

### Why do the numbers differ at all?

Three reasons, in order of impact:

1. **Different universe.** A percentile is a statement about a denominator. IBD ranks a different set of stocks, so the same performance maps to a different percentile — this is most of the gap, and it's why mid-range ratings drift most.
2. **Approximated formula.** The 0.4/0.2/0.2/0.2 weighting over 63/126/189/252 trading days is the widely-circulated reverse-engineering of IBD's method, not their published formula.
3. **Different price data.** yfinance's adjusted closes and IBD's own data handle corporate actions slightly differently.

### Why these weights?

`0.4 × ROC(63) + 0.2 × ROC(126) + 0.2 × ROC(189) + 0.2 × ROC(252)` is the commonly-used approximation of IBD's methodology, weighting the most recent quarter five times as heavily as the oldest.

They're kept unchanged deliberately. Changing them would redefine what every stored rating means and make new values incomparable to historical ones — a data-format change, not a tuning knob. If you self-host and want different weights, edit `RS_WEIGHTS` and run `recalc` to rebuild consistently.

### Is RS Rating predictive?

That's a research question this project doesn't answer. It computes a well-defined measure of past relative performance. Whether that predicts future returns depends on the market regime, your holding period, and everything else in your process.

The historical case for momentum is real but not universal, and it fails in specific conditions — sharp reversals in particular. Ratings here are one input, not a signal.

---

## Missing and unexpected values

### Why does my stock have no rating?

Most likely one of:

1. **Not in the universe** — below $50M market cap, an ETF, or a shell company.
2. **Warm-up** — fewer than 252 valid trading days of history. Recent IPOs have no rating by design; giving one from six months of data would produce a number not comparable to everyone else's.
3. **Low-coverage date** — the whole market's ratings were withheld that day.
4. **Delisted** — the ticker has historical rows but no recent ones.

Check which:

```python
rs.get("XYZ")                    # None → not tracked or no data
rs.history("XYZ", days=5)        # rows with rs_rating None → case 3
```

### Why do `rs_raw` and `rs_rating` disagree — one set, one null?

That combination means the date's coverage fell below the 90% universe threshold. RS Raw is per-stock and stays valid; the rating is a percentile against the population and was withheld because the population was incomplete.

This is the system refusing to publish a misleading number. See [Concepts](Concepts.md#universe-threshold).

### Why is a whole date missing?

Weekends and US market holidays have no data — the pipeline only runs weekdays and there's nothing to fetch on a holiday. Beyond that, a date can be absent if the pipeline failed that day, in which case the trailing window usually backfills it on a subsequent run.

### My stock rose but its rating fell. Bug?

No — this is the defining property of a percentile. The rating measures relative position. If your stock gained 2% while the market gained 5%, it fell behind and its rating drops correctly.

This is also why ratings are more informative than returns: they already account for what everything else did.

### Why does the top of the list have so many 99s?

Ratings are integers 1–99 across ~4,600 stocks, so each rating holds roughly 46 stocks. Around 46 stocks are at 99 on any given day.

`top()` breaks the tie with `rs_raw` descending, so ordering within the 99 bucket is by actual weighted return.

---

## Data and coverage

### What's in the universe?

Roughly 4,600 US-listed common stocks — NYSE, NASDAQ, AMEX — above $50M market cap, excluding ETFs and shell companies. ADRs (BABA, TSM) are included. SPY and QQQ are tracked and ranked as reference points.

Full detail in [Data Pipeline](Data-Pipeline.md#stage-1--universe-acquisition).

### Why are ETFs excluded but SPY and QQQ included?

Ranking a fund against the stocks it holds isn't a meaningful comparison — an S&P 500 ETF's momentum is by construction the average of its constituents'.

SPY and QQQ are kept as two deliberate exceptions so you can read the market's own position in the distribution: "SPY is at RS 46" tells you a stock at 60 is genuinely ahead of the index. Two tickers out of 4,600 don't measurably shift anyone else's percentile.

### How far back does the data go?

Ratings begin **2025-03-21**, the end of warm-up for the initial price load. Ratings are retained indefinitely.

Close prices are retained 13 months — long enough for the 252-trading-day lookback plus margin. Older closes are dropped so the daily computation stays a constant size.

```python
rs.dates()      # {'first': '2025-03-21', 'last': ...}
```

### How often does it update?

Once per trading day, weekdays at 21:00 UTC — one hour after the US close. No intraday or weekend updates.

### Where does the data come from?

Universe and sector/industry from a Finviz screener; prices from yfinance. Both are free, unofficial endpoints, which is a real dependency risk the project hedges (retries, fallback universe, validation) but cannot eliminate.

### Is there sector data for every stock?

Almost. It comes from the screener, so a ticker added through the Nasdaq Trader fallback path — used only when Finviz is unavailable and no cache exists — will have null sector and industry until a successful screener fetch fills it in.

---

## Using the client

### Do I need an API key?

No. No key, no account, no signup. The client fetches a short-lived anonymous token automatically on first use.

### Is there a rate limit?

No published limit, and normal use won't hit one. It's a free service, so avoid tight polling loops — data changes once a day, so caching a daily snapshot is both faster and more considerate. See [API Reference](API-Reference.md#performance-notes).

### Can I use this commercially?

The **code** is MIT licensed — do as you like.

The **data** is a different question. It derives from Finviz and Yahoo Finance, whose terms govern redistribution, and "RS Rating" is associated with IBD's brand. If you're building something commercial, read those terms and consider self-hosting with data you're licensed for. This isn't legal advice.

### Why is the client zero-dependency?

So installing it can never cause a conflict. It's meant to drop into an existing analysis environment with a pinned pandas and numpy, and adding requirements there causes real pain. Standard library only means it always installs cleanly.

The engine has whatever dependencies it needs, but it's in a separate extra that client users never install.

### Can I get intraday ratings?

No. Ratings are computed from daily closes, once daily. An intraday rating would need the whole universe's live prices continuously, which is a fundamentally different (and much more expensive) system.

---

## Self-hosting

### Should I self-host?

Yes if you need availability guarantees, a different universe or formula, longer history, or no dependency on a free service. No if you just want the data — the public endpoint is easier.

See [Operations](Operations.md).

### What does it cost to run?

Close to nothing. GitHub Actions is free on public repos; a few GB of Postgres fits most free tiers; the data sources are free.

The real cost is attention: it's an unattended job with external dependencies that will occasionally break.

### Can I change the universe?

Yes — edit `SCREENER_FILTERS` and `EXCLUDED_INDUSTRIES` in `ibd_rs/config.py`, then re-run `init`.

Remember that changing the universe changes every rating, because the universe *is* the denominator. Ratings from a 500-stock universe aren't comparable to ratings from a 4,600-stock one, even for identical stocks with identical prices.

### Can I use a database other than Postgres?

SQLite and Postgres are supported. The SQL is nearly standard, so another engine is plausible but would need work in `db.py` — upserts, bulk insert, and one date-arithmetic expression are backend-specific.

SQLite handles the full universe fine for local analysis. Use Postgres for anything shared or long-running.

---

## Project

### Why does this exist when IBD already publishes RS Ratings?

IBD's are behind a subscription, and open-source alternatives generally skip the percentile-ranking step — they compute a weighted return and present it as a rating. That's a different quantity: a transformation of one stock's own history rather than a statement about its position among thousands. This project does the ranking, which is the part that makes the number mean anything.

### Is this affiliated with IBD?

No. Not affiliated with Investor's Business Daily or William O'Neil + Co. The methodology is a reverse-engineered approximation of their published description. For official ratings, use [IBD MarketSmith](https://marketsmith.investors.com/).

### Can I trust this for real money?

Read the [Overview](Overview.md#where-the-design-effort-actually-went) section on the April 2026 outage first — the pipeline once produced five weeks of confident, wrong data while reporting success.

The guards added since make that specific failure structurally impossible and would catch several related ones. But it's a free service maintained by one person on unofficial data sources. Use it as one input, verify anything that drives a large decision, and self-host if it's load-bearing.

### How can I help?

Data-correctness bugs and pipeline-reliability fixes are the most valuable contributions. See [CONTRIBUTING.md](../../CONTRIBUTING.md).

## Next

- [Concepts](Concepts.md) — the definitions behind these answers
- [Troubleshooting](Troubleshooting.md) — when something is actually broken
- [Overview](Overview.md) — what the project is and isn't

[← Back to index](README.md)
