# Overview

What this project is, why it exists, and who should use it. Read this first — every other page assumes it.

## The problem

Momentum investing rests on a simple empirical claim: stocks that have outperformed tend to keep outperforming, at least for a while. Acting on that claim requires a way to say *how much* a stock has outperformed **relative to everything else available to buy**.

A raw return doesn't do this. "Up 18% over the past year" is meaningless in isolation — in one market that's a leader, in another it's a laggard. To rank a stock against the market you need three things:

1. A momentum score that weights recent performance more heavily than distant performance, because a rally that ended nine months ago says little about today.
2. That same score computed for **every** stock, on the **same** day, from the **same** data.
3. A ranking step that converts the score into a position within the whole distribution.

Investor's Business Daily has published exactly this since the 1980s as the **RS Rating**, a 1–99 number where 99 means the stock outperformed 99% of the market. It is the first filter in most O'Neil-style strategies, and it is behind a paid subscription.

Open-source alternatives generally do step 1 and stop. They compute a weighted return, rescale it to look like a 1–99 number, and call it an RS Rating. That is not the same thing. A rescaled return is a transformation of one stock's own history; a rating is a statement about where that stock sits among thousands of others. Only the second is comparable across stocks, across sectors, and across time.

## What this project does

It does all three steps, every trading day, for the whole US common-stock universe.

**Collect.** A screener query defines the universe — roughly 4,600 US-listed common stocks above $50M market cap, excluding ETFs and shell companies. Daily closing prices for all of them are downloaded and stored.

**Score.** Each stock gets a weighted momentum score across four lookback windows:

```
RS Raw = 0.4 × ROC(63) + 0.2 × ROC(126) + 0.2 × ROC(189) + 0.2 × ROC(252)
```

The most recent quarter carries five times the weight of the oldest. Each `ROC(n)` uses that stock's own valid trading days, so a halted or thinly-traded stock is still compared against its real price 63 trading days ago rather than whatever happened to be 63 rows up in a calendar grid.

**Rank.** Every valid score on a date is percentile-ranked against every other score on that date and scaled to 1–99. This is the step that makes the number mean something, and it is the step that is usually missing elsewhere.

The results land in Postgres and are served over a read-only HTTP API. A zero-dependency Python client reads them.

## What it delivers

**A number you can compare.** RS 90 means the same thing for a semiconductor stock and an oil driller, today and last March: it outperformed 90% of the market over the trailing year.

**Coverage of the whole universe, not a watchlist.** The ranking is only meaningful if the denominator is the real market. Roughly 4,600 stocks are rated daily; SPY and QQQ are ranked alongside them so you can see where the index itself falls.

**Sector and industry aggregation.** Average RS by sector and by industry, and the strongest names within either. O'Neil's research attributed roughly half of a stock's move to its industry group, which makes group strength a first-class question rather than an afterthought.

**Ratings that admit when they don't know.** A stock without 252 trading days of history gets no rating rather than one computed from thin data. A trading day where fewer than 90% of the universe has usable prices produces no ratings at all rather than a distorted set. These gaps are deliberate — see [Concepts](Concepts.md).

**Something you can run yourself.** The engine is in the same repository as the client. If you don't want to depend on a free hosted endpoint, point it at your own Postgres and run it on your own schedule.

## Who it's for

**Momentum and growth investors** who screen for relative strength and don't want to pay for a terminal, or who want the number available in Python next to the rest of their analysis.

**Quantitative researchers** who need a consistent cross-sectional momentum ranking as a factor or a filter, with a documented and inspectable methodology rather than a black box.

**Developers building investing tools** who want ratings behind a `pip install` with no API key, no signup, and no dependency conflicts — the client is standard library only.

**Anyone who wants to self-host market data infrastructure**, as a working example of a daily unattended pipeline with real reliability guards.

## What it is not

Being explicit about the boundaries is more useful than a longer feature list.

**Not official IBD data.** The formula is a reverse-engineered approximation of a published methodology. IBD's exact weights, universe construction, and adjustments are proprietary. Rankings correlate strongly — the same names appear at the top — but absolute values differ, more so in the middle of the distribution. See [FAQ](FAQ.md) for measured comparisons.

**Not real-time.** Ratings are computed once per trading day, after the US close. There are no intraday values and no streaming feed.

**Not a trading system.** It produces one input to a decision. There is no signal generation, no backtesting engine, no portfolio construction, and no position sizing. What you do with a rating is entirely yours.

**Not a general market-data API.** It serves RS Raw, RS Rating, close price, sector, and industry. It is not a source for fundamentals, options, intraday bars, or corporate actions — closes are adjusted upstream, and the split handling here is repair-oriented, not a corporate-actions feed.

**Not a guaranteed service.** The public endpoint is free and best-effort, run by one person. There is no uptime commitment and no support contract. If your work depends on the data, self-host it: [Operations](Operations.md).

**No index or ETF coverage beyond SPY and QQQ.** ETFs are excluded from the universe by design, because ranking a fund against its own constituents is not a meaningful comparison. SPY and QQQ are the two deliberate exceptions, kept as market reference points.

## How it compares

| | This project | IBD MarketSmith | Typical open-source RS |
|---|---|---|---|
| Percentile ranked | Yes | Yes | Usually no — weighted return only |
| Universe size | ~4,600 | Proprietary, larger | Often a watchlist or index |
| Cost | Free | Subscription | Free |
| Self-hostable | Yes | No | Varies |
| Update frequency | Daily, after US close | Daily | Varies; often manual |
| Sector/industry ranking | Yes | Yes | Rare |
| Data source transparency | Fully inspectable | Proprietary | Varies |

The honest summary: if you need the official number, subscribe to IBD. If you need a rigorous, free, inspectable approximation you can compute yourself, this is it.

## Where the design effort actually went

Reading the code, you might expect the RS formula to be the hard part. It isn't — it's about forty lines of pandas. The difficulty is keeping an unattended daily job producing *correct* data for years without anyone watching.

In April 2026 this pipeline silently froze. 97% of tickers stopped updating while the scheduled job kept reporting success, so no failure alert fired. For roughly five weeks the database accumulated ratings computed against a universe of 54 stocks instead of 4,600 — numbers that looked entirely normal and were completely wrong. Being ranked first out of 54 also produces a 99.

Most of the non-obvious machinery in this codebase exists because of that: the trailing-window download that makes per-ticker starvation structurally impossible, the universe-coverage threshold that refuses to rate an incomplete day, the completeness watchdog that exits non-zero so a stall shows up as a red build, the exact dependency pinning, the universe validation that stops a truncated screener fetch from poisoning the cache. [Architecture](Architecture.md) explains each one and why it is shaped the way it is.

The design principle that came out of it: **a wrong number is worse than a missing one**, because a missing number announces itself and a wrong one doesn't.

## Next

- [Getting Started](Getting-Started.md) — install the client and get a real result
- [Concepts](Concepts.md) — the vocabulary, in dependency order
- [Architecture](Architecture.md) — how the system is built

[← Back to index](README.md)
