# Concepts

The vocabulary you need to read the rest of the documentation, the code, and the data. Terms are defined in dependency order — each builds on the ones above it.

## Ticker and the universe

A **ticker** is one tradeable symbol, e.g. `NVDA`. The **universe** is the set of tickers the system tracks: roughly 4,600 US-listed common stocks, defined by a Finviz screener query with market cap above $50M, minus ETFs and shell companies, plus two deliberate exceptions (below).

The universe matters more than it might seem, because a rating is a statement *about the universe*. Change the universe and every rating changes, even if no price moved.

The universe is refreshed at most every 7 days (`CACHE_DAYS`) and cached in the database, because scraping it takes minutes and it barely changes day to day.

## Reference ticker

`SPY` and `QQQ`. General ETFs are excluded from the universe — ranking a fund against the stocks it holds is not a meaningful comparison — but these two are kept and **ranked alongside individual stocks**, so you can ask "where does the index itself sit in the distribution?" and get an answer like "SPY is at RS 46 today."

They are 2 tickers out of ~4,600, so their effect on everyone else's percentile is negligible.

This is worth stating clearly because it was once ambiguous in the codebase: a comment said reference tickers were excluded from ranking while the code included them. The code was right and the comment was wrong. Reference tickers are ranked.

## ROC — rate of change

`ROC(n)` is a stock's return over its last `n` **valid trading days**:

```
ROC(n) = (close_today / close_n_valid_trading_days_ago) - 1
```

"Valid trading days" is the load-bearing phrase. Prices are stored in a grid of dates × tickers, and that grid has holes — a halt, a late listing, a missing vendor value. If you compute `ROC(63)` by stepping back 63 *rows* in that grid, a stock with ten missing days is silently compared against its price 73 calendar-trading-days ago, and its momentum is measured over a different window than everyone else's.

So each ticker's series is compacted to its own non-null observations first, and `n` is counted in those. Every stock's `ROC(63)` covers 63 days on which that stock actually traded.

## RS Raw

A stock's weighted momentum score on a date — the sum of four ROCs at different lookbacks:

```
RS Raw = 0.4 × ROC(63) + 0.2 × ROC(126) + 0.2 × ROC(189) + 0.2 × ROC(252)
```

The four windows are approximately the last four quarters (63 trading days ≈ 3 months). Because the windows overlap, the effective weight on each quarter declines as it recedes:

| Period | Effective weight |
|---|---|
| Most recent quarter (0–3 months) | 100% |
| 3–6 months ago | 60% |
| 6–9 months ago | 40% |
| 9–12 months ago | 20% |

The most recent quarter counts five times as much as the oldest. That asymmetry is the point: it favours stocks whose momentum is *accelerating* over stocks coasting on an old rally.

RS Raw is a **raw, unranked** number. It is roughly a return figure — `0.1666` is not a rating, it is a weighted return of about 16.7%. On its own it tells you little, because you don't know what everyone else scored. It exists to be ranked.

*Avoid calling this "momentum score" or "raw score" in code and docs — the codebase uses `rs_raw` consistently.*

## Warm-up

The period during which a stock has too little history to score. RS Raw needs 252 trading days of lookback, so a stock must have **more than 252** valid trading days before it gets any RS Raw at all. Below that, it is in warm-up and has no rating.

This is deliberate. A newly-listed stock could be given a rating from six months of data, but that rating would not be comparable to everyone else's — it would measure a different thing under the same name. Withholding is the honest option.

Practically: the dataset's ratings begin on **2025-03-21**, the date the warm-up period ended for the initial price load.

## Population

The set of tickers being ranked against each other on a given date — the denominator of the percentile.

The population is *not* "every ticker in the universe"; it is every ticker with a valid RS Raw on that date, meaning it is in the universe, past warm-up, and has a usable price. Reference tickers are included.

This definition is the fix for a specific failure. When the pipeline stalled in April 2026, only 54 tickers had fresh prices on a given day. Ranking those 54 against each other produced perfectly well-formed ratings from 1 to 99 — and they were nonsense, because being the strongest of 54 stocks is not the same as being the strongest of 4,600. See the universe threshold below.

*Korean docs and commit messages use 모집단 for this term.*

## RS Rating

RS Raw converted to a percentile rank within the population on that date, scaled to an integer from 1 to 99:

```
rating = round(percentile_rank × 98 + 1)
```

RS 99 means the stock outperformed essentially the entire market over the trailing year, weighted toward recent months. RS 50 is the median. RS 1 is the bottom.

Two properties follow from it being a percentile:

- **It is relative, always.** A stock's rating can fall while its price rises, if other stocks rose more. Nothing is wrong when that happens.
- **It depends on every other stock.** One stock's rating is a function of that day's entire population, which is why population integrity is treated as a correctness issue rather than a data-quality nicety.

*Avoid calling this "percentile", "rank", or "score" in code and docs — the codebase uses `rs_rating`.*

## Universe threshold

The minimum fraction of the universe that must have a valid RS Raw before a date's ratings are considered trustworthy: **90%** (`RS_UNIVERSE_THRESHOLD`).

When coverage on a date falls below it, that date's RS Ratings are set to null across the board. RS Raw is still stored — it is a per-stock number and remains valid — but no ratings are published, because a percentile against a partial population is misleading in a way that is invisible downstream.

This is the direct structural answer to the April 2026 incident. A day with 54 of 4,600 stocks is 1.2% coverage; it now produces zero ratings instead of 54 confident-looking wrong ones.

A separate but similar threshold, `PRICE_COMPLETENESS_THRESHOLD` (also 90%), is what the daily job checks after it runs to decide whether the run succeeded — see [Operations](Operations.md).

## Trailing window

The download strategy: every run re-requests the last **10 calendar days** (`TRAILING_WINDOW_DAYS`) of prices for **every** ticker, and upserts the results.

The obvious alternative — "download from the newest date I already have" — is what caused the April 2026 stall. That cursor was global: one ticker whose data ran ahead advanced the start date for everyone, so tickers that fell behind were never asked for the days they were missing. They starved permanently, and because the job still completed, nothing alerted.

A fixed trailing window has no cursor to corrupt. Since upserts are idempotent, re-downloading the same days is harmless, and any ticker that failed yesterday is simply picked up today. The property this buys is **self-healing**: transient failures repair themselves on the next run with no intervention.

The cost is re-downloading data that hasn't changed. That is cheap, and the alternative was a five-week silent outage.

## Self-healing recompute window

The same idea applied to the RS calculation. Each incremental run recomputes ratings for the most recent **15 trading days** (`RS_RECOMPUTE_WINDOW_DAYS`), in addition to any dates newer than the last computed one.

Without it, a date left unrated because coverage was below threshold would stay unrated forever — the cursor would move past it and never come back. With it, once the missing prices arrive through the trailing window, the date gets rated on a subsequent run.

The window is bounded on both sides. Too small and a cohort of tickers finishing warm-up around the same time can leave a date permanently skipped; too large and recomputed dates start needing lookback data that price retention has already deleted. Under current settings the hard ceiling is 21 trading days, and there is a test that fails if configuration drifts past it.

## Starvation and silent stalls

**Starvation** is the state where some tickers stop being updated while others advance — the April 2026 failure. The trailing window makes it structurally impossible rather than merely fixed.

**Silent starvation** is starvation that reports success. The scheduled job finishes green, no failure email is sent, and the database quietly fills with ratings computed against a collapsed population. This is the dangerous variant, because every monitoring signal says everything is fine.

The countermeasure is the **completeness check**: after each daily run, coverage on the latest trading day is measured against the universe, and the process exits non-zero if it is below threshold. That converts a silent stall into a failed build, which triggers the notification that already existed. No new alerting channel was needed — the gap was never missing alerts, it was a failure that never announced itself.

## Universe validation

A truncated or blocked screener fetch must never overwrite a good cached universe. This is not hypothetical: on 2026-07-08 a fetch returned 55 tickers instead of ~4,600 and cached them for 30 days.

Every fetch is now validated on three independent axes before it is trusted:

| Guard | Constant | Rejects when |
|---|---|---|
| Absolute floor | `UNIVERSE_FLOOR = 3000` | Fewer than 3,000 tickers, whatever the reason |
| Drop guard | `UNIVERSE_DROP_GUARD = 0.90` | Below 90% of the last known-good count |
| Completeness | `UNIVERSE_COMPLETENESS_RATIO = 0.98` | Fewer rows returned than 98% of what the screener itself reports for the query |

The third is the interesting one — it compares what was received against what the source *claims* it should have sent, catching a partial fetch that happens to land above the absolute floor.

A fetch that fails validation is marked **untrusted**. The run continues on the last-good cache (or, if there is no cache at all, on a broader fallback universe from the Nasdaq Trader symbol directory) so the pipeline keeps producing data — but the untrusted flag propagates, and the daily run exits non-zero. Degraded operation is acceptable; degraded operation that looks healthy is not.

## Retention

Two different retention policies for two different kinds of data:

- **Close prices: 13 months** (`PRICE_RETENTION_MONTHS`). They are an input to the RS calculation, and the calculation only ever looks back 252 trading days. Keeping more makes every daily run slower for no benefit — and a slow run was one contributing factor to the original stall.
- **RS Ratings: forever.** They are the output, they are small, and historical ratings are the point of having a history.

Both live in the same table, so retention is expressed as: delete old rows that never got a rating, and null out the `close` column on old rows that did.

## Term summary

| Term | Meaning | Code identifier |
|---|---|---|
| Universe | The ~4,600 tracked tickers | `ticker_list` (meta) |
| Population | Tickers actually ranked on a date | — |
| ROC(n) | Return over n valid trading days | — |
| RS Raw | Weighted momentum score, unranked | `rs_raw` |
| RS Rating | Percentile rank, 1–99 | `rs_rating` |
| Warm-up | Under 252 trading days of history | — |
| Reference ticker | SPY, QQQ — ranked with the rest | `REFERENCE_TICKERS` |
| Universe threshold | 90% coverage to publish ratings | `RS_UNIVERSE_THRESHOLD` |
| Trailing window | Re-download last 10 days, always | `TRAILING_WINDOW_DAYS` |
| Recompute window | Re-rate last 15 trading days | `RS_RECOMPUTE_WINDOW_DAYS` |

## Next

- [Architecture](Architecture.md) — how these concepts are implemented
- [API Reference](API-Reference.md) — reading the data these terms describe
- [FAQ](FAQ.md) — "why does my stock have no rating?" and similar

[← Back to index](README.md)
