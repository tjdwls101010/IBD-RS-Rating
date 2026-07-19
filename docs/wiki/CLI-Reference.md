# CLI Reference

Complete reference for the `ibd-rs` command-line interface — the engine that produces ratings. If you only want to *read* ratings, you don't need this page; see [API Reference](API-Reference.md).

## Invocation

Two equivalent forms:

```bash
ibd-rs <command>          # console script, from pip install
python -m ibd_rs <command>  # module form, used by the CI workflows
```

Requires the engine extras:

```bash
pip install -e ".[engine]"        # add ",pg" for Postgres
```

### Global options

| Flag | Effect |
|---|---|
| `-v`, `--verbose` | DEBUG-level logging instead of INFO |
| `-h`, `--help` | Help; also works per command (`ibd-rs update --help`) |

Running with no command prints help and exits 0.

### Backend selection

Every command reads the `DATABASE_URL` environment variable:

```bash
# Postgres
export DATABASE_URL="postgresql://user:pass@host/db"

# SQLite (default) — just leave it unset; data goes to data/rs.db
unset DATABASE_URL
```

## Command summary

| Command | Purpose | Duration | Writes |
|---|---|---|---|
| [`init`](#init) | Build the database from scratch | 20–30 min | Everything |
| [`update`](#update) | Daily incremental update | ~3 min | Recent data |
| [`recalc`](#recalc) | Recompute all ratings from stored prices | 1–5 min | Ratings only |
| [`top`](#top) | Show highest-rated stocks | instant | — |
| [`lookup`](#lookup) | Show one ticker's history | instant | — |
| [`status`](#status) | Database statistics | instant | — |
| [`export`](#export) | Write ratings to CSV | seconds | CSV files |

---

## `init`

Build a database from nothing: fetch the universe, download two years of prices, repair splits, compute all ratings.

```bash
python -m ibd_rs init
```

```
Step 1/4: Fetching ticker list from Finviz...
  Found 4642 tickers
Step 2/4: Downloading 2-year price history (4642 tickers)...
  This may take 20-30 minutes.
Step 3/4: Checking for stock splits...
  No anomalies detected
Step 4/4: Calculating RS ratings...
  Computed 1284102 RS records

Init complete!
```

It then prints `status` output automatically.

**Universe refresh is forced** — the 7-day cache is bypassed, since building on stale membership defeats the purpose.

**It refuses to run on an untrusted universe.** If validation fails, it prints the reason and exits 1 without downloading anything:

```
  ERROR: fetched 55 tickers, below absolute floor 3000
  Refusing to init from an untrusted universe.
```

This is stricter than `update`, which degrades to cached data and continues. `init` has no fallback worth having — a database built on 55 tickers would be fully populated with meaningless ratings, which is far harder to notice than a failed command.

**Safe to re-run.** All writes are upserts, so a failed run can be restarted. It will re-download everything, though — if prices are already loaded and only ratings are wrong, use [`recalc`](#recalc).

**Exit codes:** `0` success · `1` untrusted universe

---

## `update`

The daily job. Six stages, described in full in [Data Pipeline](Data-Pipeline.md).

```bash
python -m ibd_rs update
```

```
Step 1/6: Fetching ticker list...
  4642 tickers
Step 2/6: Downloading new price data...
Step 3/6: Checking for stock splits...
  No anomalies detected
Step 4/6: Calculating RS ratings...
  Computed 4591 RS records
Step 5/6: Pruning old close prices...
  Pruned 4238 old close records
Step 6/6: Latest trading day completeness...
  Latest trading day: 2026-03-19
  Universe size: 4642
  Close coverage: 4598/4642 (99.1%)
  Missing close count: 44
  RS rating coverage: 4591/4642 (98.9%)
  Missing RS rating count: 51
  Threshold: 90.0%
  Result: PASS (complete)

Update complete!
```

Downloads a trailing 10-day window for every ticker, not just missing dates — see [Concepts](Concepts.md#trailing-window). Ratings are recomputed for anything newer than the cursor plus the most recent 15 trading days.

### Exit codes are the monitoring signal

`update` exits **1** if either:

- the universe fetch was untrusted (degraded to cache or fallback), or
- latest-day close coverage is below 90%.

Note that `Update complete!` is only printed on success — a failing run ends after the completeness report.

**This non-zero exit is the entire stall-detection mechanism.** The original April 2026 outage wasn't a missing alert channel; it was a five-week failure that kept reporting success. If you schedule this command, make sure a non-zero exit reaches you. See [Operations](Operations.md).

A failed run has still written whatever data it obtained — the failure means "don't trust this run," not "nothing happened."

**When to run:** after the US close on weekdays. The hosted pipeline uses 21:00 UTC.

**Exit codes:** `0` healthy · `1` degraded universe or incomplete coverage

---

## `recalc`

Recompute every rating from prices already stored. Downloads nothing.

```bash
python -m ibd_rs recalc
```

Equivalent to `init`'s step 4 with `recalc_all=True`, over the full date range.

Reach for it when:

- You changed `RS_WEIGHTS`, `RS_UNIVERSE_THRESHOLD`, or the computation itself
- Historical ratings are suspect and you want them rebuilt from known-good prices
- You backfilled prices and need the affected dates re-rated

It rewrites the full history, so ratings that were previously withheld may now appear (and vice versa) if coverage or thresholds changed. Prices are untouched — this is the safe way to fix ratings without re-downloading anything.

**Exit codes:** `0` always (barring an unhandled error)

---

## `top`

Show the highest-rated stocks on the latest rated date.

```bash
python -m ibd_rs top          # default 20
python -m ibd_rs top 50
```

```
IBD RS Ratings — 2026-03-19
==================================================
Rank  Ticker    RS Rating    RS Raw
--------------------------------------------------
   1  MU               99    1.9931
   2  AXTI             99   14.7900
   ...
--------------------------------------------------
Reference:
      SPY              46    0.0490
      QQQ              58    0.0630
```

The reference block shows where the indices land in the same distribution — the quickest read on market regime.

Prints `No RS data available. Run 'init' first.` and exits 0 on an empty database.

---

## `lookup`

One ticker's rating history.

```bash
python -m ibd_rs lookup NVDA
python -m ibd_rs lookup nvda --days 90
```

| Argument | Default | Description |
|---|---|---|
| `ticker` | required | Symbol, case-insensitive |
| `--days` | `30` | Rows to show, newest first |

```
RS History for NVDA
==========================================
Date            RS Raw  RS Rating
------------------------------------------
2026-03-19      0.1666         70
2026-03-18      0.1702         71
...
```

A `—` in the rating column means RS Raw was computed but no rating was published — that date's coverage was below threshold. See [Concepts](Concepts.md#universe-threshold).

Only rows with non-null `rs_raw` are shown, so warm-up dates are absent entirely.

---

## `status`

Database statistics. The first thing to run when something looks wrong.

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

Reading it:

| Field | Healthy | Concerning |
|---|---|---|
| Price tickers | ~4,600 | Far below → degraded universe fetch |
| Price range start | ~13 months back | Much older → retention isn't running |
| RS range start | ~1 year after price start | Same as price start → warm-up gate broken |
| RS range end | Latest trading day | Behind → pipeline is stalled |
| RS tickers | Close to price tickers | Large shortfall → many stocks lack history |
| Last update | Today or last trading day | Stale → job isn't running |

The gap between price-range start and RS-range start is [warm-up](Concepts.md#warm-up), and it should be there.

**`status` never fails.** It reports what it finds and always exits 0 — it's a diagnostic, not a check. The check is `update`'s step 6.

---

## `export`

Write the latest date's ratings to CSV.

```bash
python -m ibd_rs export
```

```
Exported 4591 records to data/rs_ratings_2026-03-19.csv
Updated data/tickers.csv (4591 tickers)
```

Two files, same content: a dated archive and `data/tickers.csv`, the stable path committed to the repository as a public snapshot.

Columns: `ticker`, `date`, `rs_raw`, `rs_rating`, `sector`, `industry` — sorted by `rs_raw` descending, latest rated date only. For history, query the database directly or use `history()` from the client.

---

## Typical sequences

**First-time setup**

```bash
export DATABASE_URL="postgresql://..."   # optional
python -m ibd_rs init
python -m ibd_rs status
python -m ibd_rs top 20
```

**Daily, scheduled**

```bash
python -m ibd_rs update || notify "RS pipeline failed"
```

**After changing the RS formula**

```bash
python -m ibd_rs recalc
python -m ibd_rs status
```

**Diagnosing a stale dataset**

```bash
python -m ibd_rs status            # how far behind?
python -m ibd_rs -v update         # verbose run to see where it stops
```

## Next

- [Operations](Operations.md) — scheduling and monitoring these commands
- [Data Pipeline](Data-Pipeline.md) — what each step does internally
- [Troubleshooting](Troubleshooting.md) — when output doesn't look right

[← Back to index](README.md)
