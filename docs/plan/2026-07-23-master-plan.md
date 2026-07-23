# RS Rating — Recovery & Re-architecture Master Plan

> **Status:** APPROVED PLAN (planning-only session, 2026-07-23). Implementation happens in a
> **separate** future Claude session. This document is the contract for that session.
>
> **Companion docs:** [`2026-07-23-audit-appendix.md`](2026-07-23-audit-appendix.md) (25 findings + evidence),
> [`README.md`](README.md) (index + decision log).

---

## 0. How to use this document

You are the implementing session. Work **slice by slice, in order**. Each slice is an independent
PR that must be green in CI before the next starts. Do **not** batch slices. Do **not** deviate from
the **Hard Invariants** (§4) — they exist because violating them destroys ~1 year of irreplaceable
data. When a slice says "verify," write the test first, then make it pass (project CLAUDE.md §4).

The single most important operational rule: **never run `recalc_all` / `init` against the live
pruned Neon DB.** See §4.

---

## 1. TL;DR

The daily pipeline has been silently producing no ratings since **2026-06-29** and no real prices
since **2026-07-17**, while every GitHub Action still reported partial success or a red-but-unhelpful
failure. Root causes are three independent bugs plus a fragile design:

1. **RS rating gate** divides by a *non-stationary* denominator (13-month column union, ~4,950 and
   growing) so daily coverage drifted below the 90% cliff (~06-30) and the whole rating row gets
   NULLed.
2. **Universe corruption**: `finvizfinance 1.3.0` mis-parses Finviz's new per-row **logo**, prepending
   the initial letter to every ticker (`AAPL`→`AAAPL`). The three universe guards check *count* not
   *validity*, so ~4,632 garbage symbols cached as "trusted" → yfinance 404s them → the 07-20+ price
   collapse.
3. **Watchdog blind spot**: the build fails only on *close* coverage, never *rating* coverage, so a
   3-week total rating outage ran green.

The fix restores the rating series surgically (no history loss), makes the gate stationary and the
watchdog honest, replaces the broken Finviz parser with a robust `href`-based one, demotes Finviz off
the daily critical path (weekly, guarded), and adds the CI + guardrails that would have caught all
three. Cost stays **$0**. Market-cap universe semantics are **preserved** (no liquidity re-architecture).

---

## 2. System context

IBD-style Relative Strength Rating for the US common-stock universe (~4,600 names, market cap > $50M,
ex-ETF/shell + SPY/QQQ). Daily GitHub Action:

```
Finviz universe → yfinance daily closes → RS Raw (weighted ROC 0.4·63 +0.2·126 +0.2·189 +0.2·252,
each ROC over THAT ticker's valid trading days) → RS Rating (1–99 percentile across the day's
population) → Neon Postgres (single `rs` table: ticker, date, close, rs_raw, rs_rating).
```

Ratings are the product and are kept **forever**; closes are an input kept **13 months**. Consumers
read via `rs_rating/client.py` (Neon Data API, anonymous RLS token) and a CLI. Packaged to PyPI
(current 0.4.0). Key modules: `ibd_rs/{rs,db,prices,tickers,splits,nasdaq_trader,config,cli}.py`.

---

## 3. Diagnosis (evidence)

Measured against the live Neon DB and GitHub run logs on 2026-07-23.

### Date ranges (the smoking gun)
| column | range |
|---|---|
| `close` | 2025-06-27 → **2026-07-22** |
| `rs_raw` | 2025-06-10 → **2026-07-22** |
| `rs_rating` | 2025-06-10 → **2026-06-29** ← frozen 3+ weeks |

`last_rs_date` meta = 2026-07-22, so the incremental cursor believes it is done.

### Per-date coverage (denominator drift)
`universe_size = len(price_df.columns) = 4,950` (union of every ticker with any close in 13 months).
Daily active ≈ 4,630; daily `rs_raw` ≈ 4,450 → **~89.9% of 4,950**, i.e. just under the 0.90 cliff
since ~06-30. Historical dates ≤06-29 were rated when the union was smaller.

```
date        rs_raw   raw/4950
2026-06-29   4451     89.9%   ← last rated (barely)
2026-06-30   4451     89.9%   ← NULL from here on
2026-07-17   4377     88.4%
2026-07-20    ~33      0.7%   ← universe corruption
2026-07-22    ~31      0.6%
```

### Universe corruption mechanism (confirmed at DOM level)
`finvizfinance/screener/base.py:122-127` reads the whole ticker cell `.text`. Finviz added a logo
anchor inside that cell, so `.text` = logo-initial + ticker. Live DOM sample:

```
cell.text='AAACB'  href='stock?t=AACB...'   ← real ticker AACB is clean in the href t= param
```

`ticker_list` meta = 4,632 garbage symbols (only 143 real; AAPL/MSFT/NVDA absent). `failed_tickers`
= 4,538. GH log 07-20: `$AAAPL: possibly delisted`, `404 Quote not found for symbol: AACA` — **no
rate-limit errors** (the "GitHub IP block" hypothesis was tested and REJECTED). `finvizfinance 1.3.0`
is the latest release; there is no upstream fix.

### Empirically resolved facts
- DB symbol form is **hyphen** (`BRK-B`, `BF-B`). Finviz `href t=` already emits hyphen (`t=AAC-U`).
- The `rs` table has **zero** garbage rows (yfinance 404'd fakes → nothing written). Only the
  `tickers` (sector) table is polluted: **9,438 rows** (real + garbage coexist via upsert).
- Retention margin is **~2 trading days, not ~21** (prune keeps `now − 390 calendar days` ≈ 269
  trading days; the trailing window's oldest date needs position ≥ 252 ⇒ margin ≈ 2). The config
  comment and `test_recompute_window_leaves_enough_lookback_margin` overstate it.

Full findings: [audit appendix](2026-07-23-audit-appendix.md).

---

## 4. Hard invariants & guardrails (do not violate)

1. **Never `recalc_all` / `init` on the pruned live DB.** `calculate_and_store(recalc_all=True)`
   clears rs_raw+rs_rating for every loaded date, then only re-stores dates with ≥252 prior
   in-window closes. On the 13-month-pruned DB only the newest ~17 dates survive — the ~252 mid-band
   dates' ratings (computed when 2y of closes existed) are cleared and **never restored**. This wipes
   ~1 year of history. Entry points to guard: `cmd_recalc` (cli.py:146), `cmd_init` (cli.py:53),
   `.github/workflows/init.yml`, and the **NULL-cursor degradation** (`rs.py:108 if last_rs_date:` —
   a NULL cursor silently becomes a full recompute).
2. **Recovery is rating-only.** Re-rank *stored* `rs_raw`; never recompute `rs_raw` from the pruned
   closes for historical dates. Ranking is denominator-independent, so re-ranking reproduces the exact
   ungated result. `rs_raw` and `close` must stay **byte-identical** through recovery.
3. **Never widen `RS_RECOMPUTE_WINDOW_DAYS` as a recovery lever.** The real margin is ~2 trading days;
   any window > ~17 pushes recompute below the lookback cliff and destroys history.
4. **Freeze the daily cron during recovery.** `prune_old_close` runs every `update` and DELETEs rows
   where `rs_rating IS NULL AND date < cutoff` — a scheduled run mid-recovery converts a nulled rating
   into a permanent row DELETE and races the manual backfill.
5. **Develop locally; touch production once.** SQLite (logic) → OrbStack Postgres (real migration
   rehearsal) → Neon branch (final rehearsal on prod data) → snapshot → production. See §8.

---

## 5. Decision log (locked with the maintainer)

| # | Decision | Rationale |
|---|---|---|
| P1 | **Correctness & reliability first** | It's a real daily product feeding consumers. |
| P2 | **Strictly $0** — no paid data source, proxy, or DB tier | Hard constraint. All chosen options are free. |
| P3 | Scope = fix all 3 + latent audit findings + refactor allowed | Maintainer chose the broadest scope. |
| P4 | **Full surgical backfill** of 2026-06-30 → 07-22 | Restore the rating series; no history loss. |
| P5 | **Universe = B′**: fix Finviz `href` parser + validity guards + **weekly** demotion + **keep market-cap $50M+ semantics** | Parser fix is ~15 lines and robust; big liquidity re-architecture is unnecessary. Finviz stays for sector anyway, so a guarded weekly fetch carries membership too — off the daily critical path, minimal seam. |
| P6 | Liquidity filter / Volume storage / Nasdaq-parser-as-primary **dropped** | Market cap is NOT reconstructible from OHLCV (only liquidity is — a different axis / second seam). B′ avoids it entirely. |
| P7 | Nasdaq Trader = **emergency fallback only** (Finviz dead + no cache) | Rarely used; Security-Name cleanup is low priority. |
| P8 | **Gate denominator = active validated universe (floored at `UNIVERSE_FLOOR`), identical to the watchdog denominator**; numerator restricted to `universe ∪ REFERENCE` | Stationary; gate-pass ⇔ rating-coverage-pass by construction. |
| P9 | **Ranking population unchanged** (percentile across all tickers with valid rs_raw that day) | Preserves methodology continuity with all history; ghost contamination is small. Audit D22 (rank only universe members) **deferred** — revisit only if material. |
| P10 | `rs_raw` stays **byte-frozen** through recovery | Safety; single documented boundary. |
| P11 | Symbol canonical form = **hyphen** | Matches DB and yfinance and Finviz `href t=`. |
| P12 | Cleanup scope = **`tickers` table only** | `rs` table has no garbage rows. |
| P13 | **0.5.0 minor + a universe-definition version field** surfaced to consumers | Client API surface unchanged; seam is effectively a "definition restoration," documented in CHANGELOG. |
| P14 | Sequencing = **critical/hotfix-first**, slice-by-slice PRs, release when meaningful | Maintainer delegated sequencing; system is actively broken. |
| P15 | **CI safety net first** (Slice 0) | No CI ran the tests — that is how all 3 bugs shipped. |

---

## 6. Target designs

### 6.1 Unified stationary gate + honest watchdog (fixes Problems 1 & 3 together)

One authoritative population and one denominator, shared by the RS gate and the watchdog.

- **Active universe** `U` = the current validated membership (the cleaned `ticker_list`) ∪
  `REFERENCE_TICKERS`.
- **Denominator** `D = max(|U|, UNIVERSE_FLOOR)` — identical to what
  `check_latest_trading_day_completeness` already uses (`db.py:432`).
- **Gate** (`compute_rs_rating`): a date is rated iff
  `|{ t ∈ U : rs_raw[t, date] is valid }| / D ≥ RS_UNIVERSE_THRESHOLD`.
  Change the signature from `universe_size: int` to `active_universe: set[str]`; thread it from
  `calculate_and_store` / `cli`. The numerator ignores delisted ghosts (not in `U`), so they can
  neither inflate the denominator nor pad coverage.
- **Ranking** stays as-is: `rs_raw_df.rank(axis=1, pct=True)` across all valid rs_raw (P9).
- **Watchdog**: add a *rating-coverage* failing condition to
  `classify_latest_trading_day_completeness` using the **same** `D`, with a distinct
  `reason="rating_coverage_below_threshold"`, OR'd into the `cli.py:138` exit. Because gate and
  watchdog share `D`, "gate passed" ⇔ "rating coverage will pass."

**Must ship together** — the watchdog alone just turns runs red with still-no-ratings.

### 6.2 B′ universe (fixes Problem 2, demotes Finviz)

1. **Parser fix** — extract the ticker from the anchor `href`'s `t=` query param, not cell `.text`.
   Verified against the live DOM (`t=AACB`, `t=AAC-U`, hyphen form). Implement as a thin owned
   extraction (subclass `Overview` and override the row parse, or a small dedicated `screener.ashx`
   fetch+parse). Prefer owning it — `finvizfinance` is effectively unmaintained (1.3.0 latest, logo
   bug unfixed) and this is the 2nd Finviz incident.
2. **Source-agnostic validity guards** (added to whatever source is primary, and to the weekly
   enrichment):
   - **Anchor set present**: `{SPY,QQQ,AAPL,MSFT,NVDA,AMZN,GOOGL,…}` must all be in the fetch or
     **reject**.
   - **Symbol shape**: `^[A-Z][A-Z.\-]{0,5}$`.
   - **Day-over-day Jaccard** vs last-good ≥ ~0.9 (rejects a wholesale swap).
   - On reject: keep last-good, mark run untrusted (existing `trusted=False` path).
3. **Weekly demotion**: the Finviz fetch (membership + sector) runs **weekly**, cached; the **daily**
   `update` uses the cached membership and never hits Finviz on the critical path. A stale/failed
   weekly fetch leaves the last-good membership in place and never blocks the daily run.
4. **Keep market-cap $50M+ semantics** — no liquidity filter, no Volume column.
5. **Nasdaq Trader** stays the emergency fallback (Finviz dead **and** no cache). Optional light
   Security-Name cleanup (defer unless it actually gets exercised).
6. **Purge** the corrupted `ticker_list` + `ticker_list_date` meta **only after** the parser fix
   ships (so the forced refetch can't re-corrupt). Add a read-time validity check on the cache.

---

## 7. Slice-by-slice plan

Each slice = one PR, green CI required before the next. `→ verify:` lines are the success criteria.

### Slice 0 — CI safety net (independent, ship first)
- Add `.github/workflows/ci.yml`: `pip install -r requirements.lock` + `pip install -e .[dev]`,
  run `pytest` on `push` + `pull_request`, matrix Python 3.10 & 3.12.
- Gate `publish.yml` on a green test job (`needs:`); assert release **tag == pyproject version**; add
  `python -m build` smoke.
- Fix version drift: `ibd_rs/__init__.py` (0.3.0) vs `pyproject.toml` / `rs_rating/__init__.py` (0.4.0)
  → **one source of truth**; replace the hardcoded `test_client.py` version assertion with one that
  parses `pyproject.toml` and asserts both packages match.
- → verify: a meta-test asserts a workflow file invokes `pytest`; CI is green on a trivial PR;
  `publish` cannot run without tests.

### Slice 1 — Unified stationary gate + watchdog rating gate (CO-SHIP)
- `compute_rs_rating(rs_raw_df, active_universe: set, min_universe_fraction=…)`; denominator
  `max(len(active_universe), UNIVERSE_FLOOR)`; numerator restricted to `active_universe ∪ REFERENCE`.
- Thread `active_universe` from `calculate_and_store` (currently `universe_size=len(price_df.columns)`,
  rs.py:100) and `cli`.
- Add rating-coverage failing condition to `classify_latest_trading_day_completeness` (db.py:372-384)
  with `reason="rating_coverage_below_threshold"`; OR it into `cmd_update` exit (cli.py:138).
- → verify (regression tests that reproduce the bugs):
  - **BUG1**: an inflated union (columns ≫ active set) with real coverage ≥ threshold-of-active still
    rates the date (today's fixtures never exercise this — add it).
  - **BUG3**: high close coverage + zero rating coverage ⇒ `cmd_update` raises `SystemExit(1)`.
  - Existing `test_rs.py` still passes.

### Slice 2 — Destructive-path guards + rating-only backfill primitive
- Hard-guard `recalc_all` / `cmd_recalc` / `cmd_init` / `init.yml` behind explicit `--force-full`
  **and** a precondition: refuse unless the loaded close window spans ≥ `252 + margin` trading days
  for the oldest date being cleared (i.e. never on a 13-month-pruned DB). Treat a **NULL rs cursor as
  a fatal abort**, not a full recompute (rs.py:108).
- Correct the three runbook docs that recommend `recalc` for this symptom: `Operations.md:199/203`,
  `CLI-Reference.md:152-163`, `Troubleshooting.md:218-228`.
- Add `db.get_rs_raw_df(conn, start, end)` (pivots **stored** rs_raw) + a **rating-only** backfill
  command (e.g. `recalc --from --to`) that re-ranks stored rs_raw under the fixed gate and writes
  **only** `rs_rating` via `UPDATE rs SET rs_rating=%s WHERE ticker=%s AND date=%s`. It must never call
  `clear_rs_for_dates`, `upsert_rs` (which overwrites rs_raw), or recompute rs_raw from closes.
- → verify: guard refuses on a pruned-window fixture; backfill re-ranks a nulled date without touching
  rs_raw/close (byte-identical); backfill is idempotent (second run = no-op); NULL cursor aborts.

### Slice 3 — Retention / prune safety
- Change prune DELETE predicate to `rs_raw IS NULL AND rs_rating IS NULL` (db.py:269-272) so an
  outage's rs_raw survives past cutoff and stays re-rankable.
- Express `PRICE_RETENTION_MONTHS` as trading days with explicit margin
  `≥ RS_RECOMPUTE_WINDOW_DAYS + 252 + buffer` (widen to ~15 months); rewrite
  `test_recompute_window_leaves_enough_lookback_margin` (test_rs.py:327-331) to model
  calendar→trading-day (`≈ days × 252/365`), fixing the false 21-day claim.
- Invariant in code: never clear a date unless the same unit of work rewrites it.
- → verify: prune leaves rows with rs_raw intact; corrected margin test; a holiday-dense window keeps
  a positive margin.

### Slice 4 — Universe re-architecture (B′)
- Owned `href`-based ticker parser (see §6.2.1); drop reliance on `finvizfinance`'s cell-text parse.
- Validity guards (§6.2.2): anchor-set, symbol-shape, day-over-day Jaccard; fail-loud → last-good.
- Weekly Finviz fetch (membership + sector) cached; daily `update` consumes the cache and never hits
  Finviz live (§6.2.3). Add a read-time validity check on the cache.
- Keep market-cap semantics; Nasdaq Trader remains emergency-only.
- → verify: parse test feeds the logo DOM and asserts clean tickers; anchor-rejection test (a
  corrupted-but-full-count fetch is rejected and last-good served); symbol-shape + Jaccard tests;
  daily-uses-cache test (no Finviz call on the daily path).

### Slice 5 — Sector/industry decoupling & client graceful degradation
- The weekly job (Slice 4) already yields sector/industry; ensure it **only** upserts sector/industry
  on the enrichment path, is corruption-resistant (same validity guards), and never gates the daily
  run.
- Make `client.sectors/industries/sector_ranking/industry_ranking/sector_top/industry_top`
  (client.py:311-513) degrade gracefully on partial/empty sector data.
- (Tickers-table cleanup itself happens in the Recovery run, §9.)
- → verify: client sector methods return sensible partial results when the tickers table is
  incomplete; enrichment failure does not fail `update`.

### Slice 6 — Split detection / repair correctness
- Make the scan window strictly exceed the 10-day re-download window (≥15d) **or** compare each
  re-downloaded date against the stored value before overwrite (splits.py:18-28 vs prices.py:45).
  Keep `yf.Ticker(t).splits` as the confirmation gate.
- Bound `verify_and_repair` re-download to the retention window (not `2y`, splits.py:75); route it
  through the shared batched/backoff wrapper with a per-run cap; clamp write dates ≤ today.
- → verify: auto-adjust-seam regression (a split at T shows up and is repaired); repair does not write
  pre-retention rows.

### Slice 7 — Latest-date semantics, staleness, per-ticker gaps
- One canonical **rating-based** latest date for `cmd_top` / `cmd_export` (cli.py:153,224), consistent
  with `client._latest_date`.
- Client **staleness signal**: warn when the latest rated date lags the latest close date.
- Per-ticker latest-session staleness detection in `_tickers_with_close_data` (prices.py:86-100).
- Always write `failed_tickers` (empty on a clean run) instead of only on failure.
- → verify: cmd_top on an outage-shaped DB anchors on the latest *rated* date; staleness warning
  fires; a ticker missing only the newest session is flagged.

### Slice 8 — Durability & public contract
- Document per-column NULL semantics in one place; make clear+recompute atomic (clear only what you
  rewrite / spanning transaction); add `reconnect()` liveness around the long download and before the
  RS store (not only after the Finviz fetch, tickers.py:190).
- Bring **RLS / POLICY / GRANT** DDL into a versioned migration + `init_db` (prefer **table-level**
  `GRANT SELECT` so additive columns stay covered). Add a **secrets-gated Neon-branch E2E** that hits
  the real Data API with an anonymous token (kept off the per-PR path).
- → verify: a mid-run failure leaves no committed rating hole; rebuild-from-repo grants anonymous
  read; Neon-branch E2E passes each client method.

**Release points:** Slices 0–4 + Recovery ship as **v0.5.0** (restored + re-architected). Slices 5–8
land as v0.5.x / v0.6.0 as completed.

---

## 8. Testing, E2E & local-first migration strategy

**Layered, all $0:**
1. **Local SQLite** (`db.get_connection(":memory:")`, already how tests run) — all unit + pipeline
   E2E. This is where ~90% of iteration happens. Add a synthetic **full-`update()` E2E** with ~4,600
   synthetic tickers covering: normal day, denominator-inflation, corrupted-but-full-count fetch,
   high-close/zero-rating, the recovery re-rank, and the seam.
2. **Local Postgres via OrbStack** (`docker run postgres`) — validate the pg-specific SQL
   (`execute_values`, `ON CONFLICT`, retention SQL, any migration) that SQLite can't. **Rehearse the
   production migration here on a dump of prod data.**
3. **Neon branch** (free copy-on-write) — final rehearsal on real prod data + real Neon behavior
   (serverless, Data API, RLS). Secrets-gated client E2E runs here.
4. **Production Neon** — apply the validated migration **once**, snapshot first.

**Migration rehearsal flow:** `pg_dump` prod (or a Neon branch) → load into OrbStack Postgres →
run migration + recovery → verify → repeat on a fresh Neon branch → snapshot → production.

---

## 9. Recovery runbook (operational — run after Slices 0–4 are merged & green)

Prerequisites: Slice 1 (fixed gate), Slice 2 (backfill primitive + guards), Slice 3 (prune safety),
Slice 4 (fixed universe, for the 07-20/21/22 re-download).

- **R1 — Freeze cron.** Disable the `schedule:` trigger in `daily_update.yml` (comment it out on a
  branch, or disable the workflow). No scheduled run may fire during R2–R7. *(Consider freezing early,
  even before slices land, to stop further garbage writes / prune damage — the trailing-window
  download backfills the gap on resume as long as the freeze < 10 days; if longer, do a one-off wider
  catch-up download on resume.)*
- **R2 — Snapshot.** Neon copy-on-write branch **and** `pg_dump` of `rs`/`tickers`/`meta`. Record
  baseline checksums: md5 over `(ticker,date,close,rs_raw,rs_rating)` for `date ≤ 2026-06-29`, and an
  rs_raw fingerprint for `2026-06-30..2026-07-17`.
- **R3 — Purge corrupt meta.** Delete `ticker_list` + `ticker_list_date` (only now that refetch can't
  re-corrupt), then run the fixed weekly fetch to re-cache a clean market-cap universe.
- **R4 — Pre-flight asserts.** rs_raw cursor still 07-22; anchors (AAPL/MSFT/…) present in the new
  universe; loaded close window unchanged; guard on `--force-full` confirmed active.
- **R5 — Rating-only re-rank 2026-06-30 → 2026-07-17.** Use the Slice-2 backfill (`get_rs_raw_df` +
  rating-only `UPDATE`) under the fixed gate. `rs_raw`/`close` untouched. These dates' prices are good
  (~4,700 closes); only the rating was gated off.
- **R6 — Re-download + rate 2026-07-20/21/22.** Under the **new** universe, download real closes for
  these 3 days, compute rs_raw + rating for them, then reconcile the `tickers` table: DELETE rows not
  in the corrected universe (the garbage `AAAPL`/`AABBV`/… enrichment). (The `rs` table has no garbage
  rows to clean.)
- **R7 — Verify.** `date ≤ 2026-06-29` checksums byte-identical; `06-30..07-17` rs_raw fingerprints
  byte-identical; every hole date now has rating coverage > 0 with values in [1,99]; a re-run of R5/R6
  is a no-op (idempotent).
- **R8 — Resume cron** (+ one-off catch-up download if the freeze exceeded the trailing window).

The universe/methodology boundary lands at the **07-18→07-20 weekend** by construction; because B′
preserves market-cap semantics, this is a "definition restoration," not a real discontinuity.

---

## 10. Git / release process
- Slice-by-slice: one branch + PR per slice, green CI required to merge (Slice 0 provides the gate).
- Conventional slice PR titles (matches project history: "Slice N: …").
- **v0.5.0** after Slices 0–4 + Recovery: CHANGELOG entry (restored rating series; stationary gate;
  honest watchdog; Finviz demoted + `href` parser; CI; recovery), annotated tag, GitHub release
  → `publish.yml` ships to PyPI (now test-gated). Surface a **universe-definition version** to
  consumers in the release notes.
- Slices 5–8 as v0.5.x / v0.6.0.

---

## 11. Risks & rollback
- **Backfill touches rs_raw** → rollback from R2 snapshot; the byte-identical checksums in R7 are the
  guard. The Slice-2 primitive writes only `rs_rating`, so this should be impossible by construction —
  the checksums prove it.
- **Weekly Finviz breaks again** → validity guards fail loud + last-good membership; daily runs
  continue; Nasdaq Trader is the last-resort fallback.
- **Migration on prod** → rehearsed on OrbStack + Neon branch first; snapshot before apply.
- **Freeze > 10 days** → trailing window won't cover the gap; do a one-off wider catch-up download on
  resume (bounded to ≤ retention).

---

## 12. Open items for the implementing session
- Confirm the exact list for the **anchor set** (start with mega-caps + SPY/QQQ; ~15 names).
- Decide parser form: subclass-override vs a fully owned `screener.ashx` fetch (lean owned).
- Pick the **weekly** cadence mechanism: a second scheduled workflow vs a longer cache + a "refresh if
  older than 7d" check inside `update` (guarded so a failed refresh never blocks the daily path).
- Confirm `.[dev]` extras exist in `pyproject.toml` (add pytest etc. if missing) for CI.
- The `.venv` in this repo has stale shebangs (moved from `~/Documents/...`); the implementing session
  should recreate it (`python -m venv`) or use `python -m pip`.
