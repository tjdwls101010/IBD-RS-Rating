# Master Plan — Neon Migration & Reliability Recovery

**Date: 2026-07-11** · Author: planning session (Claude) with PM (chunghun1) · Evidence: [`2026-07-11-research-appendix.md`](./2026-07-11-research-appendix.md)

**Status: APPROVED FOR IMPLEMENTATION.** All strategic decisions in §3 are locked with the PM. This document is the durable contract for a fresh implementation session — it holds everything needed to execute without re-deriving the diagnosis or re-litigating the decisions.

---

## 0. How to use this document (for the next session)

Read §1–§4 to load the full picture, then execute §5 in the order given by §6. Each slice in §5 is a vertical slice meant to become one GitHub issue and one branch/PR, following the repo's issue template in `AGENTS.md`. The RS formula (weights `0.4/0.2/0.2/0.2` over `63/126/189/252` trading days) and the settled decisions in `docs/DECISIONS.md` are **not** in scope to change — preserve them. Before touching production data, re-read §2.3 and §5 Slice 5: a naive `recalc_all` or `init` will destroy 12 months of RS history. When implementation surfaces a decision this plan did not make, stop and re-grill the PM per the "Grilling Returns" rule in `AGENTS.md`; do not silently guess.

---

## 1. Executive summary

The daily pipeline is silently frozen and the database is nearly full — two independent problems that both need fixing, plus a planned infrastructure move to Neon.

**Problem 1 — the live freeze.** On 2026-07-08 the Finviz ticker fetch returned only 55 tickers instead of ~4,600 (a fetch truncated ~3 pages into a 248-page scrape, almost certainly a rate-limit/block on the GitHub Actions IP). That 55-ticker list was cached for 30 days, so every run since has downloaded only 55 tickers and rated zero, while the workflow still reports "success." The last healthy trading day is 2026-07-07. Without intervention the dataset stays frozen until the cache expires around 2026-08-07, and only if Finviz happens to return a full list that day.

**Problem 2 — the database is at 95% of the free limit.** The Supabase database is 475 MB against the 500 MB free ceiling, driven by table bloat from the daily UPDATE/DELETE churn. Restoring full 4,600-ticker writes (the recovery) accelerates growth toward the wall, where writes start failing. The PM is on a paid Neon **Launch** plan (10 GB), so migrating to Neon both removes this ceiling and — because a fresh `pg_restore` rebuilds the tables compactly — reclaims the bloat.

**The plan.** Harden the pipeline so a degraded ticker fetch can never again freeze the dataset; migrate the database and the public read API to Neon (already paid for); surgically recover the frozen days without destroying history; then ship a new PyPI release. The reliability fixes are database-agnostic and independently valuable; the Neon read cutover is gated on an empirical proof-of-concept because Neon's Data API is in Beta.

---

## 2. Diagnosis — two independent problems

### 2.1 Problem 1: the Finviz freeze (live incident)

The root cause is a truncated ticker universe fetch that got cached, amplified by two blind spots. The causal chain, reconstructed from run logs and the `meta` cursor state:

- The ticker universe is fetched from Finviz and cached in the `meta` table for 30 days (`CACHE_DAYS = 30`). A fresh fetch happened around 2026-06-08 (~4,600 tickers) and was cached.
- On 2026-07-08 the cache hit exactly 30 days old and expired, triggering a fresh Finviz fetch. That fetch returned only 55 tickers — the `finviz` library paginates ~248 pages and was cut off after ~3 pages (`55 ≈ 3 pages × 20 rows − excluded ETFs`), consistent with Finviz rate-limiting/blocking the GitHub Actions datacenter IP.
- Nothing validated the size, so the 55-ticker list was written to `ticker_list` and `ticker_list_date` was advanced to 2026-07-08 — re-caching the bad list for another 30 days.
- Since then every daily run downloads only those 55 tickers, and the RS engine correctly leaves each day unrated (55 valid tickers against a ~4,600 full-table universe is ~1.2% coverage, far below the 90% gate), so `rs_rating` is NULL for 2026-07-08 onward while `close` exists for only ~55 tickers.

Two blind spots let this pass as "success":

- **The watchdog measures coverage against the fetched universe, not an absolute expectation.** `check_latest_trading_day_completeness` compares close coverage to the passed `ticker_list` (the collapsed 55). 55/55 = 100% → PASS. The watchdog added in v0.3.1 was designed to catch a silent stall but is blind to a *universe collapse* because it trusts the collapsed universe as its denominator.
- **The v0.3.1 recovery explicitly anticipated this and deferred the guard.** `docs/PRD.md` "Out of Scope" says the Finviz work would be "version pin + empty-list defense only." The empty-list defense was never implemented, and it would not have caught 55 anyway — the guard needed to reject *suspiciously small* lists, not just empty ones. This incident is the realization of a flagged, deferred risk.

**Finviz itself is currently healthy** (a read-only check on 2026-07-11 paginated all ~248 pages normally), so the collapse was a transient truncation, not a permanent library/site break. Recovery therefore only requires clearing the cached list; durable prevention requires the guards in §5 Slice 1–2.

### 2.2 Problem 2: database at 95% of the free limit (looming, independent)

The Supabase database measures 475 MB against the 500 MB free-tier ceiling. The `rs` table is 463 MB for 1,258,712 rows — roughly double the ~200 MB the data itself should occupy — because the daily write pattern (price upserts, RS clear-then-reset on recompute, retention deletes, split repairs) generates dead tuples faster than autovacuum reclaims them. This is not the cause of the freeze (writes still succeed; only 55 tiny rows are written per day), but it is a second, independent time bomb: when the database reaches 500 MB, writes begin to fail and the pipeline freezes again. Restoring full 4,600-ticker daily writes will accelerate the approach to that wall, so capacity relief must precede or accompany the recovery.

### 2.3 Secondary defects surfaced during diagnosis

- **The incremental RS cursor leaves permanent holes.** `calculate_and_store(recalc_all=False)` computes only dates strictly after `get_latest_rs_date()` (= `MAX(date) WHERE rs_raw IS NOT NULL`). When a day is stored with `rs_raw` but left unrated (rating NULL) because coverage was low, the cursor still advances past it, so it is never re-rated even after the data becomes complete. The frozen 2026-07-08/09 days have `rs_raw` (55/54 tickers) but NULL rating, so `get_latest_rs_date()` returns 2026-07-09 and the incremental path will never fix them.
- **`recalc_all` on the pruned database destroys history.** Prices are retained for 13 months (`prune_old_close`), but RS ratings are kept forever. Running `recalc_all` loads only the 13-month price window, so only the most recent ~21 trading days have enough 252-day lookback to be recomputed; `clear_rs_for_dates` then NULLs every date in that window and re-stores only the recomputable tail. Measured: of 268 rated trading days, only ~21 are recomputable, so `recalc_all` would erase RS for ~247 days (2025-06 to 2026-06). **The recovery must be surgical (§5 Slice 5), never `recalc_all` and never a naive re-`init`.**
- **The `meta` table is publicly readable.** The Supabase RLS policy `read_meta USING (true)` exposes internal cursors and the ticker list via the anon key. Low severity (only ticker symbols and dates), but the Neon read grants in Slice 6 will fix it by granting SELECT only on `rs` and `tickers`.
- **GitHub Actions scheduling fragility.** The 2026-07-10 (Fri) scheduled run was dropped (GitHub occasionally skips scheduled runs), and scheduled workflows are auto-disabled after 60 days without a commit. Neither caused this incident but both are operational risks worth a low-priority guard (§5 Slice 9).

The exact measured state (data fingerprint) is in the research appendix §E; use it to verify recovery.

---

## 3. Locked decisions (with rationale)

These are agreed with the PM in the planning session. Each is an ADR-worthy decision; the implementation session should also append the durable ones to `docs/DECISIONS.md`.

1. **Scope: one combined effort** — reliability hardening + full Neon migration + delivery through a new PyPI release. Rationale: the database is at 95% and the recovery accelerates growth, so capacity (which the paid Neon Launch plan solves) cannot be deferred; the PM wants infrastructure consolidated on the Neon subscription they already pay for.
2. **Migrate everything to Neon Launch** — the write database *and* the public read API. Rationale: Launch's 10 GB removes the capacity ceiling at zero marginal cost (already paid), and `pg_restore` reclaims the bloat. Neon Free would have been a lateral move (same 500 MB), but Launch is not.
3. **Public read via Neon's Data API, anonymous `db_anon_role` path.** ~~Originally gated on proving a header-less request~~ — **superseded 2026-07-11** (see `docs/DECISIONS.md`): the Slice 0 PoC proved a truly header-less GET is rejected unconditionally by the Data API regardless of `db_anon_role`/`GRANT`/RLS config (see `docs/plans/2026-07-11-slice0-poc-results.md`). The confirmed working path — and Neon's own documented design for this use case — is the client fetching a short-lived anonymous JWT from `GET /token/anonymous` on first use, caching it in memory (~1 hour TTL), and sending it as `Authorization: Bearer` on subsequent calls. PM-approved 2026-07-11: adopt this pattern. It is still zero third-party dependencies (pure stdlib) and still requires no user signup/registration — the only change from the original ideal is one extra round trip on cold start / hourly refresh and a small amount of in-memory client state. Do **not** make users interact with Neon Auth directly (no signup flow, no user-visible tokens) — the token fetch/cache is entirely internal to the `rs_rating` client.
4. **Beta safety net: all-in on cutover, decommission Supabase.** Once the PoC passes and the new client version ships, Supabase is retired. Rationale: the PM chose the simplest complete consolidation; the accepted cost is that installed `0.3.x` clients (which hardcode the Supabase endpoint) go stale until users upgrade, which is announced via CHANGELOG/README deprecation.
5. **Surgical recovery, keep current dataset depth.** Recover only the frozen recent days by recomputing them; preserve the existing 13-month RS history by migrating it as-is via dump/restore. No `recalc_all`, no re-`init`. Rationale: `recalc_all`/`init` on the pruned database would destroy ~247 days of RS history (§2.3); the existing history is fine and grows forward to 2+ years within a year.
6. **Retention unchanged: 13-month `close`, RS forever.** Rationale: this is already implemented and correct; `close` storage is already bounded, so trimming prices further yields little. The real growth driver is RS-forever (~1.16M rows/year), which Neon Launch's 10 GB absorbs for decades. Do not cut the window below 13 months — the ROC(252) term needs 252 trading days plus buffer.
7. **Price data source: free hardening of yfinance only.** No paid sources (EODHD etc.). Rationale: PM decision; the 90% completeness gate remains the backstop for the residual partial-failure days that free hardening cannot fully eliminate.
8. **Universe source: `finvizfinance` primary + Nasdaq Trader anchor/fallback + a sanity floor.** Rationale: Finviz is the only free source with cap + sector + industry in one call; `finvizfinance` is better maintained; Nasdaq Trader is a scrape-resistant size anchor and hard fallback; the floor + no-cache-on-failure is what actually prevents a repeat.
9. **Version bump to a new release with a public-endpoint change.** Recommended `0.4.0` (method surface unchanged; backend endpoint changes) with a prominent deprecation notice; `1.0.0` is defensible given the endpoint cutover. Final number is an open question for release time (§11).

---

## 4. Target architecture

The target system after this plan, described as behavior and seams (no file paths — they go stale).

- **Universe acquisition.** A `finvizfinance`-based screener returns cap-filtered tickers with sector/industry as the primary source. Every fetch is validated against a sanity floor (absolute `< 3,000` reject, or day-over-day `< 0.90 × last_good` reject, plus a completeness check against Finviz's own reported total). A failed or suspiciously small fetch is never cached and never advances the fetch timestamp; the run retries with backoff, falls back to the last-good cached list, and if none exists derives a broader universe from the Nasdaq Trader directory (sector/industry null). A fetch that cannot produce a trusted universe makes the run exit non-zero. The cache window is 7 days, with `last_successful_fetch` tracked separately.
- **Price ingestion.** yfinance downloads a fixed trailing window for the full universe in smaller batches (~50-80) with reduced concurrency, explicit exponential backoff, and a pinned known-good version. Missing tickers are detected by return-coverage (unchanged from v0.3.1). Partial-failure days below the 90% gate leave that day unrated rather than producing meaningless ratings.
- **RS computation.** Unchanged formula and population rules. The incremental path recomputes a small trailing window of recent trading days every run (not just dates strictly after the cursor), so a day left unrated by an earlier low-coverage run is re-rated automatically once its data completes — this is the self-healing mechanism and the recovery mechanism in one.
- **Completeness watchdog.** Coverage is measured against an absolute expected universe (the last-good universe size or a fixed floor), not the fetched universe, so a universe collapse fails the run and fires the existing GitHub failure email.
- **Database.** Neon Postgres (Launch plan), accessed by the daily writer over the direct (unpooled) endpoint with `sslmode=require`. Schema unchanged (`rs`, `tickers`, `meta`). The write path is plain psycopg2 `ON CONFLICT` upserts, unchanged.
- **Public read API.** Neon Data API (PostgREST-compatible) with the anonymous `db_anon_role`. RLS enabled on `rs` and `tickers` with permissive SELECT policies; SELECT granted to the anonymous role on `rs` and `tickers` only (not `meta`). The published `rs_rating` client points at the Neon `apirest` endpoint and, on first use, fetches a short-lived anonymous JWT from `GET /token/anonymous` (no auth needed for that call either), caches it in memory, and sends it as `Authorization: Bearer` on every query, refreshing automatically when it expires (~1 hour TTL, confirmed empirically in the Slice 0 PoC).
- **Client.** `rs_rating` keeps its method surface and PostgREST-style query strings (drop-in compatible), changes its default base URL to Neon, and drops the hardcoded Supabase anon key in favor of the in-memory-cached anonymous JWT described above. It retains the `url`/`key` constructor overrides for flexibility.

---

## 5. Execution plan — vertical slices

Each slice is one GitHub issue and one branch/PR. `AFK` = an agent can finish autonomously; `HITL` = needs human judgment or touches production. Behavioral scenarios use Given/When/Then; the *Then* is the acceptance criterion. All engine tests must be offline and deterministic (mock yfinance/finviz/HTTP at the boundary), matching the existing 83-test suite.

### Slice 0 — Neon project + Data API anonymous-read PoC (HITL, gate) — DONE 2026-07-11

**Context.** Everything in the read-path migration depends on whether Neon's Beta Data API can serve a header-less anonymous read. This slice proves it before any cutover work, so a failed assumption costs a PoC rather than a broken public client. Blocked by: none — do this first.

**What was built.** Confirmed the existing Neon Launch project (`IBD-RS-RATING`, id `withered-cherry-36264108`). On throwaway tables on its `production` branch, enabled the Data API, set `db_anon_role = anonymous`, `GRANT SELECT` to `anonymous` on one table only, enabled RLS with a permissive SELECT policy, and issued GETs from pure-stdlib `urllib`/`curl` both with and without an Authorization header.

**Result — gate triggered, then resolved.** A truly header-less GET is rejected unconditionally ("missing authentication credentials"), independent of grants and independent of whether an auth provider is configured — full evidence in `docs/plans/2026-07-11-slice0-poc-results.md`. The only working anonymous path is `GET /token/anonymous` → cache the returned JWT (~1hr TTL) → send as `Authorization: Bearer`, which is Neon's own documented intended design, not a workaround. This is exactly the pattern the original Gate below said not to ship, so implementation stopped and the PM was re-grilled per `AGENTS.md` Grilling Returns. **PM decision (2026-07-11, recorded in `docs/DECISIONS.md`): adopt the token-fetch-and-cache pattern.** Still zero third-party dependencies, still no user signup. See the updated Decision 3 in §3 and the updated Slice 6 below.

**Behavioral scenarios (as actually verified).**
- **Given** a Neon table with RLS + SELECT granted to the anonymous role, **When** a header-less GET hits `/rest/v1/<table>`, **Then** it is rejected with "missing authentication credentials" (not the originally hypothesized pass-through).
- **Given** the same table, **When** a GET carries `Authorization: Bearer <token>` fetched from `/token/anonymous`, **Then** the expected rows return as PostgREST-style JSON.
- **Given** a table without the anonymous SELECT grant, **When** the same Bearer-token GET runs, **Then** it is rejected with a permission-denied error (confirming the grant, not the token, is what gates data access).

**Original gate (for history).** If header-less anonymous read does not work, STOP and re-grill the PM: options were (a) keep Supabase for reads only, (b) a tiny serverless read proxy over Neon, (c) delay the read cutover until the Data API is GA, (d) adopt the token-fetch-and-cache pattern despite the original concern. **(d) was chosen.**

**Out of scope.** Any client code change (deferred to Slice 6); any production data (none touched — project schema was empty at PoC time).

### Slice 1 — Universe acquisition hardening (AFK, offline TDD)

**Context.** Directly fixes the freeze root cause (§2.1). Satisfies the durable prevention the v0.3.1 PRD deferred. Blocked by: none.

**What to build.** A universe fetch that validates size and never caches a bad result. Migrate `mariostoev/finviz` → `finvizfinance`. Add a sanity floor, a completeness guard against Finviz's reported total, retry with exponential backoff, and a Nasdaq Trader anchor + hard fallback. Reduce `CACHE_DAYS` 30 → 7 and track `last_successful_fetch`.

**Key interfaces & modules.** The `fetch_ticker_list` / `_fetch_from_finviz` behavior in the **tickers** module (ubiquitous language: 종목 유니버스). New: a validation step (floor + drop-guard + completeness), a fallback source, and a "do not cache on failure" rule. Config: `CACHE_DAYS`, a universe floor constant, a drop-guard fraction.

**Behavioral scenarios.**
- **Given** Finviz returns ~4,600 tickers, **When** the universe is fetched, **Then** it is cached and the fetch timestamp advances.
- **Given** Finviz returns 55 tickers (or any count below the absolute floor, or below 0.90 × last-good), **When** the universe is fetched, **Then** the result is rejected, `ticker_list`/`ticker_list_date` are NOT written, the last-good list is served, and the run signals failure (non-zero exit) so CI goes red.
- **Given** Finviz fails all retries and a last-good cache exists, **When** the universe is fetched, **Then** the last-good list is used and the failure is logged/annotated.
- **Given** Finviz fails all retries and no cache exists at all, **When** the universe is fetched, **Then** the universe is derived from Nasdaq Trader common stocks (sector/industry null) rather than freezing or emptying.
- **Given** a partial Finviz fetch that looks plausible but is below 0.98 × Finviz's reported total, **When** validated, **Then** it is rejected as truncated.

**Out of scope.** Residential-proxy / non-datacenter runner mitigations (note as a follow-up if Finviz blocking becomes frequent). Changing sector/industry semantics.

### Slice 2 — Watchdog absolute floor + self-healing recompute (AFK, offline TDD)

**Context.** Closes the two blind spots in §2.1/§2.3: the watchdog's universe-relative denominator and the permanent-hole cursor. The recompute change is also the recovery mechanism (Slice 5). Blocked by: none.

**What to build.** Change the completeness watchdog to measure close/rating coverage against an absolute expected universe (last-good universe size or a fixed expected floor), so a universe collapse fails the run. Change the incremental RS path to recompute a bounded trailing window of recent trading days every run (clearing and recomputing only those recent dates), so an unrated recent day is re-rated once its data completes — without touching dates outside the window (history preserved).

**Key interfaces & modules.** `check_latest_trading_day_completeness` in the **db** module: denominator becomes an absolute expectation, not the fetched universe. `calculate_and_store` incremental branch in the **rs** module: recompute a recent trailing window instead of strictly `> last_rs_date`. The trailing window must be small enough that every date in it has ≥252 trading days of lookback in the retained 13-month price window.

**Behavioral scenarios.**
- **Given** the fetched universe collapses to 55 while the last-good expectation is ~4,600, **When** the watchdog runs, **Then** coverage is ~1% against the absolute expectation and the run FAILS (not PASS).
- **Given** a recent trading day stored with `rs_raw` but NULL rating (low earlier coverage), **When** a later run executes with the full universe present, **Then** that day is re-rated with full population coverage.
- **Given** rated trading days older than the trailing recompute window, **When** the incremental path runs, **Then** those historical ratings are left untouched (no clear, no recompute).
- **Given** the retained price window (13 months), **When** the trailing recompute runs, **Then** every recomputed date has ≥252 valid trading days of lookback (no spurious NaN from a too-wide window).

**Out of scope.** Changing the 90% gate value or the RS formula.

### Slice 3 — yfinance free hardening (AFK, offline TDD)

**Context.** Reduces the ~14.6% partial-failure days (Decision 7). Blocked by: none.

**What to build.** Smaller batch size (~50-80), reduced concurrency, explicit exponential backoff with jitter on 429/failures, and a pinned known-good yfinance (≥0.2.63 with curl_cffi≥0.7) in `requirements.lock`. Keep return-coverage failure detection unchanged.

**Key interfaces & modules.** `_download_batch` / `download_update` / `download_initial` in the **prices** module: batch sizing, retry/backoff, concurrency config. `BATCH_SIZE` and new backoff/throttle constants in config. `requirements.lock` pin + a CI-dependency test.

**Behavioral scenarios.**
- **Given** a batch download raises a rate-limit error once then succeeds, **When** `download_update` runs, **Then** it backs off and retries and ultimately stores the data (mocked boundary, deterministic).
- **Given** a batch download keeps failing, **When** retries are exhausted, **Then** those tickers are recorded as failed (return-coverage) and the run continues, leaving the day to the 90% gate.
- **Given** the trailing window overlaps prior data, **When** the update runs, **Then** upserts remain idempotent (existing behavior preserved).

**Out of scope.** Any paid source. Wrapping `yf.download` in a custom thread pool (it is not thread-safe).

### Slice 4 — Neon migration: schema + data + secrets (HITL)

**Context.** Moves the write database to Neon Launch, reclaiming bloat and removing the 500 MB ceiling. Blocked by: Slice 0 (Neon project exists).

**What to build.** `pg_dump -Fc --schema=public` from Supabase (session-mode connection), `CREATE DATABASE` on Neon, `pg_restore --no-owner --no-acl` over the Neon direct endpoint. Verify row counts against the fingerprint (rs ~1.25M, tickers ~4,801, meta 5). Update the `DATABASE_URL` GitHub Actions secret to the Neon direct endpoint with `?sslmode=require`. Do not cut over reads yet.

**Behavioral scenarios.**
- **Given** the Supabase dump, **When** restored to Neon and row-counted, **Then** counts match the source and the restored `rs` table is materially smaller than 463 MB (bloat reclaimed).
- **Given** the Neon `DATABASE_URL` (direct, sslmode=require), **When** the daily `update` runs manually against Neon, **Then** the psycopg2 `ON CONFLICT` upserts succeed unchanged.
- **Given** the writer role on Neon, **When** it writes after restore, **Then** it owns/can write the restored tables (verify post `--no-owner`).

**Out of scope.** Read-path/client changes (Slice 6). Decommissioning Supabase (Slice 6, after cutover).

### Slice 5 — Surgical data recovery on Neon (HITL, time-sensitive)

**Context.** Unfreezes the pipeline and backfills the frozen days without destroying history. Time-sensitive: the trailing-window backfill for 2026-07-08/09 closes ~2026-07-18; each further frozen trading day adds a permanent 1-day hole once it falls outside the recovery's trailing window. Blocked by: Slice 1 (fixed universe), Slice 2 (self-healing recompute), Slice 4 (data on Neon).

**What to build.** On Neon (headroom, no 500 MB landmine): clear the cached ticker list (`meta` `ticker_list`/`ticker_list_date`) so the next fetch pulls the full universe; run `update`, which fetches the full-universe trailing-window `close` (including the frozen dates) and, via the Slice 2 self-healing recompute, re-rates the recent unrated days. Verify against the fingerprint. **Do not run `recalc_all` or a fresh `init`** (they destroy ~247 days of RS history per §2.3).

**Behavioral scenarios.**
- **Given** the cleared cache and the fixed universe fetch, **When** `update` runs on Neon, **Then** the latest trading days show ~4,600 close coverage and no day sits below the 90% gate.
- **Given** the frozen 2026-07-08/09 days (rs_raw present, rating NULL), **When** recovery runs within the backfill window, **Then** those days are re-rated against the full population.
- **Given** the historical RS (2025-06 .. 2026-06), **When** recovery runs, **Then** it is unchanged (no history destroyed).
- **Given** recovery completes, **When** the watchdog runs, **Then** it PASSES against the absolute expected universe.

**Out of scope.** Extending historical depth (Decision 5 keeps current depth). Re-enabling the cron before hardening is merged.

### Slice 6 — Public read cutover to Neon Data API + client (HITL then AFK)

**Context.** Moves the public read layer to Neon and retires Supabase. Blocked by: Slice 0 (PoC done — token-fetch-and-cache pattern confirmed and PM-approved), Slice 5 (fresh data on Neon).

**What to build.** On Neon: enable the Data API, enable RLS on `rs` and `tickers` with permissive SELECT policies, and `GRANT SELECT` to the anonymous role on `rs` and `tickers` only (not `meta` — fixes the §2.3 leak). Update the `rs_rating` client default base URL to the Neon `apirest` endpoint. Drop the hardcoded Supabase anon key. Add an in-memory token cache: on first request needing auth, `GET /token/anonymous` from the Neon Auth base URL (no auth needed for that call), cache the returned JWT and its `exp`, send it as `Authorization: Bearer` on every `/rest/v1/*` call, and transparently re-fetch when the cached token is expired or absent. Keep the `url`/`key` overrides — `key`, if provided, should still be sendable as a Bearer token directly (bypassing the fetch/cache) for callers who want to supply their own credential. Update the client tests (response shape is identical PostgREST JSON; adjust `test_default_credentials` to cover the token-fetch-and-cache path, including an expiry/refresh test with a mocked clock).

**Behavioral scenarios.**
- **Given** the Neon Data API with anonymous SELECT on `rs`/`tickers`, **When** the updated client calls `get`/`top`/`history`/`sectors` with no caller-supplied token, **Then** it transparently fetches and caches an anonymous token and returns the same shapes the Supabase client returned.
- **Given** a client instance that already has a cached, unexpired anonymous token, **When** a second query runs, **Then** no new `/token/anonymous` call is made (the cached token is reused).
- **Given** a client instance whose cached token has expired, **When** the next query runs, **Then** a fresh token is fetched and cached before the query proceeds.
- **Given** the anonymous role, **When** it attempts to read `meta`, **Then** it is denied (leak fixed).
- **Given** the existing client method surface, **When** the endpoint swaps to Neon, **Then** all public methods work unchanged aside from the internal auth handling (drop-in query compatibility).

**Out of scope.** Expanding the client's API surface (future). Keeping Supabase as a mirror (Decision 4 retires it). Persisting the cached token to disk (in-memory per process instance only).

### Slice 7 — End-to-end verification (HITL)

**Context.** Proves the whole system works against Neon before release. Blocked by: Slices 2, 3 (hardening merged), 5 (data on Neon), 6 (reads on Neon).

**What to build.** A manual `workflow_dispatch` daily `update` against Neon that exercises the full path (universe fetch with guards, price download, RS recompute, watchdog) and passes; a real-HTTP client read against the Neon Data API for `get`/`top`/`history`/`sectors`/`reference`; confirmation that a simulated universe collapse fails the run (watchdog); confirmation the scheduled cron runs against Neon.

**Behavioral scenarios.**
- **Given** the deployed pipeline on Neon, **When** a manual daily update runs, **Then** it completes green with ~4,600 coverage and the watchdog PASS.
- **Given** an injected 55-ticker universe, **When** the update runs, **Then** it is rejected/last-good-served and the run goes red (regression guard for the original incident).
- **Given** the published-client code against the live Neon Data API, **When** the core methods run, **Then** they return correct data with no token.

**Out of scope.** Load/perf testing.

### Slice 8 — Release: version, CHANGELOG, GitHub Release → PyPI (HITL)

**Context.** Ships the change to PyPI via the existing `publish.yml` (triggered on GitHub Release). Blocked by: Slice 7.

**What to build.** Bump `pyproject.toml` and `rs_rating.__version__` (currently stale at 0.3.0 vs pyproject 0.3.1) to the agreed version (§11). Update `CHANGELOG.md` (reliability hardening, Neon migration, read-endpoint change, Supabase deprecation). Update `README` (Neon endpoint, no anon key needed, deprecation note for old installs). Create a GitHub Release (tag `vX.Y.Z`) so `publish.yml` deploys to PyPI. Update `CONTEXT.md`/`DECISIONS.md` per Grilling Returns.

**Behavioral scenarios.**
- **Given** the version bump and a GitHub Release, **When** `publish.yml` runs, **Then** the new version is live on PyPI and `pip install -U` yields the Neon-backed client.
- **Given** a fresh `pip install` of the new version, **When** a user calls `RS().get("NVDA")` with no setup, **Then** it returns data from Neon with no token/signup.

**Out of scope.** Marketing/announcement beyond CHANGELOG/README.

### Slice 9 — GitHub Actions scheduling resilience (AFK, low priority)

**Context.** Mitigates dropped scheduled runs and the 60-day auto-disable (§2.3). Blocked by: none; can land anytime.

**What to build.** Document/handle the auto-disable risk (the regular commits during implementation already reset the 60-day clock; add a note or a lightweight keepalive). Optionally make the schedule more robust to dropped runs. Keep it minimal.

**Out of scope.** New alerting channels (existing failure email suffices per prior decisions).

---

## 6. Recommended execution order & dependencies

The reliability code (Slices 1–3) is database-agnostic and offline-testable, so it can be developed first with fast feedback. The recovery is time-sensitive (backfill window ~2026-07-18) and must run on Neon (to dodge the 500 MB landmine), so migration precedes recovery. Recommended order:

1. **Slice 0** (Neon PoC gate) — DONE 2026-07-11; result and PM decision recorded above and in `docs/DECISIONS.md`.
2. **Slice 1, 2, 3** (reliability hardening) — offline TDD, parallelizable; Slice 1 and 2 are prerequisites for a clean recovery.
3. **Slice 4** (migrate DB to Neon).
4. **Slice 5** (surgical recovery on Neon) — as soon as 1, 2, 4 are done; time-sensitive.
5. **Slice 6** (read cutover + client) — after 0 (PoC) and 5.
6. **Slice 7** (E2E) → **Slice 8** (release).
7. **Slice 9** anytime.

If the implementation session starts after ~2026-07-18, accept that 2026-07-08/09 (and any further frozen days) become permanent 1-day RS holes; the pipeline still fully recovers going forward.

---

## 7. Test strategy

Preserve the existing 83 offline/deterministic tests as regression protection; add tests only on the changed paths. Good tests exercise observable behavior through public interfaces, run without network, and survive refactors (per `AGENTS.md`).

- **Universe hardening (Slice 1):** mock the Finviz/Nasdaq boundary; assert small/truncated results are rejected and not cached, last-good is served, the Nasdaq fallback triggers with no cache, and the run signals failure. This is the RED test that reproduces the 55-ticker incident.
- **Watchdog + self-healing (Slice 2):** in-memory SQLite; assert a collapsed universe fails against the absolute expectation, an unrated recent day gets re-rated on a later full-coverage run, and historical dates outside the trailing window are untouched.
- **yfinance hardening (Slice 3):** mock the download boundary; assert backoff/retry and that exhausted retries record failures without crashing.
- **Client (Slice 6):** the existing `mock_urlopen` tests carry over because the response shape is unchanged; add/adjust the default-endpoint test and a header-less request assertion.
- **E2E (Slice 7):** real runs against Neon (manual), not part of the offline suite.

---

## 8. Release & deployment procedure

1. Land Slices 1–3 (hardening) on branches/PRs with green offline tests.
2. Slice 0 PoC done — result required a PM decision (token-fetch-and-cache adopted); proceed to the read cutover using that pattern.
3. Migrate the database (Slice 4) and update the `DATABASE_URL` secret to Neon (direct, `sslmode=require`).
4. Recover data on Neon (Slice 5) within the backfill window; verify against the fingerprint.
5. Cut over reads and the client (Slice 6); verify; decommission Supabase.
6. E2E verify (Slice 7).
7. Bump version + CHANGELOG + README (Slice 8), create the GitHub Release, and confirm `publish.yml` publishes to PyPI.
8. Re-enable the daily cron only after hardening is merged, so the pipeline cannot re-collapse.

---

## 9. Risks & mitigations

- **Neon Data API is Beta and could change or break** → gated on the Slice 0 PoC (done 2026-07-11); keep the write pipeline decoupled from the read layer; the client retains `url`/`key` overrides so the endpoint can be repointed; announce the version/endpoint change clearly.
- **The anonymous token's ~1 hour TTL means a long-lived client instance needs working refresh logic, and `/token/anonymous` is itself part of the Beta surface** → covered by the Slice 6 expiry/refresh test; if `/token/anonymous` changes shape, only the client's internal token-fetch function needs updating (the public method surface is unaffected).
- **`recalc_all`/`init` destroys RS history** → forbidden in recovery; recovery is surgical (Slice 5); this document flags it in §0, §2.3, and §5.
- **Recovery misses the backfill window** → prioritize Slices 0/1/2/4/5; if missed, accept 1-day holes (pipeline still recovers forward).
- **Finviz blocks the GitHub Actions IP again** → the guards make this a red run with last-good fallback rather than a silent freeze; residential-proxy/non-datacenter runner is a documented follow-up if blocking becomes frequent.
- **Bloat regrows on Neon** → 10 GB gives decades of headroom; rely on autovacuum; a future slice can tune the write pattern (the RS clear-then-reset churn) if needed.
- **Old `0.3.x` installed clients go stale after Supabase decommission** → accepted per Decision 4; mitigated by CHANGELOG/README deprecation and, optionally, leaving Supabase serving its last (frozen) data for a short grace period before deletion.
- **yfinance version bump breaks auth** → pin a known-good version and smoke-test before any bump (Slice 3).

---

## 10. Rollback plan

- **Database migration:** Supabase remains the source of truth until the read cutover; if Neon writes misbehave, revert the `DATABASE_URL` secret to Supabase. Keep the Supabase project until Slice 6 is verified.
- **Read cutover:** if the Neon Data API fails in production after cutover, revert the client default to the Supabase endpoint and re-point the pipeline to Supabase (this is why Supabase is not deleted until Slice 6 is verified end-to-end).
- **Everything is a revertible PR** — one slice = one branch = one PR, per `AGENTS.md`; `git revert` restores any slice.

---

## 11. Open questions / decisions deferred to implementation

- **Release version number:** `0.4.0` (recommended, method surface unchanged) vs `1.0.0` (defensible given the public-endpoint cutover). Decide at Slice 8 with the PM.
- **Exact sanity-floor value:** start at absolute `< 3,000` reject; tighten toward 3,500-4,000 after observing false-positive rate (Slice 1).
- **Trailing recompute window length (Slice 2):** pick the smallest window that reliably covers post-close data lag and short freezes while keeping every date's 252-day lookback inside the 13-month price window.
- **Supabase grace period:** delete immediately after cutover (Decision 4) vs leave the frozen data served for N days as a courtesy to un-upgraded clients. Confirm at Slice 6.
- **Nasdaq Trader fallback cap parity:** on a fallback day the universe is a broader superset (no cap filter); confirm this is acceptable or add a cap re-filter from an existing dependency (Slice 1 open risk).
