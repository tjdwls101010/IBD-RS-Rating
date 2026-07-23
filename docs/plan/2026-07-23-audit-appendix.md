# Audit Appendix — Evidence & Full Findings

_Read-only multi-agent audit, 2026-07-23. 7 agents, 6 dimensions, ~632k tokens, 51 raw findings → 25 deduped ranked defects._

Source of truth (full raw per-agent output): the workflow task output file and
`…/subagents/workflows/wf_a3cd69c7-4e7/journal.jsonl`.


---

## Ranked defects (25)

### D1. [🔴 CRITICAL] RS rating gate denominator is the non-stationary 13-month column UNION, not the active population (Problem 1 root cause)

**Files:** `ibd_rs/rs.py`, `ibd_rs/db.py`, `ibd_rs/config.py`


compute_rs_rating gates each date on valid_counts/universe_size>=0.90 with universe_size=len(price_df.columns) (rs.py:100), and price_df comes from get_prices_df which returns the UNION of every ticker with any close in the ~13mo window (db.py:296, WHERE close IS NOT NULL). That denominator only grows with churn/delistings/garbage while the numerator (>=252-valid-day tickers) is flat, so the ratio decayed below 0.90 around 06-30 and the scalar gate is a hard cliff that NULLs the ENTIRE recompute window at once (rs.py:74-76). This is the direct cause of the 06-30..07-22 rating outage. Cross-cuts three divergent denominators: RS gate (union), watchdog (max(active,FLOOR) at db.py:432), and the future liquidity filter — none share a source of truth. No regression test exercises the inflated union (all fixtures keep columns==active).


**Fix direction:** Thread the active-universe SET (floored at UNIVERSE_FLOOR) into compute_rs_rating; restrict numerator to members ∪ REFERENCE_TICKERS so delisted-but-in-retention names can't mask a real shortfall. Make it identical to the watchdog denominator so gate-passes ⇔ rating-coverage-passes by construction. Add the missing union-inflation regression test.


### D2. [🔴 CRITICAL] Universe corruption: finvizfinance LOGO misparse + count-only guards blind to validity (Problem 2 root cause)

**Files:** `ibd_rs/tickers.py`, `ibd_rs/config.py`, `ibd_rs/nasdaq_trader.py`


finvizfinance 1.3.0 prepends a per-row LOGO letter (AAPL→AAAPL); the cached ticker_list is ~4,632 garbage symbols with AAPL/MSFT/NVDA absent. All three guards (UNIVERSE_FLOOR=3000, DROP_GUARD=0.90, COMPLETENESS_RATIO=0.98 at config.py:29-31; tickers.py:121-143) check COUNT only, so the full-count-but-mangled fetch passed and cached as trusted → yfinance 404s ~4,538 fake symbols = the 07-20+ price collapse. No validity/identity check exists and no test feeds a corrupted-but-full-count fetch.


**Fix direction:** Add source-agnostic VALIDITY guards: anchor-set presence (SPY,QQQ,AAPL,MSFT,NVDA,... all present or reject), symbol-shape sanity ^[A-Z][A-Z.-]{0,5}$, day-over-day Jaccard vs last-good >=~0.9. Add anchor-rejection test. This is superseded operationally by demoting Finviz (Slice 4) but the validity guard must apply to whatever source is primary and to the weekly enrichment.


### D3. [🔴 CRITICAL] Watchdog never gates the build on rating coverage — the 3-week outage ran GREEN (Problem 3)

**Files:** `ibd_rs/db.py`, `ibd_rs/cli.py`


classify_latest_trading_day_completeness sets is_complete purely from close_coverage/universe_size (db.py:372-384, reason 'close_coverage_below_threshold'); rating_coverage is computed and printed (db.py:355-356, cli.py:93) but never enters is_complete, and cmd_update raises only on `not universe.trusted or not is_complete` (cli.py:138). During 06-30..07-22 closes stayed ~98% so the build exited 0 while every latest-day rs_rating was NULL. This is a DETECTOR only — shipping it alone just turns runs red with still-no-ratings, so it MUST ship together with the denominator fix. No test isolates high-close/zero-rating.


**Fix direction:** Add a rating-coverage failing condition to classify (distinct reason) using the SAME unified denominator as the fixed gate, OR it into the cli.py:138 exit. Add unit + cmd_update SystemExit(1) tests.


### D4. [🔴 CRITICAL] Destructive full-recompute on the pruned DB: recalc_all/init/NULL-cursor all wipe ~1yr of history; docs runbook actively recommends it (HARD INVARIANT breach)

**Files:** `ibd_rs/rs.py`, `ibd_rs/cli.py`, `.github/workflows/init.yml`, `docs/wiki/Operations.md`, `docs/wiki/CLI-Reference.md`, `docs/wiki/Troubleshooting.md`


calculate_and_store(recalc_all=True) clears rs_raw+rs_rating for every loaded date (rs.py:117-118→db.py:203) then only re-stores dates with >=252 prior in-window closes (rs.py:34). On the live DB get_prices_df exposes only ~269 trading days (prune=now-390 calendar days, db.py:264-266), so only the newest ~17 dates survive — the ~252 mid-band dates' ratings (computed when 2y of closes existed) are cleared and never restored. Unguarded entry points: cmd_recalc (cli.py:146), cmd_init (cli.py:53), init.yml. Worse, incremental update degrades to the SAME full wipe if the rs_raw cursor is ever NULL (rs.py:108 `if last_rs_date:`). And Operations.md:199/203, CLI-Reference.md:152-163, Troubleshooting.md:218-228 tell operators to run `recalc` for exactly this symptom.


**Fix direction:** Hard-guard: refuse recalc_all (and abort on NULL cursor) unless the loaded close window spans >=252+margin trading days for the oldest date being cleared, i.e. never on a 13mo-pruned DB. Gate cmd_recalc/cmd_init/init.yml behind an explicit --force-full + env check. Correct the three doc runbook entries so no human follows them onto the landmine.


### D5. [🔴 CRITICAL] No bounded rating-only backfill primitive; the 15-day self-heal window is shorter than the 16-day outage, so 06-30 is already permanently unrecoverable via the incremental path

**Files:** `ibd_rs/rs.py`, `ibd_rs/db.py`, `ibd_rs/config.py`


The incremental re-rate set is `index>cursor` ∪ trailing RS_RECOMPUTE_WINDOW_DAYS=15 (rs.py:108-116). The cursor is MAX(date) WHERE rs_raw IS NOT NULL (db.py:320-325), which advanced to 07-22 because rs_raw kept computing through the outage, so catch-up is empty and only the last 15 trading days are revisited. The hole is 16 trading days, so 06-30 sits outside the window forever and one more date drops out daily. Recovery requires re-ranking STORED rs_raw without recomputing it — but NO rating-only write primitive exists: clear_rs_for_dates nulls both columns (db.py:203), upsert_rs overwrites both (db.py:182-183), calculate_and_store recomputes rs_raw from closes first.


**Fix direction:** Add db.get_rs_raw_df(conn,start,end) that pivots stored rs_raw, feed it to compute_rs_rating with the corrected denominator, and write ONLY rs_rating via `UPDATE rs SET rs_rating=%s WHERE ticker=%s AND date=%s`. Ranking is denominator-independent so it reproduces the ungated result exactly. Do NOT widen RS_RECOMPUTE_WINDOW_DAYS (see margin defect). Prove rs_raw/close byte-identical + idempotent.


### D6. [🔴 CRITICAL] No CI runs the test suite; publish.yml is release-triggered with no test/build/version gate — how all three bugs reached production

**Files:** `.github/workflows/publish.yml`, `.github/workflows/daily_update.yml`, `.github/workflows/init.yml`


.github/workflows has only daily_update.yml, init.yml, publish.yml; nothing runs pytest. 123 offline tests (~1.9s, all mocked/in-memory SQLite) execute nowhere in CI, and publish.yml:18-28 does `twine upload dist/*` on any GitHub release with zero test step and no tag==version assertion. A PR reintroducing a count-only guard would merge green and ship to PyPI.


**Fix direction:** Add ci.yml (push+pull_request) running requirements.lock + .[dev] pytest on 3.10/3.12; make publish.yml `needs:` a green test job, assert release tag==pyproject version, add `python -m build` smoke. Add a meta-test asserting a workflow invokes pytest.


### D7. [🟠 HIGH] Retention-vs-lookback margin is ~2 trading days, not the ~21 the code comment/guard test claim; clear-then-recompute can destroy history at the knife-edge

**Files:** `ibd_rs/db.py`, `ibd_rs/config.py`, `ibd_rs/rs.py`, `tests/test_rs.py`


VERIFIED: prune cutoff = now - PRICE_RETENTION_MONTHS*30 = 390 CALENDAR days (db.py:264-266) ≈ 269 trading days in get_prices_df; the trailing window's oldest date rs_raw_df.index[-15] needs position>=252 → N>=267 → margin ≈ 269-267 = 2. config.py:50 asserts a '21 trading days' ceiling and test_rs.py:327-331 models retention as 13*21=273, both overstating by ~4-8 and hiding the true margin. In a holiday-dense window (N≈265) the margin goes negative and an ordinary `update` clears then fails to recompute rs_raw for the oldest trailing dates, permanently dropping those (ticker,date) rows.


**Fix direction:** Express retention in trading days with explicit margin >= RS_RECOMPUTE_WINDOW+252+buffer (widen to ~15 months) and rewrite the guard test to model calendar→trading-day (390*252/365). Independently, never clear a date unless the same unit of work rewrites it.


### D8. [🟠 HIGH] Prune amplification: any accidentally-nulled historical rating becomes a permanent row DELETE on the next daily update (a ~390-day recovery deadline)

**Files:** `ibd_rs/db.py`, `ibd_rs/cli.py`


prune_old_close DELETEs rows WHERE date<cutoff AND rs_rating IS NULL (db.py:269-272) and runs every day in cmd_update (cli.py:128). So if the denominator/recompute bugs null a historical rating, the very next scheduled update converts 'nulled' into 'deleted' for anything older than the ~13mo cutoff — the row and its rs_raw are gone, no longer re-rankable. It also means an outage's dates, if they ever age past cutoff, get their rs_raw+close DELETED, capping how long recovery stays possible.


**Fix direction:** Change the DELETE predicate to `rs_raw IS NULL AND rs_rating IS NULL` so an outage's rs_raw survives past cutoff and stays recoverable. Keep the daily cron disabled during recovery so no prune runs between a mistake and its discovery.


### D9. [🟠 HIGH] DECIDED-DIRECTION defect: market cap is not reconstructible from OHLCV — only a liquidity (dollar-volume) proxy is, a different axis that adds a second comparability seam and (unmanaged) makes the rank population thrash

**Files:** `ibd_rs/prices.py`, `ibd_rs/config.py`, `ibd_rs/db.py`, `ibd_rs/rs.py`


The retired filter is Market Cap>$50M (config.py:24); reconstructing it needs shares-outstanding, which the bulk yf.download OHLCV frame (prices.py:43-49) does not carry — the only source is per-ticker .info (~5,000 slow, frequently-blocked calls: the exact rate-limit/fake-symbol failure mode being escaped). Dollar-volume is LIQUIDITY, not cap (high-cap/low-float diverge), so constituents differ materially. RS Rating is a cross-sectional rank (rs.py:71), so any name flipping in/out perturbs every rating; a hard threshold + short window (TRAILING_WINDOW_DAYS=10, no volume column, db.py:44-65) makes membership thrash daily.


**Fix direction:** Explicitly REDEFINE the going-forward universe as a liquidity filter (median dollar-volume over ~50 sessions), with hysteresis (admit>T_high, drop<T_low) and WEEKLY membership recompute, calibrated empirically to ~4,600. Document it as a second versioned seam distinct from the market-cap era. Do NOT attempt per-ticker shares-outstanding.


### D10. [🟠 HIGH] nasdaq_trader parser as-implemented is unfit to be primary: keeps ~1,965 non-equity junk; must classify by Security Name, and symbol-shape/5th-letter filtering deletes real constituents

**Files:** `ibd_rs/nasdaq_trader.py`


_parse_directory (nasdaq_trader.py:18-42) filters only ETF and Test-Issue flags and DISCARDS Security Name — against live files it yields 7,469 symbols including 913 preferred/depositary, 474 warrant, 427 unit, 132 right, 17 ETN, plus ~212 CEF/ETN not flagged ETF=Y. Promoting it as-is re-creates the failed_tickers flood and corrupts the percentile population with preferreds/units. Separately, a symbol-shape/5th-letter rule (as the decided direction's wording implies) would wrongly delete 249 real Common names whose 4-char root ends W/U/R (ACIW,AEHR,ACIU) and 37 class-share names (CMCSA,BELFA,DGICA) — a 4-letter root ending W/U/R is indistinguishable by shape from root+suffix.


**Fix direction:** Rewrite _parse_directory to return Security Name and classify by it: keep common/ordinary/class-share; reject warrant|unit|right|preferred|depositary|when.?issued|notes|due 20|etn|fund|trust. ETF/Test flags first cut, name-type decisive; symbol shape only corroboration. Yields ~5,274 sane base. Add name-classification fixture test.


### D11. [🟠 HIGH] yfinance symbol-dialect mismatch: Nasdaq Trader '.'/'$' vs yfinance/Finviz/DB '-'; no normalization exists

**Files:** `ibd_rs/nasdaq_trader.py`, `ibd_rs/prices.py`, `ibd_rs/db.py`


Nasdaq Trader emits BRK.B / ABR$D; yfinance/Yahoo canonical is BRK-B and the existing DB rows use '-'. No normalization exists (db.py:388 only strips whitespace; prices.py has none), so a kept share-class name is sent verbatim to yf.download (prices.py:43) and returns nothing (counts as missing coverage), AND the DB would key BRK.B separately from the historical BRK-B rows, splitting one company across two keys and orphaning ~1yr of history.


**Fix direction:** Add one canonicalization step ('.'→'-', drop '$' preferreds) in the universe module, canonical form '-'. Confirm what the live DB actually stores for BRK/BF before choosing, to avoid orphaning history under a second key.


### D12. [🟠 HIGH] Demoting Finviz silently guts all six sector/industry client methods unless a separate corruption-resistant weekly enrichment repopulates the tickers table

**Files:** `ibd_rs/tickers.py`, `rs_rating/client.py`, `.github/workflows/daily_update.yml`


upsert_tickers (tickers.py:197) — the only writer of sector/industry — runs solely inside the trusted-Finviz branch of fetch_ticker_list. Under the re-architecture that fetch is off the daily path, so the tickers table goes stale/empty and client.sectors/industries/sector_ranking/industry_ranking/sector_top/industry_top (client.py:311-513) all silently return empty. And the LOGO bug still lives in finvizfinance 1.3.0, so a naive weekly enrichment would upsert junk rows (AAAPL) and lose the real AAPL→sector mapping.


**Fix direction:** Add a separate CLI subcommand + weekly workflow that ONLY upserts sector/industry, never gates the daily run, and is corruption-resistant (intersect fetched symbols with the known daily universe / anchor+Jaccard, or pin/patch finvizfinance). Keep fail-graceful; make client sector methods degrade gracefully; add graceful-degradation tests.


### D13. [🟠 HIGH] Split detection is structurally blind in production: 7-day scan window < 10-day auto-adjusted re-download window, so the split seam is always out of reach

**Files:** `ibd_rs/splits.py`, `ibd_rs/config.py`, `ibd_rs/prices.py`


detect_anomalous_changes scans [MAX(date)-SPLIT_LOOKBACK_DAYS=7, MAX(date)] (splits.py:18-28, config.py:55) but download_update re-downloads a TRAILING_WINDOW_DAYS=10 window with auto_adjust=True (prices.py:45, config.py:41), and auto_adjust back-adjusts the whole requested range consistently. So on split day the recent [T-10..T] is internally smooth and the only >threshold discontinuity is the seam at ~T-10/T-11 — always older than T-7, permanently outside the scan. The detector fires only on genuine non-split >40% moves (false positives). tests/test_splits.py only feeds RAW unadjusted data, masking the defect; the wiki even acknowledges the failure mode.


**Fix direction:** Make scan window strictly exceed the re-download window (>=15d), or compare each re-downloaded overlapping date against the stored value before the upsert overwrites it. Keep yf.Ticker(t).splits as the confirmation gate. Add the auto-adjust-seam regression test.


### D14. [🟠 HIGH] Corrupted ticker_list cache is live-read for up to 7 days, and purging it before the universe fix ships re-corrupts on the next fetch

**Files:** `ibd_rs/tickers.py`, `ibd_rs/db.py`, `ibd_rs/config.py`


fetch_ticker_list returns the cached ticker_list without fetching when <CACHE_DAYS=7 old (tickers.py:178-186, config.py:28); cmd_update calls it force_refresh=False (cli.py:104); db._normalize_universe_tickers also falls back to the cached list for the completeness denominator (db.py:388-393). The garbage was cached ~07-20 with a fresh date, so the next several updates short-circuit to it. The documented purge (DELETE meta ticker_list/date) forces a refetch — but through the still-broken finvizfinance it re-corrupts.


**Fix direction:** Ordering constraint: (1) merge the universe re-architecture so refetch can't re-corrupt; (2) THEN purge ticker_list + ticker_list_date; (3) run the pipeline to re-cache a clean universe. Add read-time validity check on the cache.


### D15. [🟠 HIGH] Public-read RLS/GRANT/policy layer is entirely un-versioned and un-tested — a volume column's client impact is unknowable and a rebuild-from-repo would 403 every caller

**Files:** `ibd_rs/db.py`, `rs_rating/client.py`, `scripts/poc_neon_data_api_anon_read.py`


SCHEMA_SQL_PG (db.py:44-65, run by init_db) creates rs/tickers/meta with NO RLS/POLICY/GRANT; the only such DDL in the repo is a throwaway PoC (scripts/poc_neon_data_api_anon_read.py:48-55). client.py:38 depends on RLS+anonymous grants that live only in Neon. Whether adding volume/dollar_volume is client-visible depends on whether the live grant is table- or column-level — undeterminable from the repo — and no test hits the real Data API.


**Fix direction:** Bring RLS/policy/GRANT DDL into a versioned migration + init_db (prefer table-level GRANT SELECT so additive columns stay covered). Add a secrets-gated Neon-branch E2E hitting the real Data API with an anonymous token. This directly informs the persist-Volume decision.


### D16. [🟠 HIGH] Recovery sequencing and cron freeze are required, or a scheduled run races/undoes the manual backfill

**Files:** `ibd_rs/rs.py`, `ibd_rs/cli.py`, `.github/workflows/daily_update.yml`


The daily workflow runs on cron '0 21 * * 1-5' plus workflow_dispatch (daily_update.yml). A scheduled update firing mid-recovery would run download+recompute(with the broken gate)+prune concurrently with the manual backfill (double-clear, prune between a null and its fix, cursor churn). The re-rank must use the FIXED gate; the 07-20/21/22 re-download needs the FIXED universe.


**Fix direction:** Ship order: universe re-arch + gate fix + watchdog fix + destructive-path guards + backfill primitive (merged, green) → disable cron → snapshot (Neon branch + pg_dump + baseline checksums) → purge corrupt meta → pre-flight asserts → rating-only re-rate 06-30..07-19 → surgical re-download+rate 07-20/21/22 under new universe → full verification → re-enable cron.


### D17. [🟡 MEDIUM] Three divergent 'latest date' definitions (close / rs_raw / rs_rating); the public client silently served ~3-week-stale ratings with no staleness signal

**Files:** `ibd_rs/db.py`, `rs_rating/client.py`, `ibd_rs/cli.py`


CLI get_latest_rs_date keys off rs_raw (db.py:322) so during the outage cmd_top/cmd_export (cli.py:153,224) anchored on 07-22 and printed empty/NULL-rating rows; the public client._latest_date keys off rs_rating (client.py:519-527) so every method silently served ~2026-06-27 data with no staleness warning; completeness keys off close (db.py:314). Three definitions of 'latest' across the codebase.


**Fix direction:** Make cmd_top/cmd_export use a rating-based latest date consistent with the client; add a client staleness signal (warn when latest rated date lags latest close date). Document latest-price / latest-raw / latest-rated as three distinct intentional concepts.


### D18. [🟡 MEDIUM] Single-table NULL-punning is ambiguous, and clear-then-recompute is non-atomic with no reconnect around long ops → committed rating holes on mid-run failure

**Files:** `ibd_rs/db.py`, `ibd_rs/rs.py`, `ibd_rs/prices.py`


NULL means never-had / pruned / not-computed / gate-failed / cleared / too-young across the same columns, and queries conflate them (prune, export filtering rs_raw not rs_rating at db.py:535, delete_ticker_prices leaving stale ratings at db.py:253-259). Separately, clear_rs_for_dates commits the NULLs (db.py:207) before upsert_rs commits per batch (db.py:186) with no spanning transaction and no rollback anywhere; reconnect() runs only after the Finviz fetch (tickers.py:190), not around the 20-30min download or the RS store, so a Neon auto-suspend mid-run leaves committed holes / marks all remaining batches failed.


**Fix direction:** Clear a date only in the same transaction that rewrites it (or wrap clear+recompute per range in one transaction). Add reconnect()/liveness retry around the long download and before the RS store, returning the possibly-new conn as fetch_ticker_list does. Document per-column NULL semantics in one place.


### D19. [🟡 MEDIUM] Per-ticker latest-session staleness is undetected, and failed_tickers meta goes stale (never cleared on a clean run)

**Files:** `ibd_rs/prices.py`


_tickers_with_close_data (prices.py:86-100) treats a ticker as present if it has ANY non-NaN close anywhere in the 10-day window, so a halted/delisted ticker missing only the most recent 1-2 sessions is silently accepted and its stale close feeds ROC as if current. And failed_tickers meta is written only inside `if all_failed:` (prices.py:170-172,210-211), never cleared, so a stale ~4,538-entry list persists across later healthy runs, misleading observability.


**Fix direction:** Flag a ticker degraded when its newest close is older than the frame max by > tolerance and surface it in failed_tickers. Always write failed_tickers (empty when none) or clear on success.


### D20. [🟡 MEDIUM] Split-repair re-downloads 2y and upserts wholesale, fighting 13-month retention and rewriting frozen historical closes

**Files:** `ibd_rs/splits.py`, `ibd_rs/db.py`


verify_and_repair downloads INITIAL_PERIOD='2y' (splits.py:75) and upserts every close via the overwrite path (splits.py:84-89→db.py:148-151). On the pruned DB this inserts ~11mo of pre-retention rows the next prune deletes, overwrites closes that already fed FROZEN historical ratings (stored close no longer matches the rating computed from it), and can re-populate closes prune had NULLed. Also serial per-ticker yf calls with no backoff/jitter/cap (splits.py:60-94, exceptions swallowed) and no future-date clamp on the MAX(date) anchor (splits.py:18-28).


**Fix direction:** Bound the repair re-download to the retention window (start=cutoff, not '2y'). Route repair downloads through the shared batched/backoff wrapper with a per-run cap. Clamp date anchors to <=today and reject future-dated write rows.


### D21. [🟡 MEDIUM] Universe corruption also polluted the tickers table and may have written spurious rs rows for coincidentally-real garbage symbols

**Files:** `ibd_rs/tickers.py`, `rs_rating/client.py`


The corrupt fetch was upserted via upsert_tickers (tickers.py:197), so the tickers table now carries AAAPL/AABBV/AAA with sector/industry (inflating client.sectors()/industries() lists), and garbage-but-real symbols (AA, AAA) may have downloaded closes + rs_raw on 07-20+, creating spurious percentile members. Real rows weren't deleted (upsert only), so real sector data is stale-but-present.


**Fix direction:** During recovery, reconcile: DELETE tickers-table rows not in the corrected universe and null/delete any rs rows for fake symbols on 07-20+. Recovery is not complete until the tickers table is clean, since it is the only feed for sector/industry client APIs.


### D22. [🟡 MEDIUM] No single authoritative per-day ranking population; the Nasdaq-Trader superset exceeds Yahoo coverage so the denominator must be the yfinance-resolvable set

**Files:** `ibd_rs/rs.py`, `ibd_rs/cli.py`, `ibd_rs/db.py`, `ibd_rs/prices.py`


RS gate uses len(price_df.columns), completeness uses the fetched ticker_list (cli.py:132), and the new liquidity filter adds a third set — membership derived in three places. The name-clean Nasdaq-Trader base (~5,274) also exceeds the ~4,600 documented count and many names have no Yahoo coverage; if the denominator is raw directory membership, no-coverage names permanently hold coverage under the 90% gate (db.py:444-451), and the full superset (~70 batches) must be downloaded before liquidity can filter it.


**Fix direction:** Have the universe module emit ONE explicit per-day population (name-clean, resolvable, liquidity-passing) and feed it to both compute_rs_rating's denominator and check_latest_trading_day_completeness. Define the denominator as the yfinance-resolvable set so unresolvable names fall out rather than counting as missing.


### D23. [🟡 MEDIUM] Package version drift and brittle single-package version test; no publish tag==version gate

**Files:** `ibd_rs/__init__.py`, `rs_rating/__init__.py`, `pyproject.toml`, `tests/test_client.py`


ibd_rs/__init__.py=0.3.0 while pyproject.toml and rs_rating/__init__.py=0.4.0; test_client.py:415-417 hardcodes only rs_rating=='0.4.0', ignoring ibd_rs drift and any pyproject mismatch, and publish.yml never asserts tag==version. A release could ship an engine reporting 0.3.0.


**Fix direction:** One source of truth for version; replace the test with one that parses pyproject and asserts both packages match; add a publish-time tag==version guard (folds into Slice 0).


### D24. [🟡 MEDIUM] Client tests are 100% mocked — no real PostgREST/RLS validation

**Files:** `rs_rating/client.py`, `tests/test_client.py`


Every test in test_client.py @patches urllib.request.urlopen, so the PostgREST params the client builds (rs_rating:'not.is.null', in.(...), and:(date.gte,date.lte), order nullslast; client.py:129-224,529-543) are never sent to a real endpoint. An operator rename or RLS tightening stays green in tests while every real user gets 400/403.


**Fix direction:** Add an opt-in, secrets-gated Neon-branch E2E that runs each public method against the real Data API; keep it OFF the per-PR path (offline SQLite suite stays the PR gate), run on release + schema/client changes.


### D25. [⚪ LOW] Low-severity cluster: percentile small-n artifacts, discarded full recompute, broadened-init timeout, CEF/ETN seam

**Files:** `ibd_rs/rs.py`, `ibd_rs/nasdaq_trader.py`, `.github/workflows/init.yml`


(a) rs.py:71-72 1-99 mapping has small-n/tie/banker's-rounding artifacts, safe ONLY because the gate guarantees large n — document, keep gate as guarantee. (b) calculate_and_store recomputes rs_raw for ~4,900×~269 every incremental run then discards all but 15 dates (rs.py:99-115) — pure perf. (c) The broadened Nasdaq-Trader superset could push the 2y init path past daily_update.yml's 30-min cap. (d) CEF/ETN are not ETF=Y flagged (~212 rows) so only a Security-Name filter removes them — a documented comparability seam vs Finviz's EXCLUDED_INDUSTRIES which kept many CEFs.


**Fix direction:** Treat as non-blocking: document the mapping and the seam, add contract tests pinning degenerate outputs, optimize recompute only if runtime bites, pre-filter/split init if the enlarged count risks timeout.



---

## Risky decisions flagged (re-examine)

1. 'Reconstruct the market-cap filter from yfinance' is NOT achievable: bulk yf.download OHLCV (prices.py:43-49) carries no shares-outstanding, and per-ticker .info is the exact ~5,000-call rate-limit/fake-symbol failure mode this refactor escapes. Only a LIQUIDITY (dollar-volume) proxy is feasible — a different axis (high-cap/low-float names diverge), so it introduces a SECOND comparability seam on top of the accepted Finviz→Nasdaq one. Redefine and document it as a liquidity filter with an empirically-calibrated threshold, not a market-cap reconstruction.

2. 'Filter by symbol conventions (suffixes / 5th-letter share-class codes)' as a PRIMARY discriminator is unsafe: on live files it would delete 249 real Common names whose 4-char root ends W/U/R (ACIW, AEHR, ACIU) and 37 class-share names (CMCSA, BELFA, DGICA), because a 4-letter root ending W/U/R is indistinguishable by shape from root+suffix. Security Name must be primary; symbol shape only corroboration.

3. Nasdaq Trader is named as the ready fallback, but the current parser (nasdaq_trader.py:18-42) filters only ETF/Test flags and keeps ~1,965 non-equity junk (preferreds/warrants/units/rights/CEFs/ETNs) plus symbols with '.'/'$'. Promoting it AS-IS re-creates Problem 2's failed_tickers flood and corrupts the RS percentile population — it needs the Security-Name rewrite + symbol canonicalization BEFORE it can be primary.

4. The symbol-dialect mismatch (Nasdaq '.'/'$' vs yfinance/DB '-') is unaddressed in the decided plan; without a canonicalization layer the switch silently drops kept share-class constituents (BRK.B returns nothing) AND splits ~1 year of history across two ticker keys. Must be resolved as part of the cutover, and the live DB's existing key form confirmed first.

5. The natural recovery lever — 'widen the trailing self-heal window' — is a landmine. VERIFIED margin is ~2 trading days, not the 21 the code comment (config.py:50) and guard test (test_rs.py:327-331) claim: prune keeps now-390 calendar days ≈ 269 trading days, and index[-15] needs position>=252, so any window > ~17 pushes recompute below the lookback cliff and destroys ~1 year of history. Recovery MUST be a rating-only re-rank of stored rs_raw, never a widened recompute.

6. 'Backfill the hole' cannot be done with current commands: recalc_all/init on the pruned DB destroy ~1 year of history (and the docs runbook actively recommends `recalc` for exactly this symptom), and NO rating-only write primitive exists yet. Recovery is blocked on new code (get_rs_raw_df + rating-only UPDATE) plus a cron freeze and snapshot — it is not a runbook-only operation.

7. Treating sector/industry as merely 'optional weekly Finviz enrichment' understates the coupling and the source risk: demoting Finviz silently empties all six client sector/industry methods (client.py:311-513), AND the enrichment source (finvizfinance 1.3.0) is STILL the corrupted LOGO-misparse library, so a naive weekly upsert would re-poison the tickers table. The weekly job must itself carry the anchor/Jaccard validity guard, or finvizfinance must be pinned/patched.


---

## Recommended slice ordering (raw, from audit)

1. SLICE 0 — CI safety net (independent, ship first): add ci.yml running pytest on push+PR (3.10/3.12), gate publish.yml on a green test job + assert release tag==pyproject version + `python -m build` smoke, and fix the ibd_rs 0.3.0/0.4.0 version drift with one source of truth. Rationale: every slice below needs a green gate that can't be silently removed; cheap, no dependency on the rest.

2. SLICE 1 — Unified stationary gate + watchdog rating gate (co-designed, MUST ship together): change compute_rs_rating to take the active-universe SET floored at UNIVERSE_FLOOR (numerator restricted to members ∪ REFERENCE_TICKERS), thread it from cli/rs, and add a rating-coverage failing condition to classify_latest_trading_day_completeness using the SAME denominator, OR'd into cli.py:138. Add BUG1 (union-inflation) + BUG3 (high-close/zero-rating) regression tests. Rationale: this is the forward-fix for Problems 1+3; the watchdog is only a detector so it cannot ship before or without the denominator fix (would just go red with no ratings).

3. SLICE 2 — Destructive-path guards + bounded rating-only backfill primitive (recovery tooling, depends on Slice 1's denominator): (a) hard-guard recalc_all/cmd_recalc/cmd_init/init.yml behind --force-full + a >=252+margin-closes precondition, treat a NULL rs cursor as a fatal abort not a full recompute, and correct the Operations/CLI/Troubleshooting runbook entries; (b) add db.get_rs_raw_df + a rating-only UPDATE backfill (`recalc --from --to`) that re-ranks STORED rs_raw under the corrected gate and never touches rs_raw/close/clear_rs_for_dates. Prove rs_raw byte-identical + idempotent. Rationale: protects the HARD INVARIANT before any human can start recovery, and provides the ONLY safe recovery mechanism.

4. SLICE 3 — Retention/prune safety (protects recovery from amplification and the ~2-day knife-edge; ship before running recovery): change prune's DELETE predicate to `rs_raw IS NULL AND rs_rating IS NULL` so outage rs_raw survives past cutoff, express PRICE_RETENTION_MONTHS as trading days with explicit margin >= WINDOW+252+buffer (widen ~15 months), and rewrite test_recompute_window_leaves_enough_lookback_margin to model the calendar→trading-day conversion (fixing the false 21-day-ceiling claim). Rationale: without this a single nulled rating becomes a permanent DELETE on the next daily run, and holiday-dense windows can destroy marginal-history rs_raw on ordinary updates.

5. SLICE 4 — Universe re-architecture (code lands; membership switch is what creates the go-forward seam): (a) rewrite nasdaq_trader._parse_directory to classify by Security Name (reject warrant/unit/right/preferred/depositary/CEF/ETN); (b) symbol canonicalization ('.'→'-', drop '$') after confirming the live DB's BRK/BF key form; (c) source-agnostic validity guards (anchor-set, shape, day-over-day Jaccard); (d) liquidity filter redefined explicitly (in-memory dollar-volume, hysteresis, ~50-session window, WEEKLY membership recompute) computed from a widened download that pulls Volume; (e) emit ONE authoritative per-day resolvable population fed to both the RS denominator and completeness. Tests: anchor rejection, name classification, normalization round-trip, liquidity hysteresis. Rationale: replaces Finviz on the critical path and kills the corruption class; the validity guard from Slice-1/2 world applies here.

6. SLICE 5 — Sector/industry decoupling: separate CLI subcommand + weekly workflow that ONLY upserts sector/industry, corruption-resistant (intersect with known universe / anchor+Jaccard, or pin finvizfinance), fail-graceful; make client sector/industry methods degrade gracefully on partial coverage; add tests. Rationale: demoting Finviz (Slice 4) otherwise silently empties all six client sector methods.

7. SLICE 6 — Split-detection & repair correctness: make the scan window strictly exceed the 10-day re-download window (>=15d) or compare-before-overwrite at write time, keep yf.splits as the confirmation gate, bound verify_and_repair to the retention window (not '2y'), route repair through the shared backoff wrapper with a per-run cap, clamp date anchors to <=today. Add the auto-adjust-seam regression test. Rationale: independent correctness; can land in parallel with 4/5.

8. SLICE 7 — Latest-date semantics, staleness, per-ticker gaps: one canonical rating-based latest date for cmd_top/cmd_export, a client staleness signal, per-ticker latest-session staleness detection, and always-written failed_tickers. Rationale: consumer-facing hardening that prevents silent-stale recurrence after the gate fix.

9. SLICE 8 — Durability & contract: document per-column NULL semantics, make clear+recompute atomic (clear-only-what-you-rewrite / spanning transaction), add reconnect around long download + RS store, bring RLS/policy/GRANT DDL into versioned init_db (prefer table-level GRANT), add the secrets-gated Neon-branch E2E for the client. Rationale: closes the durability + public-contract gaps; the RLS work must precede any decision to persist a Volume column.

10. RECOVERY RUN (operational, after Slices 0-4 minimum are merged & green): R1 freeze the daily cron; R2 snapshot (Neon copy-on-write branch + pg_dump rs/tickers/meta + baseline md5 checksums for date<=2026-06-29 and rs_raw fingerprints for 06-30..07-17); R3 purge corrupt ticker_list + ticker_list_date meta (only now that refetch can't re-corrupt); R4 pre-flight asserts (rs_raw cursor still 07-22; anchors present in new universe); R5 rating-only re-rank 06-30..07-19 from stored rs_raw under the corrected gate (old-universe regime); R6 surgical re-download + rate 07-20/21/22 under the NEW universe (past the warm-up cliff) and reconcile garbage tickers-table/rs rows; R7 verify checksums for <=06-29 and rs_raw fingerprints for the hole are byte-identical, all hole dates have rating coverage>0 in [1,99], re-run is a no-op; R8 re-enable cron. Rationale: the re-rate depends only on the gate fix, the re-download depends only on the universe fix, and both depend on the cron freeze + snapshot; the seam lands at the 07-18→07-20 weekend by construction.


---

## Open questions raised by audit (all now resolved — see master plan Decision Log)

Q1. Unified-gate denominator source of truth: use the active-universe SET (consistent with the watchdog, but depends on a clean fetch) or a self-referential trailing-median of the daily rated-count (immune to universe corruption/churn but lags legitimate step-changes) — or set-as-primary with the rolling median as a sanity backstop? And should compute_rs_rating's numerator be restricted to universe members (recommended; changes its signature from int to a set)?

Q2. Going-forward universe definition: confirm we REDEFINE it as a liquidity filter (dollar-volume) rather than market cap — since market cap is not reconstructible from OHLCV at $0 — and accept a second, versioned comparability seam. Do you want me to calibrate the threshold / averaging window / hysteresis bands empirically against real downloaded volume (showing resulting population size + day-to-day stability) before we commit any number?

Q3. Persist Volume in the rs table (schema + RLS + client-contract + Neon-branch E2E work) or compute dollar-volume transiently in-memory each run (zero schema/client/RLS impact)? This is coupled to whether the production Neon Data API GRANT is table-level (additive columns auto-covered) or column-level (new column invisible/403), and to whether the RLS/GRANT DDL should be brought into versioned init_db. Recommendation: transient in-memory + version the RLS DDL regardless.

Q4. Canonical symbol form: does the live DB currently store 'BRK-B' or 'BRK.B'? The new source emits 'BRK.B'; we must pick one canonical form ('-' recommended) and confirm we won't orphan ~1 year of history under a second key. (I can query the DB to settle this if given read access.)

Q5. recalc/init/docs hardening: OK to (a) gate cmd_recalc/cmd_init/init.yml behind an explicit --force-full + a >=252+margin-closes precondition that hard-refuses on the pruned live DB, and (b) correct the three runbook doc entries (Operations/CLI-Reference/Troubleshooting) now — so no operator follows the current guidance onto the ~1-year-wipe landmine?

Q6. Watchdog policy: confirm the daily build should FAIL (exit non-zero) on latest-day RATING coverage below floor, once the gate and watchdog share a denominator. This turns a silent outage red on day one but means the run cannot be green until ratings are actually restored.

Q7. Backfill scope, rs_raw immutability, and seam: confirm the rating-only re-rank covers 06-30..07-19 (old-universe stored rs_raw) and EXCLUDES 07-20..07-22 until prices are repaired, with 07-20/21/22 rated under the NEW universe (single documented seam at the 07-18→07-20 weekend). Must rs_raw for 07-01..07-17 stay byte-frozen (requires a custom 3-date rate for 07-20/21/22), or is a benign same-value re-derivation by a stock update acceptable?

Q8. Cleanup + semver: should recovery DELETE tickers-table rows and any rs rows for garbage symbols (AAAPL/AABBV plus coincidentally-real AA/AAA on 07-20+)? And do you treat the universe re-architecture as a 0.5.0 minor (client API surface unchanged) with a universe-definition version surfaced to consumers, or as a breaking change for downstream dataset consumers?
