# Research Appendix — Evidence for the Neon Migration & Reliability Recovery Plan

**Date: 2026-07-11** · Master plan: [`2026-07-11-neon-migration-and-reliability.md`](./2026-07-11-neon-migration-and-reliability.md)

This document preserves the external facts and citations that back the decisions in the master plan. It summarizes the output of six parallel research agents (four investigators plus two adversarial verifiers). Use it in the implementation session to trace *why* each decision was made.

---

## A. Neon Data API — anonymous public read (the read-path linchpin)

**Conclusion: likely supported (per docs), but Beta + internally contradictory docs → an empirical PoC is mandatory before committing.**

- Neon's **Data API** is a "PostgREST-compatible HTTP query interface" — the direct analog of Supabase's auto-generated PostgREST API. (Source: `neon.com/docs/data-api/overview.md`)
- **Status: Beta.** Verbatim: "The Neon Data API is in Beta." (Source: `data-api/overview.md`)
- **Header-less anonymous read (the make-or-break fact):** the `db_anon_role` setting (default `anonymous`) is documented verbatim as "Specifies the database role used for unauthenticated requests (requests sent without an Authorization header). To allow public access to specific data, configure this role in your database using SQL GRANT statements." This means a plain GET with no token runs as that role. (Source: `data-api/manage.md`)
- **Recipe:** enable RLS + `GRANT SELECT` on the table to the anonymous role + a permissive SELECT policy. (Source: `data-api/access-control.md`)
- **Endpoint shape:** `https://<ep>.apirest.<region>.aws.neon.tech/<db>/rest/v1/rs?...` — query strings and filters are drop-in compatible with Supabase's `/rest/v1/rs?...`; only the base host changes and the apikey header is dropped. (Source: `data-api/get-started.md`)
- **Pricing:** "a Data API for querying over HTTP" is listed under "All plans include," so it is available on every plan including Free. (Source: `neon.com/pricing`)

**Documentation contradiction (the risk):** `access-control.md` and `get-started.md` frame anonymous access as JWT-based (a Neon Auth guest token minted per-request via `GET /token/anonymous`) and show no header-less example, while `manage.md` documents the token-free `db_anon_role` path. The two pages describe two different notions of "anonymous," so the docs alone cannot confirm the token-free path works.

**Two adversarial verifiers:** one returned SUPPORTED ("a config-field reference documents real server behavior"), one returned UNCERTAIN ("the two authoritative pages contradict each other and no working header-less example exists"). Their shared conclusion is: prove it empirically with a PoC.

**Biggest risk:** binding thousands of `pip install` users to a Beta HTTP contract — a breaking change would strand every installed client, whereas Supabase's PostgREST is GA. The master plan therefore keeps the write pipeline decoupled from the read layer and gates the read cutover on a PoC.

**Sources:** `neon.com/docs/llms.txt`, `data-api/overview.md`, `data-api/access-control.md`, `data-api/get-started.md`, `data-api/manage.md`, `neon.com/pricing`

---

## B. Supabase → Neon migration mechanics

**Conclusion: `pg_dump -Fc` → `pg_restore --no-owner --no-acl` over direct (unpooled) connections with `sslmode=require`. At 1.25M rows this is a one-shot job of a few minutes; logical replication and dual-write are unnecessary.**

- **Path:** Neon's official Supabase guide recommends pg_dump/pg_restore for a full offline copy; logical replication is only for "near-zero downtime" and is overkill here. (Source: `neon.com/docs/import/migrate-from-supabase.md`)
- **Export:** `pg_dump -Fc -v -d '<supabase-conn>' --schema=public -f rs_dump.bak` — restrict to the `public` schema so Supabase's auth/storage/realtime schemas are not dragged along.
- **Restore:** first `CREATE DATABASE` on Neon (Neon does not support `pg_restore -C` or `pg_dumpall`), then `pg_restore -v --no-owner --no-acl -d '<neon-conn>' rs_dump.bak`. `--no-owner --no-acl` are mandatory because Supabase ties ownership/ACLs to its auth system.
- **Use direct (unpooled) connections for dump and restore.** PgBouncer transaction pooling drops the SET statements pg_dump relies on. On Neon that is the endpoint host without the `-pooler` suffix; on the Supabase side from an IPv4 host, use Supavisor session mode (port 5432 pooler host, not 6543).
- **SSL is mandatory on Neon:** append `?sslmode=require`. The psycopg2 `ON CONFLICT` upsert path works unchanged (execute_values builds one client-side multi-row INSERT with no prepared-statement conflict). Point the daily writer at the direct endpoint.
- **Size/time:** 1.25M rows compress to ~150-250MB; a single-threaded pg_restore finishes in ~1-3 minutes (index rebuild dominates). The restore naturally compacts the bloat.
- **Neon Free = 0.5GB = the same 500MB as Supabase** (free→free is not a capacity gain). The user is on the paid **Launch** plan, which removes the capacity problem. Scale-to-zero is harmless for a once-daily batch (cold start is a few hundred ms).
- **No sequence reset needed** (the schema has no serial/identity column; the `rs` PK is composite `(ticker, date)`).

**Sources:** `migrate-from-supabase.md`, `migrate-from-postgres.md`, `connect/connection-pooling.md`, `connect/connect-securely.md`, `connect/choose-connection.md`, `introduction/scale-to-zero.md`, `introduction/plans.md`, `supabase.com/docs/guides/database/connecting-to-postgres`

---

## C. Universe source robustness

**Conclusion: the fix is not "find a better source" but "wrap the Finviz screener in a sanity floor + no-cache-on-failure + retry, and add Nasdaq Trader as an independent size anchor and hard fallback." The 55-ticker incident had two independent defects — a truncated fetch was accepted with no completeness check, and a 30-day cache then froze it.**

- `finviz` (mariostoev) v2.0.0 has no rate-limit/throttle handling; a truncation issue (#86) was closed unresolved; it paginates ~248 pages, squarely in the risky regime. Cloud/datacenter IPs (GitHub Actions) are at elevated risk of being blocked by Finviz — the 55-ticker result matches a fetch cut off ~3 pages in.
- `finvizfinance` (lit26) v1.3.0 is better maintained and adds a request timeout and a proxy hook, but it scrapes the same Finviz HTML, so it inherits the blocking risk. The migration buys maintenance health, a timeout, and a proxy hook — not immunity.
- **Nasdaq Trader symbol directory is the authoritative, scrape-resistant backbone.** `nasdaqlisted.txt` + `otherlisted.txt` are HTTPS, pipe-delimited, with no anti-scraping. Together they list ~11k securities (~6,000-7,000 common stocks after excluding ETFs and test issues) across all cap tiers, but they carry **no sector, no industry, and no market cap**, so they cannot reproduce the cap filter — they serve as a size anchor and hard fallback only.
- SEC `company_tickers.json` has only cik/ticker/title (no sector/industry/cap/ETF flag), requires a declared User-Agent, and is limited to 10 req/s — usable only as an optional cross-check.

**Recommended design:**
- **Primary:** keep a Finviz screener (the only free source returning cap + sector + industry in one call), migrate `mariostoev/finviz` → `finvizfinance`, and harden the call with (a) a completeness guard (require fetched ≥ 0.98 × Finviz's own reported total to catch mid-pagination truncation) and (b) exponential backoff with jitter (3-5 attempts, fresh session and realistic User-Agent each try).
- **Anchor + fallback:** add Nasdaq Trader in two roles — an independent size anchor (reject if Finviz returns far below the anchor band), and a hard fallback (on total Finviz failure serve the last-good cached list; only if no cache exists at all, derive the universe from Nasdaq Trader common stocks with sector/industry null).
- **Sanity floor (expected ~4,600):** reject when `fetched < 3,000` (~65% of expected; tighten toward 3,500-4,000 if false positives stay near zero) OR when `fetched < 0.90 × last_good_count`.
- **On failure:** do NOT write `ticker_list` and do NOT advance `ticker_list_date` (this alone prevents the 30-day freeze), retry with backoff, keep serving last-good, but exit non-zero so CI goes red instead of green-but-frozen.
- **`CACHE_DAYS` 30 → 7**, track `last_successful_fetch` separately, and hard-fail if cache age exceeds ~10-14 days while fetches keep failing.

**Sources:** `github.com/mariostoev/finviz` (issues #86, #113), `pypi.org/project/finvizfinance`, `github.com/lit26/finvizfinance`, `nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt` · `otherlisted.txt` · `trader.aspx?id=symboldirdefs`, `sec.gov/files/company_tickers.json`

---

## D. yfinance robustness (free hardening only — PM decision: no paid sources)

**Conclusion: the 14.6% partial-failure days are the structural result of yfinance's per-ticker fan-out against Yahoo's 2025-tightened IP rate limits. Free hardening reduces but cannot eliminate them (paid bulk sources are excluded by decision).**

- yfinance has no built-in retry; the default `threads=True` concurrency is what trips the 429s. `yf.download` is not thread-safe (it shares a global dict), so it must not be wrapped in your own thread pool.
- Yahoo tightened rate limits in 2025 (the `YFRateLimitError` 429 wave began ~2025-04-29). Blocks are usually transient (access restored after ~1 hour).
- **Version-pin trap:** 0.2.61 added a hard `curl_cffi>=0.7` dependency and moved cookie/crumb auth to curl_cffi; too-old a pin breaks auth, bleeding-edge churns. Pin a known-good recent version (≥0.2.63 on curl_cffi≥0.7) and smoke-test before any bump.

**Recommended free hardening:** (a) drop batch size from 500 to ~50-80, (b) reduce concurrency (`threads=False` or a small worker cap), (c) add explicit exponential backoff with jitter (1s→2s→4s→8s, 4-5 tries), (d) throttle ~100 requests then sleep ~30s, (e) pin a known-good version. This lowers the failure rate but will not fully eliminate partial-failure days; the 90% completeness gate remains the backstop.

**Excluded paid alternative (for the record):** EODHD's bulk EOD endpoint ($19.99/mo, all ~4,600 tickers in one call) is technically the best fix, but the PM decided to exclude paid sources. Tiingo/Alpha Vantage/Stooq are unsuitable (per-ticker fan-out, tiny free tiers, or automation restrictions).

**Sources:** `ranaroussi.github.io/yfinance`, yfinance issues #2422 · #2411 · #2496 · #2557 · #2567, `github.com/ValueRaider/yfinance-cache`, `eodhd.com/pricing` (excluded, for reference), `tiingo.com/about/pricing`

---

## E. Measured snapshot (as of 2026-07-11 — data fingerprint for verification)

Use this as the baseline to compare before/after recovery in the implementation session.

```
Supabase DB size: 475 MB / 500 MB (95%)  <- the second time bomb
  rs table: 463 MB, 1,258,712 rows (heavily bloated; real data ~200MB)
  tickers:  896 kB, 4,801 rows
  meta:     168 kB, 5 rows

meta cursor state:
  ticker_list_date = 2026-07-08   (the collapse date)
  last_update_date = 2026-07-09
  last_rs_date     = 2026-07-09
  cached ticker_list length = 55  (the freeze cause; stuck until ~2026-08-07)

per-date coverage (close / rs_rating):
  2026-07-07: 4,604 / 4,425   OK - last healthy day
  2026-07-08:    57 / 0       FROZEN (rs_raw 55, rating 0)
  2026-07-09:    56 / 0       FROZEN (rs_raw 54, rating 0)

RS recompute-ability (basis for surgical recovery):
  rated trading days total: 268
  recomputable with current prices: only the most recent ~21 days
  => running recalc_all would DESTROY RS for 2025-06..2026-06 (~247 days) -> forbidden

retention state:
  close date range: 2025-06-16 .. 2026-07-09 (13 months, prune working)
  RS   date range:  2025-06-10 .. 2026-07-07 (rated)
```

**GitHub Actions:** cron `0 21 * * 1-5`; the 2026-07-10 (Fri) run was missing (a dropped scheduled run). The last commit was 2026-06-08 (32 days ago) — note GitHub disables scheduled workflows after 60 days with no commits.
