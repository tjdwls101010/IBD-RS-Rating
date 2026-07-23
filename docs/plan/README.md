# docs/plan — Recovery & Re-architecture (2026-07-23)

Planning output for restoring the RS Rating pipeline and hardening it against the failure class that
broke it. **Planning only** — implementation happens in a separate future session.

## Documents
- **[2026-07-23-master-plan.md](2026-07-23-master-plan.md)** — the contract: diagnosis, decisions,
  target designs, slice-by-slice plan, recovery runbook, testing/migration/release strategy. **Start here.**
- **[2026-07-23-audit-appendix.md](2026-07-23-audit-appendix.md)** — the 25 ranked defects, risky
  decisions, and raw audit evidence behind the plan.

## One-line problem statement
RS ratings silently stopped 2026-06-29 and real prices stopped 2026-07-17: a non-stationary rating-gate
denominator, a `finvizfinance` logo mis-parse that corrupted the universe, and a watchdog blind to
rating coverage — none caught because no CI ran the tests.

## Decision log (locked)
1. Correctness & reliability first · strictly **$0** · fix all + latent + refactor allowed · **full
   surgical backfill** of the 06-30→07-22 hole.
2. **Universe = B′**: fix the Finviz `href` parser + validity guards + demote Finviz to **weekly**
   (off the daily critical path) + **keep market-cap $50M+ semantics**. No liquidity filter, no Volume
   column (market cap isn't reconstructible from OHLCV). Nasdaq Trader = emergency fallback only.
3. Gate denominator = **active validated universe, floored, identical to the watchdog**; ranking
   population unchanged. Watchdog must also fail on rating coverage.
4. Recovery is **rating-only** (re-rank stored rs_raw); `rs_raw`/`close` byte-frozen; **never**
   `recalc_all`/`init` on the pruned DB; freeze cron during recovery.
5. Symbol form = **hyphen**; cleanup scope = **`tickers` table only**; **0.5.0** minor + a
   universe-definition version field.
6. Sequencing = **CI safety net first**, then critical/hotfix slices, then hardening; slice-by-slice
   PRs; develop **local-first** (SQLite → OrbStack Postgres → Neon branch → snapshot → prod).

## Slice map
`0 CI` → `1 gate+watchdog (co-ship)` → `2 destructive guards + backfill primitive` → `3 retention/prune
safety` → **[Recovery run]** → `4 universe B′` → `5 sector decoupling` → `6 splits` → `7 latest-date/
staleness` → `8 durability/RLS/E2E`. Release **v0.5.0** after 0–4 + recovery.
