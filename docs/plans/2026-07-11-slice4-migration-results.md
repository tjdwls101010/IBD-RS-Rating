# Slice 4 Results — Neon Migration: Schema + Data + Secrets

**Date: 2026-07-11** · Master plan: [`2026-07-11-neon-migration-and-reliability.md`](./2026-07-11-neon-migration-and-reliability.md) §5 Slice 4

**Result: DONE.** The write database is now Neon. Row counts match the Supabase source exactly, table bloat is reclaimed (463 MB → 128 MB for `rs`), the writer role can write, and the GitHub Actions `DATABASE_URL` secret now points at Neon.

---

## What was done

- **Target:** the existing Neon Launch project `IBD-RS-RATING` (id `withered-cherry-36264108`, org plan `launch`), its default `production` branch, database `neondb` (already provisioned, no separate `CREATE DATABASE` needed since it already exists and was empty).
- **Dump:** `pg_dump -Fc -v --schema=public` from Supabase over the **session-mode** pooler connection (`aws-1-us-east-1.pooler.supabase.com:5432`, not the transaction-mode `:6543` the app uses for normal queries — transaction pooling drops the `SET` statements `pg_dump` relies on). Took 56s, produced a 24 MB compressed file.
- **Restore:** `pg_restore -v --no-owner --no-acl` over Neon's **direct** (unpooled) endpoint. Took 21s. One harmless ignored error (`schema "public" already exists` — Neon's `neondb` ships with a `public` schema by default; `pg_restore` continues past it and everything else applied cleanly, including the `rs`/`tickers`/`meta` tables, PKs, the `idx_rs_date` index, and the RLS policies/`ROW SECURITY` settings that were present in the Supabase schema).
- **Secret:** `DATABASE_URL` GitHub Actions secret updated to the Neon direct endpoint with `?sslmode=require&channel_binding=require`. Local `.env` updated to match (old Supabase URL kept commented out for reference until Supabase is decommissioned in Slice 6).

## Verification against the fingerprint

```
                Supabase (before)      Neon (after)
db size:        475 MB                 136 MB
rs table:        463 MB (bloated)       128 MB
rs rows:        1,258,712              1,258,712   <- exact match
tickers rows:        4,801                  4,801  <- exact match
meta rows:                5                     5  <- exact match
rs table owner:        --             neondb_owner (writer role, confirmed writable)
```

Bloat reclamation exceeded the plan's ~150-250 MB estimate (actual: 128 MB) — a fresh `pg_restore` rebuilds tables without the dead-tuple accumulation from years of daily UPDATE/DELETE churn.

## Write-path verification

Ran the app's actual `ibd_rs.db.upsert_prices()` (the exact `ON CONFLICT ... DO UPDATE` path `cmd_update` uses) against Neon directly via `db.get_connection()` with `DATABASE_URL` pointed at Neon: inserted a throwaway row, upserted a conflicting update, confirmed the updated value round-tripped correctly, then deleted the test row. Also verified `get_meta`/`set_meta`. All passed. No throwaway data was left behind.

## Deliberately not done in this slice

- **No read cutover.** The public `rs_rating` client still points at Supabase (Slice 6, gated on the Slice 0 PoC's token-cache pattern).
- **No pipeline run.** The cached ticker list (`meta.ticker_list`, still the frozen 55-ticker list from 2026-07-08) was not cleared and no `update`/`recalc` was run against Neon — that is Slice 5's job (surgical recovery), which needs the fixed universe fetch (Slice 1, merged) and self-healing recompute (Slice 2, merged) already in place, which they now are.
- **Supabase not decommissioned.** Left as-is and untouched (still holds the same frozen pre-migration data as a fallback/rollback target) until the read cutover in Slice 6 is verified end-to-end, per the plan's rollback design (§10).

## What the next session needs to know

The GitHub Actions cron (`daily_update.yml`, weekdays 21:00 UTC) is active and will next fire 2026-07-13 (Monday). It now runs the Slice 1-3 hardened code against Neon (via the updated secret) but will still see the stale 55-ticker cache (Slice 1's fresh-cache path doesn't re-validate an already-cached list) and correctly FAIL via Slice 2's absolute-floor watchdog rather than silently succeeding. Slice 5 should clear that cache before the next scheduled run to avoid an avoidable red run, and should proceed soon — the backfill window for the frozen 2026-07-08/09 days closes around 2026-07-18.
