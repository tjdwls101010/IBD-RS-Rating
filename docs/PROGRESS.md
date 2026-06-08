# IBD RS Rating — Recovery Progress & Handoff

**As of 2026-06-08** · main = `93fb792` · 6 of 8 slices merged · 2 remaining

This is a living handoff doc for resuming the pipeline-recovery work — on another machine
or in a fresh session. Authoritative details live in `CONTEXT.md`, `docs/PRD.md`,
`docs/DECISIONS.md`, and the GitHub issues/PRs; this file is the map.

## What was wrong

The goal: **every ~4,600 US stock gets an accurate RS Rating stored every trading day.**
That broke on **2026-04-17** — 97% of tickers froze while the workflow still reported
"success" (a "silent stall"). Four interlocking root causes (see `docs/DECISIONS.md`):

1. Global max-date cursor (price + RS) → ticker starvation.
2. RS Raw → NaN if any of 4 ROC terms missing (87% of cells dropped).
3. Percentile denominator = only that day's live tickers → meaningless ratings.
4. Unpinned deps (`>=`, no lock) → yfinance 0.2→1.4 broke `yf.shared._ERRORS`.

## Slices

| # | What | Status | PR |
|---|------|--------|-----|
| #1 | Pin CI deps (`requirements.lock`) + fix `init.yml` engine | ✅ merged | #9 |
| #2 | yfinance failure detection → return-coverage check | ✅ merged | #10 |
| #4 | RS correctness: per-ticker valid-day ROC, 90% universe gate, reference | ✅ merged | #11 |
| #3 | Trailing-window download (kills the stall) | ✅ merged | #12 |
| #5 | Retention: prune old `close`, preserve RS | ✅ merged | #13 |
| #6 | Silent-stall watchdog (fail `update` when latest-day coverage < 90%) | ✅ merged | #14 |
| **#7** | **Full data rebuild** (HITL — a human runs it) | ⏳ **next** | — |
| #8 | CHANGELOG + version bump (0.3.1) | ⏳ blocked by #7 | — |

**Tests:** 83 passing, all offline/deterministic (`.venv/bin/python -m pytest`).

## Remaining work

### #7 — Full rebuild (HITL, the main remaining goal)

The code is fixed but **Supabase still holds the corrupted data** (97% of tickers frozen at
2026-04-17; ratings from 4/20–5/22 computed against a 54-ticker population are wrong).
Rebuild procedure (see issue #7 for the full contract):

1. **Back up Supabase first** (the rebuild overwrites rs ratings).
2. **Sanity-run on ~100 tickers** to confirm the fixed engine behaves, then
3. **Full recompute**: RS for 2025-03-21 → present, reusing existing `close` prices,
   `recalc_all=True`, overwriting the corrupted 4/20–5/22 ratings.
4. Verify: recent trading days cover ~4,600 tickers; no day below the 90% gate.

Run locally with the repo `.venv` and `DATABASE_URL` set. This is supervised (HITL) because
it overwrites production data — watch the logs, intervene if needed.

### #8 — CHANGELOG + version

After #7: record the data-reliability recovery in `CHANGELOG.md`, bump `pyproject.toml`
0.3.0 → 0.3.1 (rs_rating client public contract unchanged → patch).

## Resuming on a new machine

```bash
# 1. Clone (code, specs, lockfile, issues/PRs all come with it)
git clone https://github.com/tjdwls101010/IBD-RS-Rating.git
cd IBD-RS-Rating

# 2. Bring .env over (NOT in git — it holds the Supabase DATABASE_URL secret).
#    Copy it securely, or recreate from Supabase dashboard > Settings > Database.
#    File contents: DATABASE_URL=postgresql://...

# 3. Recreate the venv from the lockfile (Python 3.12)
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.lock
.venv/bin/pip install --no-deps -e .

# 4. Verify
.venv/bin/python -m pytest -q          # expect 83 passed
set -a && . ./.env && set +a && .venv/bin/python -m ibd_rs status   # check Supabase reachable

# 5. (Optional, only for #8 AFK delegation) install + auth Codex
#    codex login          # #7 is HITL and does not need Codex
```

Then open Claude Code in the repo and say something like:
*"IBD-RS-Rating 복구 이어서. main에 #1~#6 머지됨, docs/PROGRESS.md 참고. 다음은 #7 전체 재구축(HITL) — 백업부터."*

## Notes / open items

- **#6 false-positive watch:** a partial latest day (post-close data lag) can trip the
  watchdog. Tune `PRICE_COMPLETENESS_THRESHOLD` or target day if it fires spuriously.
- **Trailing window** is 10 calendar days (~7 trading days); widen the constant if a longer
  safety margin is wanted.
- The daily GitHub Action will keep failing until #7 runs (no fresh data yet); that's expected.
