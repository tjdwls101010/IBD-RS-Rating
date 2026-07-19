# Operations

Running the pipeline yourself: database setup, scheduling, the reliability guards and what each one protects against, and how to know the data is still good.

If you only read ratings from the hosted endpoint, you don't need this page.

## Why self-host

The public endpoint is free, best-effort, and run by one person with no uptime commitment. Self-host if you need guaranteed availability, want a different universe or formula, need history beyond the retention window, or simply don't want a production dependency on someone else's free service.

The cost is real but modest: one scheduled job, a Postgres database of a few GB, and attention when it fails.

## Database setup

### SQLite

Nothing to configure. Leave `DATABASE_URL` unset and data goes to `data/rs.db`. Fine for local analysis; not for a shared or long-running deployment.

### Postgres

```bash
export DATABASE_URL="postgresql://user:password@host:5432/dbname"
```

Schema creation is automatic and idempotent — `init_db()` runs at the start of `init` and `update` with `CREATE TABLE IF NOT EXISTS`. There is no migration step and no migration tool.

**Sizing.** With 13-month close retention and indefinite rating retention, expect a few GB after the first year, growing slowly. The `rs` table dominates; `idx_rs_date` is the index that matters, since almost every query is date-scoped.

**Bloat.** Heavy upsert traffic on the same rows generates dead tuples. Postgres autovacuum normally handles it; if the table grows conspicuously faster than the data, check that autovacuum is running and consider a manual `VACUUM (FULL, ANALYZE) rs` during a maintenance window.

### Serverless Postgres

Neon and similar services suspend compute when idle, which interacts badly with a long pipeline. Two mitigations are built in:

- TCP keepalives on the connection (`keepalives_idle=30`) for proxies dropping idle sockets.
- `db.reconnect()`, which probes with `SELECT 1` and transparently reconnects — because keepalives don't help when the *server* ends the session. The Finviz scrape can run for minutes with no database activity, and the socket survives while the session doesn't.

Both are automatic. The consequence for contributors is that `fetch_ticker_list()` returns a connection alongside its result, and callers must use the returned one.

## Scheduling

### GitHub Actions

The repository ships three workflows in `.github/workflows/`:

| Workflow | Trigger | Timeout | Purpose |
|---|---|---|---|
| `daily_update.yml` | Cron `0 21 * * 1-5`, or manual | 30 min | The daily `update` |
| `init.yml` | Manual only | 120 min | First-time or rebuild `init` |
| `publish.yml` | GitHub release published | — | Build and upload to PyPI |

To run it in your own fork: set `DATABASE_URL` as a repository secret, run `init.yml` once manually, and let `daily_update.yml` take over.

**21:00 UTC** is one hour after the US close (16:00 ET / 21:00 UTC in winter, 20:00 UTC in summer), leaving margin for vendor data to settle. Weekdays only — there are no weekend trading days to fetch.

Both data workflows install from `requirements.lock`, then `pip install --no-deps .`:

```yaml
- name: Install dependencies
  run: |
    python -m pip install -r requirements.lock
    python -m pip install --no-deps .
```

`--no-deps` is deliberate. Installing the package normally would let pip resolve `pyproject.toml`'s loose ranges and quietly override the exact versions just installed, defeating the lock. `tests/test_ci_dependencies.py` asserts the workflows keep doing it this way.

### cron

```cron
0 21 * * 1-5 cd /path/to/IBD-RS-Rating && /path/to/.venv/bin/python -m ibd_rs update >> /var/log/ibd-rs.log 2>&1 || /path/to/alert.sh
```

The `|| alert` is not optional — see below.

### systemd timer

```ini
# ibd-rs-update.service
[Service]
Type=oneshot
WorkingDirectory=/opt/IBD-RS-Rating
Environment="DATABASE_URL=postgresql://..."
ExecStart=/opt/IBD-RS-Rating/.venv/bin/python -m ibd_rs update
```

```ini
# ibd-rs-update.timer
[Timer]
OnCalendar=Mon..Fri 21:00 UTC
Persistent=true
```

`OnFailure=` on the service unit gives you alerting.

## Monitoring

### The one thing that matters

**`update` exits non-zero when the run is not trustworthy. Make sure that reaches a human.**

Everything else on this page is secondary. The April 2026 outage lasted five weeks not because alerting was missing — GitHub's failure emails worked — but because the failure never reported itself as one. The job went green while 97% of tickers were frozen. `update`'s exit code is the fix, and it only works if something is listening.

On GitHub Actions, a failed run emails the repository owner by default; nothing extra is needed. On cron or systemd, wire it up explicitly.

### What triggers a failure

| Condition | Meaning |
|---|---|
| Universe fetch untrusted | The screener returned a suspicious result; the run used cached or fallback membership |
| Latest-day close coverage < 90% | Prices are missing for too much of the universe |

Both mean "this run's data is suspect," not "the run did nothing." Data was still written.

### Routine checks

```bash
python -m ibd_rs status
```

Weekly is plenty if failures alert reliably. The fields to watch are in [CLI Reference](CLI-Reference.md#status).

Directly against the database, the sharpest single query is the coverage trend:

```sql
SELECT date,
       COUNT(*) FILTER (WHERE close IS NOT NULL)     AS with_close,
       COUNT(*) FILTER (WHERE rs_rating IS NOT NULL) AS with_rating
FROM rs
WHERE date >= (CURRENT_DATE - INTERVAL '30 days')::text
GROUP BY date
ORDER BY date DESC;
```

Healthy output holds both columns near the universe size every trading day. A `with_close` that declines while `with_rating` drops to zero is the signature of a stall being correctly caught by the threshold — data is degrading, and the system is refusing to publish ratings rather than publishing wrong ones.

### Failure modes worth recognising

| Symptom | Likely cause |
|---|---|
| Coverage fine, ratings all null on recent dates | Universe threshold rejecting dates — check universe size, it may have inflated |
| Universe size collapsed | Screener blocked or truncated; validation should have caught it and flagged untrusted |
| Run times out | Retention not running, so the pivot keeps growing; check step 5 output |
| Many failed tickers | yfinance rate limiting; consider lowering `BATCH_SIZE` or raising `INTER_BATCH_SLEEP_SECONDS` |
| `SSL connection has been closed unexpectedly` | Serverless suspend outside a `reconnect()` guard — worth reporting as a bug |

[Troubleshooting](Troubleshooting.md) has the diagnosis-and-fix detail.

## The reliability guards

Each guard exists because of a specific failure. Removing one to "simplify" reopens that failure.

| Guard | Where | Protects against |
|---|---|---|
| Trailing-window download | `prices.py` | Ticker starvation from a global cursor |
| Universe floor (3,000) | `tickers.py` | Catastrophically truncated screener fetch |
| Universe drop guard (90%) | `tickers.py` | Gradual universe degradation |
| Universe completeness (98%) | `tickers.py` | Partial fetch that clears the floor |
| Cache-on-trusted-only | `tickers.py` | A bad fetch poisoning the cache for 7 days |
| RS universe threshold (90%) | `rs.py` | Ratings against a collapsed population |
| Recompute window (15 days) | `rs.py` | Dates permanently stuck unrated |
| Per-batch failure isolation | `prices.py` | One bad batch killing the whole run |
| Transaction rollback on write failure | `prices.py` | An aborted transaction poisoning later batches |
| Connection reconnect | `db.py` | Serverless auto-suspend mid-run |
| Completeness check + exit 1 | `db.py`, `cli.py` | Silent stalls reporting success |
| Exact dependency pins | `requirements.lock` | Upstream releases breaking the job overnight |

### Tuning

Safe to adjust for your environment:

| Constant | When to change |
|---|---|
| `BATCH_SIZE`, `DOWNLOAD_THREADS`, `INTER_BATCH_SLEEP_SECONDS` | Rate limiting — lower them |
| `CACHE_DAYS` | More or less frequent universe refresh |
| `PRICE_RETENTION_MONTHS` | Storage vs. recompute headroom |
| `SCREENER_FILTERS`, `EXCLUDED_INDUSTRIES` | You want a different universe |

Change with care:

| Constant | Why |
|---|---|
| `RS_WEIGHTS` | Redefines what every rating means; new values aren't comparable to stored history. Run `recalc` after. |
| `RS_UNIVERSE_THRESHOLD` | Lowering it publishes ratings computed against thinner populations — exactly the failure the threshold exists to prevent |
| `RS_RECOMPUTE_WINDOW_DAYS` | Bounded on both sides; above ~21 trading days, recomputed dates need lookback prices retention has deleted. A test enforces this. |
| `UNIVERSE_FLOOR` and the two ratios | These are the truncation defences |

## Backup and recovery

**Back up before any bulk operation** — a `recalc`, a retention change, a formula change:

```bash
pg_dump "$DATABASE_URL" -t rs -t tickers -t meta | gzip > rs_backup_$(date +%F).sql.gz
```

Recovery paths, cheapest first:

| Damage | Fix |
|---|---|
| Ratings wrong, prices good | `python -m ibd_rs recalc` — no downloads |
| Recent prices missing | `python -m ibd_rs update` — the trailing window backfills up to 10 days |
| Older prices missing | Restore from backup, or `init` to rebuild |
| Universe cache poisoned | `DELETE FROM meta WHERE key IN ('ticker_list','ticker_list_date')`, then `update` |
| Total loss | `python -m ibd_rs init` — 30 minutes, but history beyond 2 years is gone |

Note the asymmetry: prices are re-downloadable, ratings are re-computable, but **history older than the 2-year download window is not recoverable** once lost. If long history matters to you, back it up.

## Cost

Roughly, per month:

| Item | Cost |
|---|---|
| GitHub Actions | Free on public repos; ~2 hr/month of private minutes otherwise |
| Postgres | Free tier on most serverless providers; a few GB |
| yfinance / Finviz | Free, unofficial, no SLA |

The data sources are the real risk, not the money. Both are free unofficial endpoints that can rate-limit, change format, or disappear. The universe fallback and the download retry logic are hedges, not guarantees.

## Next

- [Data Pipeline](Data-Pipeline.md) — the mechanics of each stage
- [CLI Reference](CLI-Reference.md) — the commands you're scheduling
- [Troubleshooting](Troubleshooting.md) — when something goes wrong

[← Back to index](README.md)
