# Slice 0 PoC Results — Neon Data API Anonymous Read

**Date: 2026-07-11** · Issue: [#19](https://github.com/tjdwls101010/IBD-RS-Rating/issues/19) · Master plan: [`2026-07-11-neon-migration-and-reliability.md`](./2026-07-11-neon-migration-and-reliability.md) §5 Slice 0

**Result: GATE TRIGGERED.** A truly header-less anonymous read (a GET request against `/rest/v1/<table>` with no `Authorization` header at all) does **not** work on the Neon Data API, even with `db_anon_role` correctly configured and `GRANT SELECT` correctly granted to the `anonymous` role.

Reproducible script: [`scripts/poc_neon_data_api_anon_read.py`](../../scripts/poc_neon_data_api_anon_read.py).

---

## Setup

Used the existing Neon Launch project `IBD-RS-RATING` (id `withered-cherry-36264108`, org plan `launch`), its default `production` branch (empty at the time, no production data touched).

Created two throwaway tables on that branch:

- `poc_read_test` — RLS enabled, permissive `SELECT` policy, `GRANT SELECT ... TO anonymous` applied.
- `poc_no_grant` — RLS enabled, permissive `SELECT` policy, **no** grant to `anonymous` (negative control).

Provisioned the Data API via `neonctl data-api create --db-anon-role anonymous --db-schemas public`, which auto-created the `anonymous`, `authenticated`, and `authenticator` Postgres roles. Both throwaway tables were dropped at the end of the PoC; no schema or data was left behind on the project.

## Test 1 — header-less GET, granted table

```
curl https://ep-shiny-bread-ato0bxsg.apirest.c-9.us-east-1.aws.neon.tech/neondb/rest/v1/poc_read_test?select=*
```

Result: `HTTP 400`, `{"message":"missing authentication credentials: required authorization bearer token in JWT format"}`.

## Test 2 — header-less GET, ungranted table (negative control)

Same request against `poc_no_grant`: identical `HTTP 400` "missing authentication credentials" error.

Because both the granted and the ungranted table were rejected identically, the rejection happens before the grant is ever evaluated — this is a hard, unconditional requirement for a bearer token on every `/rest/v1/*` request, not a permissions issue.

## Ruled out: missing auth provider

Hypothesized that no auth provider being configured might be why the server refuses all requests outright. Enabled Neon Auth (`neonctl neon-auth enable`, provider `better_auth`) on the same branch and re-ran Test 1 and Test 2 unchanged. Identical `HTTP 400` "missing authentication credentials" result in both cases. An auth provider being present or absent made no difference — this is unrelated to the rejection.

## Test 3 — the documented token-fetch pattern

Tried the endpoint named in `access-control.md`/`get-started.md`: `GET /token/anonymous` off the Neon Auth base URL.

```
curl https://ep-shiny-bread-ato0bxsg.neonauth.c-9.us-east-1.aws.neon.tech/neondb/auth/token/anonymous
```

Result: `HTTP 200`, a JWT with `"role":"anonymous"` and a 1-hour expiry (`exp` = `iat` + 3600s).

Using that token as `Authorization: Bearer <jwt>`:

- Against `poc_read_test` (granted): `HTTP 200`, both rows returned as PostgREST-style JSON.
- Against `poc_no_grant` (not granted): `HTTP 42501`, `"permission denied for table poc_no_grant"` — the grant *is* correctly enforced once a valid token is presented.

## Root cause of the doc contradiction

A follow-up fetch of `access-control.md` surfaced the sentence the original research pass missed: with Neon Auth, a client SDK can set `allowAnonymous: true`, which "fetches a short-lived anonymous token (`GET /token/anonymous`) on the first request, caches it, and sends it as `Authorization: Bearer <jwt>` on every query."

`manage.md`'s "requests sent without an Authorization header" describes the *application developer's* experience through that SDK (you don't write an Authorization header yourself), not the actual wire protocol. At the HTTP level, every `/rest/v1/*` call requires a bearer JWT — there is no server-side header-less path. The two docs are describing different layers, not contradicting facts.

## What this means for the plan

Decision 3 (§3.3 of the master plan) assumed a truly header-less request was achievable, matching Supabase's static hardcoded anon-key model (zero round trips, no expiry, no client-side state). That assumption is now proven false. The only anonymous-read path Neon supports is fetch-a-short-lived-token-then-send-it-as-bearer, which is Neon's own intended design for this use case — not a workaround — but it is exactly the pattern the plan's Gate said not to ship ("Do NOT ship a per-request `/token/anonymous` fetch — it breaks the zero-dependency client").

Per the master plan §5 Slice 0 Gate and `AGENTS.md`'s Grilling Returns rule, this stops implementation here. See the PM decision recorded in `docs/DECISIONS.md` (once made) for how Slice 6 (public read cutover) proceeds.
