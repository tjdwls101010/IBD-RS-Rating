# Security Policy

## Supported versions

Only the latest published release receives security fixes. Older versions are not patched — upgrade to the current release before reporting an issue you found on an older one.

| Version | Supported |
|---------|-----------|
| 0.4.x   | Yes       |
| < 0.4   | No        |

Version 0.4.0 moved the client's backend from Supabase to Neon. Releases before it read from an endpoint that no longer receives data, so they are unsupported both functionally and for security purposes.

## Reporting a vulnerability

**Do not open a public issue for a security problem.** A public issue discloses the vulnerability to everyone before there is a fix.

Email **chunghun1@naver.com** with the subject line prefixed `[SECURITY] IBD-RS-Rating`.

## What to include

The more of this you can provide, the faster it can be confirmed and fixed:

- What the vulnerability is and what an attacker could achieve with it
- The affected version, and whether it affects the `rs_rating` client, the `ibd_rs` engine, or both
- Steps to reproduce, ideally as a minimal script
- Any relevant environment detail — Python version, database backend, whether `DATABASE_URL` is set

## What to expect

This project is maintained by one person in their own time, so no fixed response-time guarantee is made. In practice you should expect an acknowledgement within about a week. You will be told whether the report is accepted, and kept informed as a fix progresses.

Please allow a reasonable window for a fix to ship before disclosing publicly. If you would like credit in the release notes, say so in your report.

## Scope notes

Things that are in scope:

- Anything allowing writes, deletion, or unintended data access through the public read endpoint, which is meant to be strictly read-only
- Credential or connection-string leakage in the engine — for example a `DATABASE_URL` appearing in logs or error output
- Code execution or injection reachable through the client's query construction or the engine's SQL

Things that are **not** in scope:

- Inaccurate or stale RS Rating data. That is a data-quality bug — open a normal issue. See [Troubleshooting](docs/wiki/Troubleshooting.md).
- Vulnerabilities in upstream dependencies (`yfinance`, `finvizfinance`, `pandas`, `psycopg2`) that do not have a specific exploitable path through this project. Report those upstream; if there is an exploitable path here, that path is in scope.
- Availability of the free hosted endpoint. It is best-effort with no uptime commitment; self-host if you need guarantees.
