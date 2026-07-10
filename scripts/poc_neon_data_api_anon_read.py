"""Slice 0 PoC: does the Neon Data API serve a truly header-less anonymous read?

Reproduces the test from docs/plans/2026-07-11-slice0-poc-results.md.
Requires env vars:
  NEON_DATABASE_URL   - direct (unpooled) connection string to the Neon branch under test
  NEON_DATA_API_URL   - e.g. https://<ep>.apirest.<region>.aws.neon.tech/<db>/rest/v1
  NEON_AUTH_BASE_URL  - e.g. https://<ep>.neonauth.<region>.aws.neon.tech/<db>/auth (only needed for the token-fetch check)

Creates two throwaway tables, grants anonymous SELECT on only one, and checks:
  1. A request with NO Authorization header against the granted table.
  2. A request with NO Authorization header against the ungranted table.
  3. A request using a token fetched from GET /token/anonymous, against both tables.
Drops the throwaway tables when done, regardless of outcome.
"""
import json
import os
import urllib.error
import urllib.request

import psycopg2

DB_URL = os.environ["NEON_DATABASE_URL"]
DATA_API_URL = os.environ["NEON_DATA_API_URL"]
AUTH_BASE_URL = os.environ.get("NEON_AUTH_BASE_URL")


def anon_get(path, token=None):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    req = urllib.request.Request(f"{DATA_API_URL}{path}", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def fetch_anonymous_token():
    with urllib.request.urlopen(f"{AUTH_BASE_URL}/token/anonymous", timeout=15) as resp:
        return json.loads(resp.read().decode())["token"]


def main():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("CREATE TABLE poc_read_test (id serial PRIMARY KEY, note text)")
    cur.execute("INSERT INTO poc_read_test (note) VALUES ('hello anon'), ('second row')")
    cur.execute("ALTER TABLE poc_read_test ENABLE ROW LEVEL SECURITY")
    cur.execute("CREATE POLICY poc_read_test_select ON poc_read_test FOR SELECT USING (true)")
    cur.execute("GRANT SELECT ON poc_read_test TO anonymous")

    cur.execute("CREATE TABLE poc_no_grant (id serial PRIMARY KEY, note text)")
    cur.execute("INSERT INTO poc_no_grant (note) VALUES ('should not be visible')")
    cur.execute("ALTER TABLE poc_no_grant ENABLE ROW LEVEL SECURITY")
    cur.execute("CREATE POLICY poc_no_grant_select ON poc_no_grant FOR SELECT USING (true)")

    try:
        print("=== header-less GET, granted table ===")
        print(anon_get("/poc_read_test?select=*"))

        print("=== header-less GET, ungranted table ===")
        print(anon_get("/poc_no_grant?select=*"))

        if AUTH_BASE_URL:
            token = fetch_anonymous_token()
            print("=== GET /token/anonymous + Bearer, granted table ===")
            print(anon_get("/poc_read_test?select=*", token=token))
            print("=== GET /token/anonymous + Bearer, ungranted table ===")
            print(anon_get("/poc_no_grant?select=*", token=token))
    finally:
        cur.execute("DROP TABLE IF EXISTS poc_read_test")
        cur.execute("DROP TABLE IF EXISTS poc_no_grant")
        print("cleaned up throwaway tables")


if __name__ == "__main__":
    main()
