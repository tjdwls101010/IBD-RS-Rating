"""Tests for database operations."""

import struct
from datetime import datetime, timedelta
from pathlib import Path

import pytest

import pandas as pd
from ibd_rs import db
from ibd_rs.config import PRICE_RETENTION_MONTHS


@pytest.fixture
def conn():
    c = db.get_connection(":memory:")
    db.init_db(c)
    yield c
    c.close()


def test_reconnect_returns_same_connection_when_alive(conn):
    assert db.reconnect(conn) is conn


def test_reconnect_returns_a_fresh_connection_when_dead(monkeypatch):
    class DeadConn:
        def cursor(self):
            raise Exception("SSL connection has been closed unexpectedly")

        def close(self):
            pass

    fresh_conn = db.get_connection(":memory:")
    monkeypatch.setattr(db, "get_connection", lambda *a, **kw: fresh_conn)

    assert db.reconnect(DeadConn()) is fresh_conn


def _set_retention_now(monkeypatch):
    fixed_now = datetime(2026, 6, 8)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls):
            return fixed_now

    monkeypatch.setattr(db, "datetime", FixedDateTime)
    return (
        fixed_now - timedelta(days=PRICE_RETENTION_MONTHS * 30)
    ).strftime("%Y-%m-%d")


def _offset_date(date, days):
    return (
        datetime.strptime(date, "%Y-%m-%d") + timedelta(days=days)
    ).strftime("%Y-%m-%d")


def test_init_db_creates_tables(conn):
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    names = [t[0] for t in tables]
    assert "rs" in names
    assert "tickers" in names
    assert "meta" in names


def test_init_db_idempotent(conn):
    db.init_db(conn)


def test_init_db_pg_schema_includes_idempotent_rls_and_migration_parity():
    executed = []

    class FakeCursor:
        def execute(self, sql):
            executed.append(sql)

        def close(self):
            pass

    class FakePgConnection:
        def __init__(self):
            self.committed = False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            self.committed = True

    conn = FakePgConnection()
    db.init_db(conn)

    schema_sql = " ".join(db.SCHEMA_SQL_PG.split())
    rls_sql = " ".join(db.RLS_GRANT_SQL_PG.split())
    for table in ("rs", "tickers", "meta"):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in schema_sql
        assert f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;" in rls_sql
        assert f"DROP POLICY IF EXISTS read_{table} ON {table};" in rls_sql
        assert (
            f"CREATE POLICY read_{table} ON {table} FOR SELECT USING (true);"
            in rls_sql
        )

    guarded_grant = (
        "IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anonymous') THEN "
        "GRANT SELECT ON rs, tickers, meta TO anonymous; "
        "END IF;"
    )
    assert guarded_grant in rls_sql
    assert executed == [db.SCHEMA_SQL_PG, db.RLS_GRANT_SQL_PG]
    assert conn.committed

    migration_path = (
        Path(__file__).resolve().parents[1] / "migrations" / "0001_rls_grant.sql"
    )
    expected_migration = (
        db.SCHEMA_SQL_PG + db.RLS_GRANT_SQL_PG
    ).encode().removeprefix(b"\n")
    assert migration_path.read_bytes() == expected_migration


def test_init_db_sqlite_does_not_execute_postgresql_rls():
    sqlite_conn = db.get_connection(":memory:")
    statements = []
    sqlite_conn.set_trace_callback(statements.append)
    try:
        db.init_db(sqlite_conn)
        tables = {
            row[0]
            for row in sqlite_conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    finally:
        sqlite_conn.close()

    assert {"rs", "tickers", "meta"} <= tables
    assert not any("ROW LEVEL SECURITY" in statement for statement in statements)


def test_upsert_prices(conn):
    records = [
        ("AAPL", "2026-01-01", 150.0),
        ("AAPL", "2026-01-02", 152.0),
        ("NVDA", "2026-01-01", 800.0),
    ]
    db.upsert_prices(conn, records)
    count = conn.execute("SELECT COUNT(*) FROM rs WHERE close IS NOT NULL").fetchone()[0]
    assert count == 3


def test_upsert_prices_does_not_overwrite_rs(conn):
    """Upserting prices should not overwrite existing rs_raw/rs_rating."""
    db.upsert_rs(conn, [("AAPL", "2026-01-01", 0.345, 72)])
    db.upsert_prices(conn, [("AAPL", "2026-01-01", 155.0)])

    row = conn.execute(
        "SELECT close, rs_raw, rs_rating FROM rs WHERE ticker='AAPL' AND date='2026-01-01'"
    ).fetchone()
    assert row[0] == 155.0  # close updated
    assert row[1] == 0.345  # rs_raw preserved
    assert row[2] == 72     # rs_rating preserved


def test_upsert_rs(conn):
    records = [
        ("AAPL", "2026-01-01", 0.345, 72),
        ("SPY", "2026-01-01", 0.123, 55),
    ]
    db.upsert_rs(conn, records)
    count = conn.execute("SELECT COUNT(*) FROM rs").fetchone()[0]
    assert count == 2


def test_clear_rs_for_dates_commit_false_leaves_uncommitted(conn):
    date = "2026-01-01"
    db.upsert_rs(conn, [("AAPL", date, 0.345, 72)])
    before = conn.execute(
        "SELECT rs_raw, rs_rating FROM rs WHERE ticker = ? AND date = ?",
        ("AAPL", date),
    ).fetchone()

    db.clear_rs_for_dates(conn, [date], commit=False)

    assert conn.execute(
        "SELECT rs_raw, rs_rating FROM rs WHERE ticker = ? AND date = ?",
        ("AAPL", date),
    ).fetchone() == (None, None)

    conn.rollback()
    assert conn.execute(
        "SELECT rs_raw, rs_rating FROM rs WHERE ticker = ? AND date = ?",
        ("AAPL", date),
    ).fetchone() == before



def test_update_rs_ratings_writes_only_rs_rating(conn):
    date = "2026-01-02"
    close = 123.456789012345
    rs_raw = 0.123456789012345
    db.upsert_prices(conn, [("AAPL", date, close)])
    db.upsert_rs(conn, [("AAPL", date, rs_raw, 41)])

    before = conn.execute(
        "SELECT close, rs_raw, rs_rating FROM rs WHERE ticker = ? AND date = ?",
        ("AAPL", date),
    ).fetchone()
    immutable_bytes = tuple(struct.pack("!d", value) for value in before[:2])

    db.update_rs_ratings(
        conn,
        [
            ("AAPL", date, 87),
            ("MISSING", "2026-01-03", 99),
        ],
    )

    after = conn.execute(
        "SELECT close, rs_raw, rs_rating FROM rs WHERE ticker = ? AND date = ?",
        ("AAPL", date),
    ).fetchone()
    assert tuple(struct.pack("!d", value) for value in after[:2]) == immutable_bytes
    assert after[2] == 87
    assert conn.execute(
        "SELECT COUNT(*) FROM rs WHERE ticker = ? AND date = ?",
        ("MISSING", "2026-01-03"),
    ).fetchone()[0] == 0

def test_upsert_tickers(conn):
    records = [
        ("AAPL", "Technology", "Consumer Electronics"),
        ("NVDA", "Technology", "Semiconductors"),
        ("LLY", "Healthcare", "Drug Manufacturers"),
    ]
    db.upsert_tickers(conn, records)
    count = conn.execute("SELECT COUNT(*) FROM tickers").fetchone()[0]
    assert count == 3

    row = conn.execute("SELECT sector, industry FROM tickers WHERE ticker='NVDA'").fetchone()
    assert row[0] == "Technology"
    assert row[1] == "Semiconductors"


def test_get_prices_df(conn):
    db.upsert_prices(conn, [
        ("AAPL", "2026-01-01", 150.0),
        ("AAPL", "2026-01-02", 152.0),
        ("NVDA", "2026-01-01", 800.0),
        ("NVDA", "2026-01-02", 810.0),
    ])
    df = db.get_prices_df(conn)
    assert df.shape == (2, 2)
    assert "AAPL" in df.columns
    assert "NVDA" in df.columns



def test_get_rs_raw_df_pivots_stored_rs_raw(conn):
    db.upsert_rs(
        conn,
        [
            ("NVDA", "2026-01-02", 0.4, 90),
            ("AAPL", "2026-01-01", 0.1, 40),
            ("NVDA", "2026-01-01", 0.3, 80),
            ("AAPL", "2026-01-02", 0.2, 60),
            ("SPY", "2026-01-03", 0.5, 70),
        ],
    )
    db.upsert_prices(conn, [("CLOSE_ONLY", "2026-01-01", 10.0)])

    raw_df = db.get_rs_raw_df(conn)
    assert list(raw_df.index) == list(pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"]))
    assert list(raw_df.columns) == ["AAPL", "NVDA", "SPY"]
    assert raw_df.at[pd.Timestamp("2026-01-01"), "AAPL"] == 0.1
    assert raw_df.at[pd.Timestamp("2026-01-02"), "NVDA"] == 0.4
    assert all(pd.api.types.is_float_dtype(dtype) for dtype in raw_df.dtypes)

    bounded = db.get_rs_raw_df(conn, start="2026-01-02", end="2026-01-02")
    assert list(bounded.index) == [pd.Timestamp("2026-01-02")]
    assert list(bounded.columns) == ["AAPL", "NVDA"]


def test_get_stored_rs_raw_keys(conn):
    dates = list(pd.date_range("2024-01-01", periods=503).strftime("%Y-%m-%d"))
    db.upsert_rs(
        conn,
        [("AAPL", date, float(i), 50) for i, date in enumerate(dates)]
        + [("NVDA", dates[0], 0.25, 75)]
        + [("OUTSIDE", "2026-01-01", 0.5, 90)]
        + [("NULL_RAW", dates[1], None, 80)],
    )

    keys = db.get_stored_rs_raw_keys(conn, (date for date in dates))

    assert keys == {("AAPL", date) for date in dates} | {("NVDA", dates[0])}
    assert db.get_stored_rs_raw_keys(conn, []) == set()

def test_get_latest_price_date(conn):
    assert db.get_latest_price_date(conn) is None
    db.upsert_prices(conn, [("AAPL", "2026-01-01", 150.0), ("AAPL", "2026-03-01", 160.0)])
    assert db.get_latest_price_date(conn) == "2026-03-01"


def test_get_latest_rated_date(conn):
    db.upsert_rs(
        conn,
        [
            ("RATED", "2026-07-18", 0.5, 91),
            ("OUTAGE", "2026-07-22", 0.6, None),
        ],
    )

    assert db.get_latest_rated_date(conn) == "2026-07-18"
    assert db.get_latest_rs_date(conn) == "2026-07-22"
    assert db.get_latest_rated_date(conn) != db.get_latest_rs_date(conn)


def test_get_oldest_rs_date(conn):
    assert db.get_oldest_rs_date(conn) is None

    db.upsert_prices(conn, [("PRICE_ONLY", "2025-01-01", 10.0)])
    db.upsert_rs(
        conn,
        [
            ("AAPL", "2026-03-01", 0.3, 70),
            ("NVDA", "2026-01-01", 0.2, 60),
        ],
    )

    assert db.get_oldest_rs_date(conn) == "2026-01-01"
def test_check_latest_trading_day_completeness_detects_stalled_database(conn):
    universe = [f"T{i}" for i in range(10)]
    db.upsert_prices(
        conn,
        [(ticker, "2026-04-17", 10.0) for ticker in universe],
    )
    db.upsert_prices(
        conn,
        [(ticker, "2026-05-22", 20.0) for ticker in universe[:2]],
    )

    report = db.check_latest_trading_day_completeness(
        conn,
        universe,
        threshold=0.90,
    )

    assert report["latest_date"] == "2026-05-22"
    assert report["universe_size"] == 10
    assert report["close_coverage"] == 2
    assert report["missing_close_count"] == 8
    assert report["rating_coverage"] == 0
    assert report["missing_rating_count"] == 10
    assert report["coverage_ratio"] == 0.2
    assert report["threshold"] == 0.90
    assert report["is_complete"] is False
    assert report["reason"] == "close_coverage_below_threshold"


def test_classify_latest_trading_day_completeness_threshold_boundary():
    failed = db.classify_latest_trading_day_completeness(
        latest_date="2026-05-22",
        universe_size=100,
        close_coverage=89,
        rating_coverage=90,
        threshold=0.90,
    )
    passed = db.classify_latest_trading_day_completeness(
        latest_date="2026-05-22",
        universe_size=100,
        close_coverage=90,
        rating_coverage=90,
        threshold=0.90,
    )

    assert failed["is_complete"] is False
    assert failed["missing_close_count"] == 11
    assert passed["is_complete"] is True
    assert passed["missing_close_count"] == 10


def test_classify_gates_on_rating_coverage_boundary():
    """BUG3 regression: with close coverage satisfied, rating coverage below
    the threshold must FAIL, with a distinct reason. The 3-week silent outage
    had ~98% close coverage while the ratings were NULL."""
    failed = db.classify_latest_trading_day_completeness(
        latest_date="2026-06-30",
        universe_size=100,
        close_coverage=98,
        rating_coverage=89,
        threshold=0.90,
    )
    passed = db.classify_latest_trading_day_completeness(
        latest_date="2026-06-30",
        universe_size=100,
        close_coverage=98,
        rating_coverage=90,
        threshold=0.90,
    )

    assert failed["is_complete"] is False
    assert failed["reason"] == "rating_coverage_below_threshold"
    assert passed["is_complete"] is True
    assert passed["reason"] == "complete"


def test_classify_zero_rating_coverage_fails_even_with_full_close():
    """The exact silent-outage shape: full close coverage, zero ratings."""
    report = db.classify_latest_trading_day_completeness(
        latest_date="2026-06-30",
        universe_size=100,
        close_coverage=100,
        rating_coverage=0,
        threshold=0.90,
    )

    assert report["is_complete"] is False
    assert report["reason"] == "rating_coverage_below_threshold"


def test_check_latest_trading_day_completeness_fails_without_price_data(conn):
    report = db.check_latest_trading_day_completeness(
        conn,
        ["AAPL", "NVDA"],
        threshold=0.90,
    )

    assert report["latest_date"] is None
    assert report["universe_size"] == 2
    assert report["close_coverage"] == 0
    assert report["missing_close_count"] == 2
    assert report["coverage_ratio"] == 0.0
    assert report["is_complete"] is False
    assert report["reason"] == "no_price_data"


def test_check_latest_trading_day_completeness_fails_on_universe_collapse_against_absolute_floor(conn):
    """Reproduces the watchdog blind spot from the 2026-07-08 incident: the
    fetched universe itself collapsed to 55 tickers, so measuring coverage
    against that same collapsed universe reads as 100% PASS. Passing
    min_universe_size (the last-good/expected universe size, independent of
    what this run happened to fetch) must fail the run instead."""
    collapsed_universe = [f"T{i}" for i in range(55)]
    db.upsert_prices(conn, [(ticker, "2026-07-08", 10.0) for ticker in collapsed_universe])

    report = db.check_latest_trading_day_completeness(
        conn,
        collapsed_universe,
        threshold=0.90,
        min_universe_size=4600,
    )

    assert report["universe_size"] == 4600
    assert report["close_coverage"] == 55
    assert report["coverage_ratio"] < 0.02
    assert report["is_complete"] is False


def test_check_latest_trading_day_completeness_min_universe_size_defaults_to_no_floor(conn):
    universe = [f"T{i}" for i in range(10)]
    db.upsert_prices(conn, [(ticker, "2026-05-22", 20.0) for ticker in universe])

    report = db.check_latest_trading_day_completeness(conn, universe, threshold=0.90)

    assert report["universe_size"] == 10  # unaffected when min_universe_size is not passed


def test_check_latest_trading_day_completeness_fails_without_universe(conn):
    report = db.check_latest_trading_day_completeness(
        conn,
        [],
        threshold=0.90,
    )

    assert report["latest_date"] is None
    assert report["universe_size"] == 0
    assert report["is_complete"] is False
    assert report["reason"] == "universe_unknown"


def test_meta(conn):
    assert db.get_meta(conn, "test_key") is None
    db.set_meta(conn, "test_key", "test_value")
    assert db.get_meta(conn, "test_key") == "test_value"
    db.set_meta(conn, "test_key", "updated")
    assert db.get_meta(conn, "test_key") == "updated"


def test_get_price_stats(conn):
    db.upsert_prices(conn, [("AAPL", "2026-01-01", 150.0)])
    db.upsert_rs(conn, [("AAPL", "2026-01-01", 0.3, 70)])
    stats = db.get_price_stats(conn)
    assert stats["price_rows"] == 1
    assert stats["price_tickers"] == 1
    assert stats["rs_rows"] == 1


def test_prune_old_close_deletes_old_rows_without_rating_and_preserves_rs(conn, monkeypatch):
    cutoff = _set_retention_now(monkeypatch)
    old_date = _offset_date(cutoff, -1)
    recent_date = _offset_date(cutoff, 1)

    db.upsert_prices(
        conn,
        [
            ("AAPL", old_date, 150.0),
            ("AAPL", recent_date, 160.0),
            ("MSFT", old_date, 250.0),
        ],
    )
    db.upsert_rs(conn, [("AAPL", old_date, 0.345, 72)])
    conn.execute(
        "INSERT INTO rs (ticker, date, close, rs_raw, rs_rating) VALUES (?, ?, ?, ?, ?)",
        ("GOOGL", old_date, None, None, None),
    )

    pruned = db.prune_old_close(conn)

    assert pruned == 3
    old_with_rs = conn.execute(
        "SELECT close, rs_raw, rs_rating FROM rs WHERE ticker = ? AND date = ?",
        ("AAPL", old_date),
    ).fetchone()
    recent = conn.execute(
        "SELECT close, rs_raw, rs_rating FROM rs WHERE ticker = ? AND date = ?",
        ("AAPL", recent_date),
    ).fetchone()
    old_close_only = conn.execute(
        "SELECT close, rs_raw, rs_rating FROM rs WHERE ticker = ? AND date = ?",
        ("MSFT", old_date),
    ).fetchone()
    old_empty_shell = conn.execute(
        "SELECT close, rs_raw, rs_rating FROM rs WHERE ticker = ? AND date = ?",
        ("GOOGL", old_date),
    ).fetchone()

    assert old_with_rs == (None, 0.345, 72)
    assert recent == (160.0, None, None)
    assert old_close_only is None
    assert old_empty_shell is None
    assert db.get_rs_history(conn, "AAPL", 10) == [(old_date, 0.345, 72)]


def test_prune_old_close_keeps_cutoff_boundary_and_is_idempotent(conn, monkeypatch):
    cutoff = _set_retention_now(monkeypatch)
    old_date = _offset_date(cutoff, -1)
    recent_date = _offset_date(cutoff, 1)

    assert db.prune_old_close(conn) == 0

    db.upsert_prices(
        conn,
        [
            ("AAPL", cutoff, 150.0),
            ("AAPL", recent_date, 160.0),
            ("NVDA", old_date, 800.0),
        ],
    )

    assert db.prune_old_close(conn) == 1
    assert db.prune_old_close(conn) == 0

    rows = conn.execute(
        "SELECT ticker, date, close FROM rs ORDER BY ticker, date"
    ).fetchall()
    assert rows == [
        ("AAPL", cutoff, 150.0),
        ("AAPL", recent_date, 160.0),
    ]


def test_prune_old_close_keeps_unrated_rs_raw_for_recovery(conn, monkeypatch):
    """An outage leaves rs_raw stored but rs_rating NULL. Retention must NOT
    delete those rows (they must stay re-rankable via backfill); it only nulls
    their old close. This is the recovery-deadline fix (D8)."""
    cutoff = _set_retention_now(monkeypatch)
    old_date = _offset_date(cutoff, -1)
    conn.execute(
        "INSERT INTO rs (ticker, date, close, rs_raw, rs_rating) VALUES (?, ?, ?, ?, ?)",
        ("OUT", old_date, 12.5, 0.42, None),
    )
    conn.commit()

    db.prune_old_close(conn)

    row = conn.execute(
        "SELECT close, rs_raw, rs_rating FROM rs WHERE ticker = ? AND date = ?",
        ("OUT", old_date),
    ).fetchone()
    assert row is not None      # survives: rs_raw is not deleted past cutoff
    assert row[1] == 0.42       # rs_raw intact -> still re-rankable
    assert row[0] is None       # old close nulled by retention
    assert row[2] is None
