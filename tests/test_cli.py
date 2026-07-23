"""Tests for CLI orchestration."""

from types import SimpleNamespace

import pytest
import finvizfinance.util as finviz_util

from ibd_rs import db
from ibd_rs import cli
from ibd_rs import finviz
from ibd_rs.config import ANCHOR_TICKERS, REFERENCE_TICKERS
from ibd_rs.tickers import UniverseResult


class FakeConn:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def test_cmd_top_anchors_on_latest_rated_date_not_latest_rs_raw(
    monkeypatch, capsys
):
    conn = db.get_connection(":memory:")
    db.init_db(conn)
    db.upsert_rs(
        conn,
        [
            ("RATED", "2026-07-18", 0.5, 91),
            ("OUTAGE", "2026-07-22", 0.6, None),
        ],
    )
    monkeypatch.setattr(cli.db, "get_connection", lambda: conn)

    cli.cmd_top(SimpleNamespace(n=20))

    output = capsys.readouterr().out
    assert "IBD RS Ratings — 2026-07-18" in output
    assert "RATED" in output
    assert "OUTAGE" not in output


def test_cmd_export_anchors_on_latest_rated_date_not_latest_rs_raw(
    monkeypatch, capsys, tmp_path
):
    conn = db.get_connection(":memory:")
    db.init_db(conn)
    db.upsert_rs(
        conn,
        [
            ("RATED", "2026-07-18", 0.5, 91),
            ("OUTAGE", "2026-07-22", 0.6, None),
        ],
    )
    monkeypatch.setattr(cli.db, "get_connection", lambda: conn)
    monkeypatch.setattr(cli, "DATA_DIR", tmp_path)

    cli.cmd_export(SimpleNamespace())

    expected_path = tmp_path / "rs_ratings_2026-07-18.csv"
    assert expected_path.exists()
    output = capsys.readouterr().out
    exported = expected_path.read_text(encoding="utf-8")
    assert "rs_ratings_2026-07-18.csv" in output
    assert "RATED" in exported
    assert "OUTAGE" not in exported


def test_cmd_recalc_from_to_runs_rating_only_backfill(monkeypatch, capsys):
    conn = FakeConn()
    calls = []

    monkeypatch.setattr(cli.db, "get_connection", lambda: conn)
    monkeypatch.setattr(
        cli.rs,
        "backfill_ratings",
        lambda passed_conn, start, end: calls.append((passed_conn, start, end)) or 12,
    )
    monkeypatch.setattr(
        cli.rs,
        "calculate_and_store",
        lambda *_args, **_kwargs: pytest.fail("full recompute must not run"),
    )

    cli.cmd_recalc(
        SimpleNamespace(
            from_date="2026-06-30",
            to_date="2026-07-17",
            force_full=False,
        )
    )

    assert calls == [(conn, "2026-06-30", "2026-07-17")]
    assert conn.closed
    assert "12" in capsys.readouterr().out


def test_cmd_recalc_from_to_empty_range_exits_nonzero(monkeypatch, capsys):
    conn = FakeConn()
    calls = []

    monkeypatch.setattr(cli.db, "get_connection", lambda: conn)
    monkeypatch.setattr(
        cli.rs,
        "backfill_ratings",
        lambda passed_conn, start, end: calls.append((passed_conn, start, end)) or 0,
    )

    with pytest.raises(SystemExit) as exc:
        cli.cmd_recalc(
            SimpleNamespace(
                from_date="2026-06-30",
                to_date="2026-07-17",
                force_full=False,
            )
        )

    assert exc.value.code == 1
    assert calls == [(conn, "2026-06-30", "2026-07-17")]
    assert conn.closed
    assert "No stored rs_raw in [from,to] to re-rank." in capsys.readouterr().out


def test_cmd_recalc_reversed_dates_exits_nonzero(monkeypatch, capsys):
    conn = FakeConn()
    monkeypatch.setattr(cli.db, "get_connection", lambda: conn)
    monkeypatch.setattr(
        cli.rs,
        "backfill_ratings",
        lambda *_args, **_kwargs: pytest.fail("backfill must not run"),
    )

    with pytest.raises(SystemExit) as exc:
        cli.cmd_recalc(
            SimpleNamespace(
                from_date="2026-07-17",
                to_date="2026-06-30",
                force_full=False,
            )
        )

    assert exc.value.code == 1
    assert conn.closed
    assert "--from date must be on or before --to date" in capsys.readouterr().out


@pytest.mark.parametrize(
    ("from_date", "to_date"),
    [
        ("not-a-date", "2026-07-17"),
        ("2026-06-30", "2026-02-30"),
    ],
)
def test_cmd_recalc_invalid_dates_exit_nonzero(monkeypatch, capsys, from_date, to_date):
    conn = FakeConn()
    monkeypatch.setattr(cli.db, "get_connection", lambda: conn)
    monkeypatch.setattr(
        cli.rs,
        "backfill_ratings",
        lambda *_args, **_kwargs: pytest.fail("backfill must not run"),
    )

    with pytest.raises(SystemExit) as exc:
        cli.cmd_recalc(
            SimpleNamespace(
                from_date=from_date,
                to_date=to_date,
                force_full=False,
            )
        )

    assert exc.value.code == 1
    assert conn.closed
    assert "valid dates in YYYY-MM-DD format" in capsys.readouterr().out


def test_cmd_recalc_force_full_runs_guarded_full(monkeypatch):
    conn = FakeConn()
    calls = []

    monkeypatch.setattr(cli.db, "get_connection", lambda: conn)
    monkeypatch.setattr(
        cli.rs,
        "backfill_ratings",
        lambda *_args, **_kwargs: pytest.fail("rating-only backfill must not run"),
    )

    def calculate(passed_conn, **kwargs):
        calls.append((passed_conn, kwargs))
        return 7

    monkeypatch.setattr(cli.rs, "calculate_and_store", calculate)

    cli.cmd_recalc(
        SimpleNamespace(
            from_date=None,
            to_date=None,
            force_full=True,
        )
    )

    assert calls == [(conn, {"recalc_all": True, "force_full": True})]
    assert conn.closed


def test_cmd_recalc_no_args_exits_nonzero(monkeypatch, capsys):
    conn = FakeConn()
    monkeypatch.setattr(cli.db, "get_connection", lambda: conn)
    monkeypatch.setattr(
        cli.rs,
        "backfill_ratings",
        lambda *_args, **_kwargs: pytest.fail("rating-only backfill must not run"),
    )
    monkeypatch.setattr(
        cli.rs,
        "calculate_and_store",
        lambda *_args, **_kwargs: pytest.fail("full recompute must not run"),
    )

    with pytest.raises(SystemExit) as exc:
        cli.cmd_recalc(
            SimpleNamespace(
                from_date=None,
                to_date=None,
                force_full=False,
            )
        )

    assert exc.value.code == 1
    assert conn.closed
    output = capsys.readouterr().out
    assert "recalc --from <date> --to <date>" in output
    assert "recalc --force-full" in output


def test_cmd_update_prunes_old_close_once_after_rs_calculation(monkeypatch, capsys):
    conn = FakeConn()
    calls = []

    monkeypatch.setattr(cli.db, "get_connection", lambda: conn)
    monkeypatch.setattr(cli.db, "init_db", lambda passed_conn: calls.append("init"))
    monkeypatch.setattr(
        cli.db,
        "reconnect",
        lambda passed_conn: calls.append("reconnect") or passed_conn,
    )
    monkeypatch.setattr(
        cli.tickers_mod,
        "get_cached_universe",
        lambda passed_conn: calls.append("tickers") or (UniverseResult(["AAPL"], True, "ok"), passed_conn),
    )
    monkeypatch.setattr(
        cli.prices,
        "download_update",
        lambda tickers, passed_conn: calls.append("prices") or {},
    )
    monkeypatch.setattr(
        cli.splits,
        "detect_anomalous_changes",
        lambda passed_conn: calls.append("splits") or [],
    )
    monkeypatch.setattr(
        cli.rs,
        "calculate_and_store",
        lambda passed_conn, recalc_all, active_universe=None: calls.append("rs") or 3,
    )
    monkeypatch.setattr(
        cli.db,
        "prune_old_close",
        lambda passed_conn: calls.append("prune") or 2,
        raising=False,
    )
    monkeypatch.setattr(
        cli.db,
        "check_latest_trading_day_completeness",
        lambda passed_conn, tickers, min_universe_size=0: calls.append("completeness") or {
            "latest_date": "2026-05-22",
            "universe_size": 1,
            "close_coverage": 1,
            "missing_close_count": 0,
            "rating_coverage": 1,
            "missing_rating_count": 0,
            "coverage_ratio": 1.0,
            "threshold": 0.90,
            "is_complete": True,
            "reason": "complete",
        },
        raising=False,
    )

    cli.cmd_update(args=None)

    output = capsys.readouterr().out
    assert calls == [
        "init",
        "tickers",
        "reconnect",
        "prices",
        "splits",
        "reconnect",
        "rs",
        "prune",
        "completeness",
    ]
    assert conn.closed
    assert "Pruned 2 old close records" in output
    assert "Latest trading day completeness" in output


def test_cmd_update_reconnects_before_download_and_store(monkeypatch):
    conn = FakeConn()
    conn_a = FakeConn()
    conn_b = FakeConn()
    replacements = iter((conn_a, conn_b))
    calls = []
    reconnect_connections = []
    download_connections = []
    store_connections = []

    monkeypatch.setattr(cli.db, "get_connection", lambda: conn)
    monkeypatch.setattr(cli.db, "init_db", lambda passed_conn: calls.append("init"))
    monkeypatch.setattr(
        cli.tickers_mod,
        "get_cached_universe",
        lambda passed_conn: calls.append("tickers")
        or (UniverseResult(["AAPL"], True, "ok"), passed_conn),
    )

    def reconnect(passed_conn):
        calls.append("reconnect")
        reconnect_connections.append(passed_conn)
        return next(replacements)

    monkeypatch.setattr(cli.db, "reconnect", reconnect)

    def download_update(tickers, passed_conn):
        calls.append("download")
        download_connections.append(passed_conn)
        return {}

    monkeypatch.setattr(cli.prices, "download_update", download_update)
    monkeypatch.setattr(
        cli.splits,
        "detect_anomalous_changes",
        lambda passed_conn: calls.append("splits") or [],
    )

    def calculate_and_store(passed_conn, recalc_all, active_universe=None):
        calls.append("store")
        store_connections.append(passed_conn)
        return 1

    monkeypatch.setattr(cli.rs, "calculate_and_store", calculate_and_store)
    monkeypatch.setattr(
        cli.db,
        "prune_old_close",
        lambda passed_conn: calls.append("prune") or 0,
    )
    monkeypatch.setattr(
        cli.db,
        "check_latest_trading_day_completeness",
        lambda passed_conn, tickers, min_universe_size=0: {
            "latest_date": "2026-07-23",
            "universe_size": 1,
            "close_coverage": 1,
            "missing_close_count": 0,
            "rating_coverage": 1,
            "missing_rating_count": 0,
            "coverage_ratio": 1.0,
            "threshold": 0.90,
            "is_complete": True,
            "reason": "complete",
        },
    )

    cli.cmd_update(args=None)

    assert calls == [
        "init",
        "tickers",
        "reconnect",
        "download",
        "splits",
        "reconnect",
        "store",
        "prune",
    ]
    assert reconnect_connections == [conn, conn_a]
    assert download_connections == [conn_a]
    assert store_connections == [conn_b]
    assert conn_b.closed


def test_cmd_refresh_universe_reports_count_and_trust(monkeypatch, capsys):
    conn = FakeConn()
    monkeypatch.setattr(cli.db, "get_connection", lambda: conn)
    monkeypatch.setattr(cli.db, "init_db", lambda passed_conn: None)
    monkeypatch.setattr(
        cli.tickers_mod,
        "refresh_universe",
        lambda passed_conn: (
            UniverseResult(["AAPL", "MSFT"], True, "ok"),
            passed_conn,
        ),
    )

    cli.cmd_refresh_universe(args=None)

    assert conn.closed
    assert "2 tickers (trusted=True)" in capsys.readouterr().out


def test_cmd_refresh_universe_exits_nonzero_when_untrusted(monkeypatch, capsys):
    conn = FakeConn()
    monkeypatch.setattr(cli.db, "get_connection", lambda: conn)
    monkeypatch.setattr(cli.db, "init_db", lambda passed_conn: None)
    monkeypatch.setattr(
        cli.tickers_mod,
        "refresh_universe",
        lambda passed_conn: (
            UniverseResult(["AAPL"], False, "missing anchor tickers"),
            passed_conn,
        ),
    )

    with pytest.raises(SystemExit) as exc:
        cli.cmd_refresh_universe(args=None)

    assert exc.value.code == 1
    assert conn.closed
    output = capsys.readouterr().out
    assert "1 tickers (trusted=False)" in output
    assert "missing anchor tickers" in output


def test_cmd_update_never_calls_finviz_on_daily_path(monkeypatch, capsys):
    conn = db.get_connection(":memory:")
    db.init_db(conn)
    monkeypatch.setattr("ibd_rs.tickers.UNIVERSE_FLOOR", 5)
    cached = ANCHOR_TICKERS + REFERENCE_TICKERS
    db.set_meta(conn, "ticker_list", ",".join(cached))
    downloaded = []

    def forbidden(*_args, **_kwargs):
        raise AssertionError("daily update must not call Finviz")

    monkeypatch.setattr(finviz, "fetch_screener_records", forbidden)
    monkeypatch.setattr(finviz_util, "web_scrap", forbidden)
    monkeypatch.setattr(
        cli.tickers_mod.nasdaq_trader,
        "fetch_common_stock_tickers",
        lambda: pytest.fail("valid cache must not use the emergency fallback"),
    )
    monkeypatch.setattr(cli.db, "get_connection", lambda: conn)
    monkeypatch.setattr(
        cli.prices,
        "download_update",
        lambda ticker_list, passed_conn: downloaded.append(ticker_list) or {},
    )
    monkeypatch.setattr(cli.splits, "detect_anomalous_changes", lambda passed_conn: [])
    monkeypatch.setattr(
        cli.rs,
        "calculate_and_store",
        lambda passed_conn, recalc_all, active_universe=None: 1,
    )
    monkeypatch.setattr(cli.db, "prune_old_close", lambda passed_conn: 0)
    monkeypatch.setattr(
        cli.db,
        "check_latest_trading_day_completeness",
        lambda passed_conn, ticker_list, min_universe_size=0: {
            "latest_date": "2026-07-23",
            "universe_size": len(ticker_list),
            "close_coverage": len(ticker_list),
            "missing_close_count": 0,
            "rating_coverage": len(ticker_list),
            "missing_rating_count": 0,
            "coverage_ratio": 1.0,
            "threshold": 0.90,
            "is_complete": True,
            "reason": "complete",
        },
    )

    cli.cmd_update(args=None)

    assert downloaded == [cached]
    assert "Update complete!" in capsys.readouterr().out


def test_cmd_update_exits_1_when_latest_day_completeness_fails(monkeypatch, capsys):
    conn = db.get_connection(":memory:")
    db.init_db(conn)
    universe = [f"T{i}" for i in range(10)]
    db.upsert_prices(conn, [(ticker, "2026-04-17", 10.0) for ticker in universe])
    db.upsert_prices(conn, [(ticker, "2026-05-22", 20.0) for ticker in universe[:2]])

    monkeypatch.setattr(cli.db, "get_connection", lambda: conn)
    monkeypatch.setattr(
        cli.tickers_mod, "get_cached_universe",
        lambda passed_conn: (UniverseResult(universe, True, "ok"), passed_conn),
    )
    monkeypatch.setattr(cli.prices, "download_update", lambda tickers, passed_conn: {})
    monkeypatch.setattr(cli.splits, "detect_anomalous_changes", lambda passed_conn: [])
    monkeypatch.setattr(cli.rs, "calculate_and_store", lambda passed_conn, recalc_all, active_universe=None: 0)
    monkeypatch.setattr(cli.db, "prune_old_close", lambda passed_conn: 0)

    with pytest.raises(SystemExit) as exc:
        cli.cmd_update(args=None)

    output = capsys.readouterr().out
    assert exc.value.code == 1
    assert "Latest trading day completeness" in output
    assert "Latest trading day: 2026-05-22" in output
    assert "Close coverage: 2/3000" in output  # denominator floored to UNIVERSE_FLOOR, not the 10-ticker test universe
    assert "Missing close count: 2998" in output
    assert "Result: FAIL" in output


def test_cmd_update_exits_1_when_universe_fetch_is_degraded(monkeypatch, capsys):
    conn = db.get_connection(":memory:")
    db.init_db(conn)
    universe = [f"T{i}" for i in range(10)]
    db.upsert_prices(conn, [(ticker, "2026-05-22", 20.0) for ticker in universe])

    monkeypatch.setattr(cli.db, "get_connection", lambda: conn)
    monkeypatch.setattr(
        cli.tickers_mod, "get_cached_universe",
        lambda passed_conn: (UniverseResult(universe, False, "fetched 55 tickers, below absolute floor 3000"), passed_conn),
    )
    monkeypatch.setattr(cli.prices, "download_update", lambda tickers, passed_conn: {})
    monkeypatch.setattr(cli.splits, "detect_anomalous_changes", lambda passed_conn: [])
    monkeypatch.setattr(cli.rs, "calculate_and_store", lambda passed_conn, recalc_all, active_universe=None: 0)
    monkeypatch.setattr(cli.db, "prune_old_close", lambda passed_conn: 0)

    with pytest.raises(SystemExit) as exc:
        cli.cmd_update(args=None)

    output = capsys.readouterr().out
    assert exc.value.code == 1
    assert "WARNING: universe fetch degraded" in output
    assert "below absolute floor" in output


def test_cmd_init_refuses_on_populated_db_before_download(monkeypatch, capsys, tmp_path):
    conn = db.get_connection(":memory:")
    db.init_db(conn)
    db.upsert_rs(conn, [("AAPL", "2026-06-30", 0.44, 91)])
    calls = []

    def forbidden(name):
        def call(*_args, **_kwargs):
            calls.append(name)
            raise AssertionError(f"{name} must not be called")

        return call

    monkeypatch.setattr(cli.db, "get_connection", lambda: conn)
    monkeypatch.setattr(cli, "DATA_DIR", tmp_path)
    monkeypatch.setattr(cli.tickers_mod, "refresh_universe", forbidden("refresh_universe"))
    monkeypatch.setattr(cli.prices, "download_initial", forbidden("download_initial"))
    monkeypatch.setattr(cli.prices, "download_update", forbidden("download_update"))

    with pytest.raises(SystemExit) as exc:
        cli.cmd_init(SimpleNamespace(force_full=False))

    output = capsys.readouterr().out
    assert exc.value.code == 1
    assert calls == []
    assert "Refusing init: database already has RS history" in output
    assert "Step 1/4" not in output

def test_cmd_init_refuses_to_run_on_an_untrusted_universe(monkeypatch, capsys, tmp_path):
    conn = db.get_connection(":memory:")

    monkeypatch.setattr(cli.db, "get_connection", lambda: conn)
    monkeypatch.setattr(cli, "DATA_DIR", tmp_path)
    monkeypatch.setattr(
        cli.tickers_mod, "refresh_universe",
        lambda passed_conn: (UniverseResult(["T0"], False, "fetched 55 tickers, below absolute floor 3000"), passed_conn),
    )

    with pytest.raises(SystemExit) as exc:
        cli.cmd_init(args=None)

    output = capsys.readouterr().out
    assert exc.value.code == 1
    assert "Refusing to init from an untrusted universe" in output


def test_cmd_update_exits_1_when_rating_coverage_is_zero_despite_full_close(monkeypatch, capsys):
    """BUG3: a day with full close coverage but no ratings (the silent-outage
    shape) must fail the build, not exit green. UNIVERSE_FLOOR is lowered so a
    small test universe can clear the close gate while ratings stay empty."""
    monkeypatch.setattr(cli, "UNIVERSE_FLOOR", 10)
    conn = db.get_connection(":memory:")
    db.init_db(conn)
    universe = [f"T{i}" for i in range(10)]
    # Full close coverage on the latest day, but zero ratings written.
    db.upsert_prices(conn, [(ticker, "2026-06-30", 20.0) for ticker in universe])

    monkeypatch.setattr(cli.db, "get_connection", lambda: conn)
    monkeypatch.setattr(
        cli.tickers_mod, "get_cached_universe",
        lambda passed_conn: (UniverseResult(universe, True, "ok"), passed_conn),
    )
    monkeypatch.setattr(cli.prices, "download_update", lambda tickers, passed_conn: {})
    monkeypatch.setattr(cli.splits, "detect_anomalous_changes", lambda passed_conn: [])
    monkeypatch.setattr(cli.rs, "calculate_and_store", lambda passed_conn, recalc_all, active_universe=None: 0)
    monkeypatch.setattr(cli.db, "prune_old_close", lambda passed_conn: 0)

    with pytest.raises(SystemExit) as exc:
        cli.cmd_update(args=None)

    output = capsys.readouterr().out
    assert exc.value.code == 1
    assert "Close coverage: 10/10" in output
    assert "RS rating coverage: 0/10" in output
    assert "rating_coverage_below_threshold" in output
