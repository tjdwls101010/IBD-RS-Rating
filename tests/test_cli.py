"""Tests for CLI orchestration."""

import pytest

from ibd_rs import db
from ibd_rs import cli


class FakeConn:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def test_cmd_update_prunes_old_close_once_after_rs_calculation(monkeypatch, capsys):
    conn = FakeConn()
    calls = []

    monkeypatch.setattr(cli.db, "get_connection", lambda: conn)
    monkeypatch.setattr(cli.db, "init_db", lambda passed_conn: calls.append("init"))
    monkeypatch.setattr(
        cli.tickers_mod,
        "fetch_ticker_list",
        lambda passed_conn: calls.append("tickers") or ["AAPL"],
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
        lambda passed_conn, recalc_all: calls.append("rs") or 3,
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
        lambda passed_conn, tickers: calls.append("completeness") or {
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
    assert calls == ["init", "tickers", "prices", "splits", "rs", "prune", "completeness"]
    assert conn.closed
    assert "Pruned 2 old close records" in output
    assert "Latest trading day completeness" in output


def test_cmd_update_exits_1_when_latest_day_completeness_fails(monkeypatch, capsys):
    conn = db.get_connection(":memory:")
    db.init_db(conn)
    universe = [f"T{i}" for i in range(10)]
    db.upsert_prices(conn, [(ticker, "2026-04-17", 10.0) for ticker in universe])
    db.upsert_prices(conn, [(ticker, "2026-05-22", 20.0) for ticker in universe[:2]])

    monkeypatch.setattr(cli.db, "get_connection", lambda: conn)
    monkeypatch.setattr(cli.tickers_mod, "fetch_ticker_list", lambda passed_conn: universe)
    monkeypatch.setattr(cli.prices, "download_update", lambda tickers, passed_conn: {})
    monkeypatch.setattr(cli.splits, "detect_anomalous_changes", lambda passed_conn: [])
    monkeypatch.setattr(cli.rs, "calculate_and_store", lambda passed_conn, recalc_all: 0)
    monkeypatch.setattr(cli.db, "prune_old_close", lambda passed_conn: 0)

    with pytest.raises(SystemExit) as exc:
        cli.cmd_update(args=None)

    output = capsys.readouterr().out
    assert exc.value.code == 1
    assert "Latest trading day completeness" in output
    assert "Latest trading day: 2026-05-22" in output
    assert "Close coverage: 2/10" in output
    assert "Missing close count: 8" in output
    assert "Result: FAIL" in output
