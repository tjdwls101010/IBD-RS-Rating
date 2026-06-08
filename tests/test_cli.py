"""Tests for CLI orchestration."""

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

    cli.cmd_update(args=None)

    output = capsys.readouterr().out
    assert calls == ["init", "tickers", "prices", "splits", "rs", "prune"]
    assert conn.closed
    assert "Pruned 2 old close records" in output
