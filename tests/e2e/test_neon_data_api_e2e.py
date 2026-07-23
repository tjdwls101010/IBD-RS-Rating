"""Secrets-gated end-to-end checks against a Neon branch Data API."""

import ast
import os
from pathlib import Path

import pytest

from rs_rating.client import RS


_DATA_API_URL = os.environ.get("NEON_DATA_API_URL")
_AUTH_BASE_URL = os.environ.get("NEON_AUTH_BASE_URL")
_ANONYMOUS_TOKEN = os.environ.get("NEON_DATA_API_ANON_TOKEN")

_EXPECTED_PUBLIC_METHODS = {
    "get",
    "history",
    "top",
    "bottom",
    "filter",
    "compare",
    "reference",
    "movers",
    "dates",
    "staleness",
    "sectors",
    "industries",
    "sector_ranking",
    "industry_ranking",
    "sector_top",
    "industry_top",
}


@pytest.fixture(scope="module")
def client():
    if not _DATA_API_URL or not (_AUTH_BASE_URL or _ANONYMOUS_TOKEN):
        pytest.skip(
            "Neon E2E requires NEON_DATA_API_URL and either "
            "NEON_AUTH_BASE_URL or NEON_DATA_API_ANON_TOKEN"
        )
    client = RS(url=_DATA_API_URL, auth_url=_AUTH_BASE_URL)
    if _ANONYMOUS_TOKEN:
        client._token = _ANONYMOUS_TOKEN
        client._token_expires_at = float("inf")
    return client


@pytest.fixture(scope="module")
def live_values(client):
    available_dates = client.dates()
    assert isinstance(available_dates, dict)
    latest_date = available_dates.get("last")
    assert isinstance(latest_date, str) and latest_date

    top_rows = client.top(n=5, date=latest_date)
    assert isinstance(top_rows, list)
    assert top_rows, "Neon branch has no rated rows"
    ticker = top_rows[0].get("ticker")
    assert isinstance(ticker, str) and ticker

    sectors = client.sectors()
    assert isinstance(sectors, list)
    assert sectors, "Neon branch has no sector data"

    for sector in sectors:
        industries = client.industries(sector=sector)
        assert isinstance(industries, list)
        if industries:
            industry = industries[0]
            break
    else:
        pytest.fail("Neon branch has no industry data")

    return {
        "date": latest_date,
        "ticker": ticker,
        "sector": sector,
        "industry": industry,
    }


def test_e2e_references_every_public_rs_method():
    public_methods = {
        name
        for name, member in vars(RS).items()
        if callable(member) and not name.startswith("_")
    }
    syntax_tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
    referenced_methods = {
        node.func.attr
        for node in ast.walk(syntax_tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "client"
    }

    assert public_methods == _EXPECTED_PUBLIC_METHODS
    assert _EXPECTED_PUBLIC_METHODS <= referenced_methods


def test_get_against_neon_branch(client, live_values):
    response = client.get(live_values["ticker"], date=live_values["date"])

    assert isinstance(response, dict)


def test_history_against_neon_branch(client, live_values):
    response = client.history(
        live_values["ticker"],
        start=live_values["date"],
        end=live_values["date"],
        days=1,
    )

    assert isinstance(response, list)


def test_top_against_neon_branch(client, live_values):
    response = client.top(n=5, date=live_values["date"])

    assert isinstance(response, list)


def test_bottom_against_neon_branch(client, live_values):
    response = client.bottom(n=5, date=live_values["date"])

    assert isinstance(response, list)


def test_filter_against_neon_branch(client, live_values):
    response = client.filter(
        min_rating=1,
        max_rating=99,
        date=live_values["date"],
    )

    assert isinstance(response, list)


def test_compare_against_neon_branch(client, live_values):
    response = client.compare(
        [live_values["ticker"]],
        date=live_values["date"],
    )

    assert isinstance(response, list)


def test_reference_against_neon_branch(client, live_values):
    response = client.reference(date=live_values["date"])

    assert isinstance(response, list)


def test_movers_against_neon_branch(client, live_values):
    response = client.movers(days=1, n=5, direction="up")

    assert isinstance(response, list)


def test_dates_against_neon_branch(client):
    response = client.dates()

    assert isinstance(response, dict)
    assert {"first", "last"} <= response.keys()


def test_staleness_against_neon_branch(client):
    response = client.staleness()

    assert isinstance(response, dict)
    assert {
        "latest_rated_date",
        "latest_close_date",
        "is_stale",
        "lag_days",
    } <= response.keys()


def test_sectors_against_neon_branch(client):
    response = client.sectors()

    assert isinstance(response, list)


def test_industries_against_neon_branch(client, live_values):
    response = client.industries(sector=live_values["sector"])

    assert isinstance(response, list)


def test_sector_ranking_against_neon_branch(client, live_values):
    response = client.sector_ranking(date=live_values["date"])

    assert isinstance(response, list)


def test_industry_ranking_against_neon_branch(client, live_values):
    response = client.industry_ranking(
        date=live_values["date"],
        sector=live_values["sector"],
    )

    assert isinstance(response, list)


def test_sector_top_against_neon_branch(client, live_values):
    response = client.sector_top(
        live_values["sector"],
        n=5,
        date=live_values["date"],
    )

    assert isinstance(response, list)


def test_industry_top_against_neon_branch(client, live_values):
    response = client.industry_top(
        live_values["industry"],
        n=5,
        date=live_values["date"],
    )

    assert isinstance(response, list)
