"""Enrichment API contract.

Runs on whatever sentiment tier is installed — in CI that is usually VADER,
since torch is not a test dependency. The assertions are therefore about the
contract and the entity matching, not about specific model scores.
"""
import sys
from pathlib import Path

import pytest

fastapi = pytest.importorskip("fastapi", reason="enrichment service deps not installed")
from fastapi.testclient import TestClient    # noqa: E402

from conftest import load_service_module     # noqa: E402

service = load_service_module("enrichment", "app")


@pytest.fixture(scope="module")
def client():
    with TestClient(service.app) as c:
        yield c


def enrich(client, *titles):
    payload = {"articles": [{"article_id": str(i), "title": t} for i, t in enumerate(titles)]}
    resp = client.post("/v1/enrich/batch", json=payload)
    assert resp.status_code == 200
    return resp.json()


# -- health / stats ---------------------------------------------------------
def test_healthz_reports_the_active_tier(client):
    body = client.get("/healthz").json()
    assert body["status"] == "ok"
    assert body["tier"] in {"finbert", "finvader", "vader", "none"}


def test_stats_exposes_tier_for_the_dashboard_pill(client):
    body = client.get("/stats").json()
    assert "tier" in body and "avg_batch_ms" in body


def test_trace_id_header_is_echoed(client):
    resp = client.post("/v1/enrich/batch", json={"articles": []},
                       headers={"x-trace-id": "abc123"})
    assert resp.headers["x-trace-id"] == "abc123"


# -- contract ---------------------------------------------------------------
def test_empty_batch_is_not_an_error(client):
    body = enrich(client)
    assert body["results"] == [] and body["total_articles"] == 0


def test_one_result_per_input_article_in_order(client):
    body = enrich(client, "Tesla recalls vehicles", "Nvidia raises guidance", "Nothing relevant")
    assert [r["article_id"] for r in body["results"]] == ["0", "1", "2"]


def test_sentiment_is_within_range(client):
    body = enrich(client, "Tesla plunges on recall", "Apple hits record high")
    assert all(-1.0 <= r["sentiment"] <= 1.0 for r in body["results"])


def test_companies_are_detected(client):
    body = enrich(client, "Tesla recalls 400,000 vehicles")
    assert body["results"][0]["companies"] == [{"ticker": "TSLA", "role": "primary"}]


def test_unrelated_article_matches_nothing(client):
    body = enrich(client, "Pineapple farming subsidies expanded")
    assert body["results"][0]["companies"] == []


def test_multi_ticker_article(client):
    body = enrich(client, "Nvidia and Tesla both slide in afternoon trading")
    tickers = {c["ticker"] for c in body["results"][0]["companies"]}
    assert tickers == {"NVDA", "TSLA"}


def test_batch_is_capped(client):
    payload = {"articles": [{"article_id": str(i), "title": "Tesla news"}
                            for i in range(service.MAX_BATCH + 25)]}
    body = client.post("/v1/enrich/batch", json=payload).json()
    assert body["total_articles"] == service.MAX_BATCH


def test_blank_titles_do_not_crash_the_batch(client):
    body = enrich(client, "", "   ", "Tesla recalls vehicles")
    assert len(body["results"]) == 3
    assert body["results"][2]["companies"] == [{"ticker": "TSLA", "role": "primary"}]


def test_scoring_failure_degrades_to_neutral(client, monkeypatch):
    """A model blowup must not fail the batch — the Flink operator depends on it."""
    monkeypatch.setattr(service.engine, "score_batch",
                        lambda texts: (_ for _ in ()).throw(RuntimeError("boom")))
    body = enrich(client, "Tesla recalls vehicles")
    assert body["results"][0]["sentiment"] == 0.0
    assert body["results"][0]["companies"] == [{"ticker": "TSLA", "role": "primary"}]
