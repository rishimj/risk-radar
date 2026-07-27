"""Webapp: auth, watchlist scoping, the risk API, and simulate guards."""
import json
import sys
from pathlib import Path

import fakeredis
import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient    # noqa: E402

from conftest import load_service_module          # noqa: E402


@pytest.fixture
def client(dynamo, monkeypatch):
    webapp = load_service_module("webapp", "app")

    fake = fakeredis.FakeRedis()
    monkeypatch.setattr(webapp, "rds", fake)
    from riskcore.baseline import BaselineStore
    monkeypatch.setattr(webapp, "baselines", BaselineStore(fake))
    monkeypatch.setattr(webapp.simulate_mod.kafka, "send", lambda *a, **k: None)
    monkeypatch.setattr(webapp.simulate_mod.kafka, "flush", lambda *a, **k: None)

    with TestClient(webapp.app) as c:
        c.fake_redis = fake
        yield c


def register(client, email="a@example.com", password="hunter2hunter2"):
    return client.post("/signup", data={"email": email, "password": password},
                       follow_redirects=False)


# -- auth -------------------------------------------------------------------
def test_signup_creates_session_and_redirects_to_onboarding(client):
    resp = register(client)
    assert resp.status_code == 303
    assert resp.headers["location"] == "/onboarding"
    assert "riskradar_session" in resp.cookies


def test_duplicate_email_rejected(client):
    register(client)
    resp = register(client)
    assert resp.status_code == 400
    assert "already registered" in resp.text


def test_short_password_rejected(client):
    resp = client.post("/signup", data={"email": "b@example.com", "password": "short"})
    assert resp.status_code == 400


def test_login_with_wrong_password_fails(client):
    register(client)
    client.cookies.clear()
    resp = client.post("/login", data={"email": "a@example.com", "password": "wrong"})
    assert resp.status_code == 401


def test_login_round_trip(client):
    register(client)
    client.cookies.clear()
    resp = client.post("/login",
                       data={"email": "a@example.com", "password": "hunter2hunter2"},
                       follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers["location"] == "/dashboard"


def test_password_is_not_stored_in_plaintext(client, dynamo):
    register(client)
    store = load_service_module("webapp", "store")
    user = store.user_by_email("a@example.com")
    assert "hunter2hunter2" not in json.dumps(user)
    assert user["password_hash"].startswith("$2")


def test_api_requires_auth(client):
    assert client.get("/api/risk").status_code == 401
    assert client.get("/api/alerts").status_code == 401


def test_public_endpoints_need_no_auth(client):
    assert client.get("/").status_code == 200
    assert client.get("/healthz").status_code == 200
    assert client.get("/api/headlines").status_code == 200


# -- watchlist --------------------------------------------------------------
def test_watchlist_round_trip(client):
    register(client)
    resp = client.post("/api/watchlist", json={"tickers": ["TSLA", "NVDA"]})
    assert resp.json()["watching"] == ["NVDA", "TSLA"]


def test_unknown_ticker_rejected(client):
    register(client)
    resp = client.post("/api/watchlist", json={"tickers": ["TSLA", "FAKE"]})
    assert resp.status_code == 400


def test_watchlist_replaces_rather_than_appends(client):
    register(client)
    client.post("/api/watchlist", json={"tickers": ["TSLA", "NVDA"]})
    resp = client.post("/api/watchlist", json={"tickers": ["AAPL"]})
    assert resp.json()["watching"] == ["AAPL"]


# -- risk api ---------------------------------------------------------------
def test_risk_returns_a_row_per_watched_ticker(client):
    register(client)
    client.post("/api/watchlist", json={"tickers": ["TSLA", "NVDA"]})
    body = client.get("/api/risk").json()
    assert {r["ticker"] for r in body["risk"]} == {"TSLA", "NVDA"}


def test_risk_reports_warming_up_before_the_baseline_exists(client):
    register(client)
    client.post("/api/watchlist", json={"tickers": ["TSLA"]})
    row = client.get("/api/risk").json()["risk"][0]
    assert row["baseline_ready"] is False
    assert row["baseline"] is None


def test_risk_exposes_the_per_ticker_cut_once_warm(client):
    register(client)
    client.post("/api/watchlist", json={"tickers": ["TSLA"]})

    webapp = load_service_module("webapp", "app")
    for i in range(80):
        webapp.baselines.record("TSLA", 0.2 + (i % 10) * 0.01)
    webapp.rds.set("feat:TSLA:latest", json.dumps({
        "ticker": "TSLA", "risk_score": 0.95, "sentiment_score": -0.7,
        "total_mentions": 4, "window_end": "2026-07-27T12:15:00Z",
        "top_headline": "Tesla recalls vehicles", "top_url": "https://x/1",
    }))

    row = client.get("/api/risk").json()["risk"][0]
    assert row["baseline_ready"] is True
    assert 0 < row["baseline"] < 0.95
    assert row["risk_score"] == 0.95


def test_risk_is_scoped_to_the_caller(client):
    register(client, "one@example.com")
    client.post("/api/watchlist", json={"tickers": ["TSLA"]})
    client.cookies.clear()

    register(client, "two@example.com")
    client.post("/api/watchlist", json={"tickers": ["AAPL"]})
    tickers = {r["ticker"] for r in client.get("/api/risk").json()["risk"]}
    assert tickers == {"AAPL"}


# -- simulate ---------------------------------------------------------------
def test_simulate_requires_the_ticker_to_be_watched(client):
    register(client)
    client.post("/api/watchlist", json={"tickers": ["AAPL"]})
    resp = client.post("/api/simulate", json={"ticker": "TSLA"})
    assert resp.status_code == 400
    assert "not on your watchlist" in resp.json()["detail"]


def test_simulate_publishes_both_batches(client):
    register(client)
    client.post("/api/watchlist", json={"tickers": ["TSLA"]})
    body = client.post("/api/simulate", json={"ticker": "TSLA"}).json()
    assert body["ticker"] == "TSLA"
    assert body["crisis_articles"] == 10
    assert body["filler_articles"] == 5


def test_simulate_is_rate_limited(client):
    register(client)
    client.post("/api/watchlist", json={"tickers": ["TSLA"]})
    assert client.post("/api/simulate", json={"ticker": "TSLA"}).status_code == 200
    assert client.post("/api/simulate", json={"ticker": "TSLA"}).status_code == 429


def test_simulate_clears_the_cooldown_so_a_repeat_demo_fires(client):
    from riskcore import alerting
    register(client)
    client.post("/api/watchlist", json={"tickers": ["TSLA"]})
    alerting.mark_sent(client.fake_redis, "TSLA")
    assert alerting.in_cooldown(client.fake_redis, "TSLA") is True
    client.post("/api/simulate", json={"ticker": "TSLA"})
    assert alerting.in_cooldown(client.fake_redis, "TSLA") is False


# -- slack ------------------------------------------------------------------
def test_slack_url_is_validated(client):
    register(client)
    assert client.post("/api/slack", json={"webhook_url": "http://evil.example"}).status_code == 400
    ok = client.post("/api/slack",
                     json={"webhook_url": "https://hooks.slack.com/services/T/B/x"})
    assert ok.status_code == 200 and ok.json()["configured"] is True


def test_slack_test_without_a_webhook_is_rejected(client):
    register(client)
    assert client.post("/api/slack/test").status_code == 400


# -- pages ------------------------------------------------------------------
def test_dashboard_redirects_to_onboarding_without_a_watchlist(client):
    register(client)
    resp = client.get("/dashboard", follow_redirects=False)
    assert resp.status_code == 303 and resp.headers["location"] == "/onboarding"


def test_dashboard_renders_once_watching(client):
    register(client)
    client.post("/api/watchlist", json={"tickers": ["TSLA"]})
    resp = client.get("/dashboard")
    assert resp.status_code == 200 and "Risk by ticker" in resp.text


def test_landing_renders_for_anonymous_visitors(client):
    assert "Know when the news turns" in client.get("/").text
