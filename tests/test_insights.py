"""UI read models: sparkline history, counters, the public overview."""
import json
from datetime import datetime, timezone

import fakeredis
import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient        # noqa: E402

from riskcore import stages                       # noqa: E402
from riskcore.baseline import BaselineStore       # noqa: E402
from riskcore.models import RiskFeatures          # noqa: E402

from conftest import load_service_module          # noqa: E402

webapp = load_service_module("webapp", "app")
insights = webapp.insights


def feats(i, ticker="TSLA", simulated=False):
    return RiskFeatures(ticker=ticker, window_start="a", window_end=f"2026-09-25T00:{i:02d}:00Z",
                        risk_score=0.5, sentiment_score=-0.1, neg_count=2, pos_count=1,
                        total_mentions=3, alert_score=0.4 + i / 100, simulated=simulated)


def test_history_is_capped_and_oldest_first():
    r = fakeredis.FakeRedis()
    store = BaselineStore(r)
    for i in range(stages.HISTORY_LEN + 10):
        f = feats(0)
        f.window_end = f"2026-09-25T{i // 60:02d}:{i % 60:02d}:00Z"   # strictly increasing
        stages.write_features(r, store, f)
    assert r.llen("hist:TSLA") == stages.HISTORY_LEN
    h = insights.history(r, "TSLA", 5)
    assert [p["t"] for p in h] == sorted(p["t"] for p in h)           # oldest first
    assert h[-1]["t"] == f"2026-09-25T{(stages.HISTORY_LEN + 9) // 60:02d}:{(stages.HISTORY_LEN + 9) % 60:02d}:00Z"


def test_counters_track_the_pipeline():
    r = fakeredis.FakeRedis()
    store = BaselineStore(r)
    stages.write_features(r, store, feats(1))
    stages.write_headline(r, {"title": "t", "url": "https://x", "companies": []})
    assert int(r.get("stats:windows")) == 1 and int(r.get("stats:articles")) == 1


def test_histogram_bins_cover_every_value():
    vals = [0.1, 0.2, 0.2, 0.9, 1.5]
    bins = insights.histogram(vals, bins=10, hi=1.0)
    assert sum(b["count"] for b in bins) == len(vals)
    assert bins[-1]["count"] == 2                  # 0.9 and the overflow 1.5


@pytest.fixture
def client(dynamo, monkeypatch):
    fake = fakeredis.FakeRedis()
    monkeypatch.setattr(webapp, "rds", fake)
    monkeypatch.setattr(webapp, "baselines", BaselineStore(fake, min_samples=5))
    webapp._overview_cache["value"] = None
    with TestClient(webapp.app) as c:
        c.fake_redis = fake
        yield c


def test_public_overview_needs_no_login_and_exposes_no_users(client):
    store = webapp.baselines
    for i in range(20):
        store.record("TSLA", 0.3 + i * 0.02)
    stages.write_features(client.fake_redis, store, feats(5))
    webapp._overview_cache["value"] = None
    body = client.get("/api/public/overview").json()
    assert {t["ticker"] for t in body["tickers"]} >= {"TSLA", "AAPL"}
    tsla = next(t for t in body["tickers"] if t["ticker"] == "TSLA")
    assert tsla["history"] and tsla["baseline_ready"]
    assert body["distribution"]["samples"] >= 20 and body["distribution"]["cut"]
    text = json.dumps(body)
    assert "@" not in text and "user_id" not in text and "password" not in text


def test_dashboard_risk_includes_history(client):
    client.post("/signup", data={"email": "h@example.com", "password": "hunter2hunter2"})
    client.post("/api/watchlist", json={"tickers": ["TSLA"]})
    stages.write_features(client.fake_redis, webapp.baselines, feats(7))
    row = client.get("/api/risk").json()["risk"][0]
    assert row["history"][-1]["t"].endswith("00:07:00Z")


def test_landing_hides_stories_that_only_mention_a_company(client):
    client.fake_redis.lpush("recent:headlines", json.dumps({
        "title": "Older dogs can develop dementia", "url": "https://x/d", "source": "facebook.com",
        "companies": [{"ticker": "META", "role": "mentioned"}]}))
    client.fake_redis.lpush("recent:headlines", json.dumps({
        "title": "Tesla cuts prices again", "url": "https://x/t", "source": "Reuters",
        "companies": [{"ticker": "TSLA", "role": "primary"}]}))
    html = client.get("/").text
    assert "Tesla cuts prices again" in html and "Older dogs" not in html


def test_standalone_backfills_history_from_window_keys(monkeypatch):
    """An upgrade must show existing windows immediately, oldest first."""
    import importlib.util, sys
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    monkeypatch.setenv("SECRET_KEY", "x" * 40)
    spec = importlib.util.spec_from_file_location(
        "_node_under_test", root / "services/standalone/riskradar_node.py")
    node_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(node_mod)

    r = fakeredis.FakeRedis()
    for i in (3, 1, 2):
        f = feats(i)
        r.set(f"feat:TSLA:{f.window_end}", json.dumps(f.to_dict()))
    node = node_mod.Node.__new__(node_mod.Node)
    node.redis = r
    node.backfill_history()
    h = insights.history(r, "TSLA", 10)
    assert [p["t"] for p in h] == [feats(i).window_end for i in (1, 2, 3)]


def test_syndicated_duplicates_are_listed_once():
    from riskcore import headlines
    r = fakeredis.FakeRedis()
    for url in ("https://a/1", "https://b/2"):
        headlines.push(r, {"title": "Wall Street ends higher as investors buy AI stocks",
                           "url": url, "companies": []})
    headlines.push(r, {"title": "Something else", "url": "https://c/3", "companies": []})
    assert [h["title"] for h in headlines.recent(r)] == [
        "Something else", "Wall Street ends higher as investors buy AI stocks"]
