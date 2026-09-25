"""riskcore.stages: the per-record logic both stream engines execute."""
from riskcore import stages
from riskcore.models import NewsArticle


class _Resp:
    def __init__(self, data, status=200):
        self._data, self.status_code = data, status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._data


class _Session:
    def __init__(self, resp=None, exc=None):
        self.resp, self.exc, self.bodies = resp, exc, []

    def post(self, url, json, timeout):
        self.bodies.append(json)
        if self.exc:
            raise self.exc
        return self.resp


def _article(aid):
    return NewsArticle(article_id=aid, title=f"t{aid}", url="u", source="s",
                       published_at="2026-07-27T12:00:00Z", feed="gnews", summary="x")


def test_enrich_batch_is_one_call_for_the_whole_batch():
    s = _Session(_Resp({"model": "finbert", "results": [
        {"article_id": "a", "sentiment": -0.5, "companies": [{"ticker": "TSLA"}]},
        {"article_id": "b", "sentiment": 0.2, "companies": []},
    ]}))
    out = stages.enrich_batch(s, [_article("a"), _article("b")])
    assert len(s.bodies) == 1 and len(s.bodies[0]["articles"]) == 2
    assert [e.article_id for e in out] == ["a", "b"]
    assert out[0].sentiment == -0.5 and out[0].model == "finbert" and out[0].feed == "gnews"


def test_enrich_batch_drops_articles_the_service_did_not_return():
    s = _Session(_Resp({"results": [{"article_id": "a", "sentiment": 0.1}]}))
    out = stages.enrich_batch(s, [_article("a"), _article("b")])
    assert [e.article_id for e in out] == ["a"]


def test_enrich_batch_never_raises():
    assert stages.enrich_batch(_Session(exc=ConnectionError("down")), [_article("a")]) == []
    assert stages.enrich_batch(_Session(_Resp({}, status=503)), [_article("a")]) == []


def test_decode_articles_skips_garbage():
    good = _article("a").to_json()
    assert [a.article_id for a in stages.decode_articles([good, "{not json"])] == ["a"]


# -- simulate must not poison the shared baseline ---------------------------
def _feats(simulated, score=1.8):
    from riskcore.models import RiskFeatures
    return RiskFeatures(ticker="TSLA", window_start="a", window_end="b", risk_score=1.0,
                        sentiment_score=-0.9, neg_count=10, pos_count=0,
                        total_mentions=10, alert_score=score, simulated=simulated)


def _store():
    import fakeredis
    from riskcore.baseline import BaselineStore
    r = fakeredis.FakeRedis()
    store = BaselineStore(r, window_hours=24, percentile_p=99, min_samples=50)
    for _ in range(100):
        store.record("TSLA", 0.3)
    return r, store


def test_simulated_windows_never_enter_the_baseline():
    r, store = _store()
    stages.write_features(r, store, _feats(simulated=True))
    assert store.sample_count("TSLA") == 100
    assert r.get("feat:TSLA:latest") is not None       # still shown on the dashboard
    stages.write_features(r, store, _feats(simulated=False))
    assert store.sample_count("TSLA") == 101


def test_repeated_simulations_keep_firing(monkeypatch):
    """Before the fix, each press dragged p99 up until the demo stopped working."""
    from riskcore import alerting
    r, store = _store()
    fired = []
    monkeypatch.setattr("riskcore.alerting.fan_out", fired.append)
    for _ in range(10):
        alerting.clear_cooldown(r, "TSLA")
        feats = _feats(simulated=True)
        stages.evaluate_and_alert(r, store, feats)
        stages.write_features(r, store, feats)
    assert len(fired) == 10


def test_alerts_from_simulated_windows_are_tagged(monkeypatch):
    r, store = _store()
    fired = []
    monkeypatch.setattr("riskcore.alerting.fan_out", fired.append)
    stages.evaluate_and_alert(r, store, _feats(simulated=True))
    assert fired[0].source == "simulated"
    alerting_clear = __import__("riskcore.alerting", fromlist=["x"]).clear_cooldown
    alerting_clear(r, "TSLA")
    stages.evaluate_and_alert(r, store, _feats(simulated=False))
    assert fired[1].source == "live"


def test_build_features_marks_windows_containing_simulated_articles():
    from datetime import datetime, timezone
    from riskcore.models import SIMULATED_SOURCE, CompanyMention
    from riskcore.risk import build_features
    t = datetime(2026, 7, 27, tzinfo=timezone.utc)
    real = CompanyMention("TSLA", "1", "t", "https://x/1", "Reuters", -0.5, "primary", 0)
    sim = CompanyMention("TSLA", "2", "t", "https://x/2", SIMULATED_SOURCE, -0.9, "primary", 0)
    assert build_features("TSLA", [real], t, t).simulated is False
    assert build_features("TSLA", [real, sim], t, t).simulated is True


# -- untrusted URLs -----------------------------------------------------------
def test_safe_url_allows_only_http_and_https():
    from riskcore.models import safe_url
    assert safe_url("https://example.com/a?b=1") == "https://example.com/a?b=1"
    assert safe_url("http://example.com") == "http://example.com"
    for bad in ("javascript:alert(1)", " JavaScript:alert(1)", "data:text/html,x",
                "vbscript:x", "//evil.com", "https://", "", None, 42):
        assert safe_url(bad) == "", bad


def test_headlines_strip_dangerous_urls_on_write_and_read():
    import fakeredis, json
    from riskcore import headlines
    r = fakeredis.FakeRedis()
    headlines.push(r, {"title": "x", "url": "javascript:alert(1)", "companies": []})
    r.lpush("recent:headlines", json.dumps({"title": "old", "url": "javascript:x"}))
    assert all(h["url"] == "" for h in headlines.recent(r))


def test_mentions_carry_only_safe_urls():
    from riskcore.risk import explode_mentions
    doc = {"article_id": "a", "title": "t", "url": "javascript:alert(1)",
           "published_at": "2026-07-27T12:00:00Z", "sentiment": -0.5,
           "companies": [{"ticker": "TSLA", "role": "primary"}]}
    assert explode_mentions(doc)[0].url == ""
