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
