"""Per-record stage logic shared by both stream engines.

RiskRadar runs on either of two engines that execute the same topology:

  * flink/job/news_job.py       PyFlink on a Flink cluster (the reference)
  * services/processor/         a single pure-Python process (the "lite" engine)

Everything that decides an OUTCOME (what enrichment returns, what a headline row
looks like, whether a window alerts, what gets written to Redis) lives here, so
the two engines cannot drift. The engines only own scheduling: batching, event
time, and window firing.
"""
from typing import Dict, List, Optional, Sequence
import json
import logging

from . import config
from .models import Alert, EnrichedArticle, NewsArticle, RiskFeatures

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# enrichment
# ---------------------------------------------------------------------------
def enrich_batch(session, articles: Sequence[NewsArticle]) -> List[EnrichedArticle]:
    """One HTTP call to the enrichment service for a micro-batch.

    Never raises: an enrichment outage drops the batch with an error log rather
    than killing the engine. Articles the service did not return are dropped.
    """
    if not articles:
        return []

    body = {"articles": [
        {"article_id": a.article_id, "title": a.title, "summary": a.summary}
        for a in articles
    ]}
    try:
        resp = session.post(
            f"{config.ENRICHMENT_URL}/v1/enrich/batch",
            json=body, timeout=config.ENRICH_TIMEOUT_SECONDS,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:                           # noqa: BLE001 - never kill the engine
        log.error("enrichment failed for %d articles: %s", len(articles), exc)
        return []

    results = {r["article_id"]: r for r in data.get("results", [])}
    model = data.get("model", "")

    out: List[EnrichedArticle] = []
    for article in articles:
        payload = results.get(article.article_id)
        if payload is None:
            continue
        out.append(EnrichedArticle(
            article_id=article.article_id,
            title=article.title,
            url=article.url,
            source=article.source,
            published_at=article.published_at,
            sentiment=float(payload.get("sentiment", 0.0)),
            companies=payload.get("companies", []),
            model=model,
            feed=article.feed,
        ))
    return out


def decode_articles(raw_values: Sequence[str]) -> List[NewsArticle]:
    out: List[NewsArticle] = []
    for raw in raw_values:
        try:
            out.append(NewsArticle.from_json(raw))
        except Exception:                              # noqa: BLE001
            log.warning("undecodable article dropped")
    return out


# ---------------------------------------------------------------------------
# headlines
# ---------------------------------------------------------------------------
def headline_row(doc: Dict) -> Dict:
    """The subset of an enriched article the dashboard's ring buffer stores."""
    return {
        "article_id": doc.get("article_id"),
        "title": doc.get("title"),
        "url": doc.get("url"),
        "source": doc.get("source"),
        "published_at": doc.get("published_at"),
        "sentiment": doc.get("sentiment"),
        "companies": doc.get("companies", []),
    }


def write_headline(redis_client, doc: Dict) -> None:
    from . import headlines
    headlines.push(redis_client, headline_row(doc))


# ---------------------------------------------------------------------------
# window outputs
# ---------------------------------------------------------------------------
def evaluate_and_alert(redis_client, store, feats: RiskFeatures) -> Optional[Alert]:
    """Decide against the ticker's baseline and fan out. Returns the alert, if any.

    Must run BEFORE write_features for the same window: write_features folds
    this window into the baseline, and a window cannot be an outlier relative to
    a sample set it already belongs to.
    """
    from . import alerting

    decision = store.evaluate(feats.ticker, feats.alert_score, feats.total_mentions)
    if not decision.should_alert:
        return None

    if alerting.in_cooldown(redis_client, feats.ticker):
        log.info("%s over baseline but in cooldown", feats.ticker)
        return None

    alert = alerting.build_alert(feats, decision.threshold or 0.0)
    alerting.fan_out(alert)
    alerting.mark_sent(redis_client, feats.ticker)
    log.info("ALERT %s score=%.3f baseline=%.3f n=%d",
             feats.ticker, feats.alert_score, decision.threshold or 0.0,
             decision.samples)
    return alert


def write_features(redis_client, store, feats: RiskFeatures) -> None:
    """Persist the window for the dashboard and feed the ticker's baseline."""
    payload = json.dumps(feats.to_dict(), separators=(",", ":"))

    pipe = redis_client.pipeline()
    pipe.set(f"feat:{feats.ticker}:latest", payload)
    pipe.setex(f"feat:{feats.ticker}:{feats.window_end}", 7 * 86400, payload)
    pipe.execute()

    # Every window feeds the baseline, including quiet ones — otherwise the
    # distribution would only contain spikes and the percentile would be
    # meaningless.
    store.record(feats.ticker, feats.alert_score)
