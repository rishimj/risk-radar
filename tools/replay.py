"""Offline replay of real news through the live scoring path.

Shared by calibrate.py (report a fire rate) and seed_baseline.py (populate the
cold-start distribution). Both need the same thing: fetch a real corpus, score
it, bucket it into the same sliding windows the Flink job uses, and produce a
risk score per (ticker, window).

Using riskcore.windows and riskcore.risk here rather than reimplementing is the
point — a calibration number computed by different code than the running job is
worse than no number at all.
"""
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple
import logging
import os
import sys

from riskcore import config, feeds
from riskcore.entities import detect_companies, ticker_query
from riskcore.models import NewsArticle, parse_iso
from riskcore.risk import build_features, explode_mentions
from riskcore.windows import Window, assign_windows

log = logging.getLogger("replay")


@dataclass
class WindowScore:
    ticker: str
    window: Window
    risk: float
    mentions: int
    neg: int
    pos: int
    top_headline: str

    @property
    def end_epoch(self) -> float:
        return self.window.end_ms / 1000.0


# ---------------------------------------------------------------------------
# sentiment
# ---------------------------------------------------------------------------
def local_scorer() -> Tuple[Callable[[Sequence[str]], List[float]], str]:
    """Score without the enrichment container.

    Prefers FinBERT when torch is present, falls back to VADER. Calibration run
    with VADER is directionally useful but NOT authoritative — VADER reads
    "murder" and "jailed" as extreme financial negativity. Always re-run against
    FinBERT before locking a threshold.
    """
    try:
        import torch
        from transformers import AutoModelForSequenceClassification, AutoTokenizer

        name = os.getenv("SENTIMENT_MODEL", "ProsusAI/finbert")
        tok = AutoTokenizer.from_pretrained(name)
        model = AutoModelForSequenceClassification.from_pretrained(name)
        model.eval()
        labels = {v.lower(): k for k, v in model.config.id2label.items()}
        pos_i, neg_i = labels["positive"], labels["negative"]

        def score(texts: Sequence[str]) -> List[float]:
            enc = tok(list(texts), padding=True, truncation=True,
                      max_length=128, return_tensors="pt")
            with torch.no_grad():
                probs = torch.softmax(model(**enc).logits, dim=-1)
            return [float(v) for v in (probs[:, pos_i] - probs[:, neg_i])]

        return score, f"finbert:{name}"
    except Exception as exc:                           # noqa: BLE001
        log.info("FinBERT unavailable (%s); using VADER", exc)

    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    analyzer = SentimentIntensityAnalyzer()

    def score(texts: Sequence[str]) -> List[float]:
        return [float(analyzer.polarity_scores(t)["compound"]) for t in texts]

    return score, "vader"


# ---------------------------------------------------------------------------
# corpus
# ---------------------------------------------------------------------------
def fetch_corpus(hours: int, tickers: Optional[Sequence[str]] = None) -> List[NewsArticle]:
    """Pull the last `hours` of real news for the Mag 7."""
    tickers = list(tickers or config.MAG7)
    window = f"{hours}h" if hours <= 48 else "7d"

    feed_list = [
        feeds.Feed(name=f"gnews:{t}", url=feeds.google_news_url(ticker_query(t), window), ticker=t)
        for t in tickers
    ]
    return feeds.fetch_all(feed_list, max_age_hours=hours, stagger_seconds=0.3)


def enrich_locally(articles: Sequence[NewsArticle],
                   scorer: Callable[[Sequence[str]], List[float]],
                   batch_size: int = 32) -> List[dict]:
    """Attach sentiment + companies, mirroring what the enrichment service does."""
    docs: List[dict] = []
    for i in range(0, len(articles), batch_size):
        chunk = articles[i:i + batch_size]
        sentiments = scorer([a.title for a in chunk])
        for article, sentiment in zip(chunk, sentiments):
            docs.append({
                "article_id": article.article_id,
                "title": article.title,
                "url": article.url,
                "source": article.source,
                "published_at": article.published_at,
                "sentiment": float(sentiment),
                "companies": detect_companies(article.title, article.summary),
            })
    return docs


# ---------------------------------------------------------------------------
# windowing
# ---------------------------------------------------------------------------
def score_windows(docs: Iterable[dict],
                  size_s: Optional[int] = None,
                  slide_s: Optional[int] = None) -> List[WindowScore]:
    """Bucket mentions into sliding windows and score each, exactly as the job does."""
    size_ms = (size_s if size_s is not None else config.WINDOW_SIZE_SECONDS) * 1000
    slide_ms = (slide_s if slide_s is not None else config.WINDOW_SLIDE_SECONDS) * 1000

    buckets: Dict[Tuple[str, Window], list] = defaultdict(list)
    for doc in docs:
        for mention in explode_mentions(doc):
            for window in assign_windows(mention.event_ts, size_ms, slide_ms):
                buckets[(mention.ticker, window)].append(mention)

    out: List[WindowScore] = []
    for (ticker, window), mentions in buckets.items():
        start = datetime.fromtimestamp(window.start_ms / 1000, tz=timezone.utc)
        end = datetime.fromtimestamp(window.end_ms / 1000, tz=timezone.utc)
        feats = build_features(ticker, mentions, start, end)
        out.append(WindowScore(
            ticker=ticker, window=window, risk=feats.risk_score,
            mentions=feats.total_mentions, neg=feats.neg_count, pos=feats.pos_count,
            top_headline=feats.top_headline,
        ))
    out.sort(key=lambda w: (w.ticker, w.window.start_ms))
    return out


def build(hours: int, tickers: Optional[Sequence[str]] = None,
          size_s: Optional[int] = None, slide_s: Optional[int] = None):
    """fetch -> enrich -> window. Returns (window_scores, articles, model_name)."""
    scorer, model = local_scorer()
    articles = fetch_corpus(hours, tickers)
    docs = enrich_locally(articles, scorer)
    return score_windows(docs, size_s, slide_s), articles, model
