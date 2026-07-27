"""Window risk scoring.

The aggregation is ported from the old repo's FeatureAggregator
(news_processing_job.py:160-226) and produces the same 0-1 gauge that the
dashboard displays.

IMPORTANT — this score is NOT compared against a fixed threshold to decide
whether to alert.  Replaying 672 real Mag 7 headlines through this exact math
with the old 0.7 cut fired on 202/1499 windows (13.5%), because
`sentiment_risk = (1 - s) / 2` maps perfectly neutral news to 0.5 and the
all-negative negative_bias of 1.25 pushes a *single* mildly negative headline
(n=1, s=-0.30) to 0.810.  Alerting lives in baseline.py and is relative to each
ticker's own trailing distribution.  See the plan's Calibration section.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence

from .models import CompanyMention, RiskFeatures, iso, parse_iso


@dataclass
class Aggregation:
    neg_count: int = 0
    pos_count: int = 0
    sentiment_ewm: float = 0.0
    count: int = 0
    alpha: float = 0.1

    def add(self, sentiment: float) -> None:
        if sentiment < 0:
            self.neg_count += 1
        elif sentiment > 0:
            self.pos_count += 1

        if self.count == 0:
            self.sentiment_ewm = sentiment
        else:
            self.sentiment_ewm = self.alpha * sentiment + (1 - self.alpha) * self.sentiment_ewm
        self.count += 1

    @property
    def total_mentions(self) -> int:
        """Non-zero-sentiment mentions, matching the original's volume basis."""
        return self.neg_count + self.pos_count

    @property
    def sentiment(self) -> float:
        return round(self.sentiment_ewm, 3) if self.count else 0.0

    def risk(self) -> float:
        if self.count == 0:
            return 0.0

        overall = self.sentiment
        # -1.0 sentiment -> 1.0 risk, +1.0 sentiment -> 0.0 risk
        sentiment_risk = max(0.0, min(1.0, (1.0 - overall) / 2.0))

        n = self.total_mentions
        volume_multiplier = min(1.5, 1.0 + (n - 1) * 0.1) if n else 1.0

        if n > 0:
            negative_ratio = self.neg_count / n
            negative_bias = 1.0 + (negative_ratio - 0.5) * 0.5   # 0.75x .. 1.25x
        else:
            negative_bias = 1.0

        return round(min(1.0, sentiment_risk * volume_multiplier * negative_bias), 3)


def explode_mentions(doc: Dict[str, Any]) -> List[CompanyMention]:
    """Enriched article -> one CompanyMention per matched ticker.

    Lives here rather than in the Flink job so the calibration harness buckets
    exactly what the running pipeline buckets. An article matching two tickers
    produces two mentions and counts toward both windows, which is intended.
    """
    try:
        event_ts = int(parse_iso(doc["published_at"]).timestamp() * 1000)
    except (KeyError, ValueError, TypeError):
        return []

    out: List[CompanyMention] = []
    for company in doc.get("companies", []) or []:
        ticker = company.get("ticker")
        if not ticker:
            continue
        out.append(CompanyMention(
            ticker=ticker,
            article_id=doc.get("article_id", ""),
            title=doc.get("title", ""),
            url=doc.get("url", ""),
            source=doc.get("source", ""),
            sentiment=float(doc.get("sentiment", 0.0) or 0.0),
            role=company.get("role", "mentioned"),
            event_ts=event_ts,
        ))
    return out


def score_sentiments(sentiments: Sequence[float]) -> float:
    """Risk for a bare sequence of sentiments. Used by the calibration harness."""
    agg = Aggregation()
    for s in sentiments:
        agg.add(s)
    return agg.risk()


def aggregate(mentions: Iterable[CompanyMention]) -> Aggregation:
    agg = Aggregation()
    for m in mentions:
        agg.add(m.sentiment)
    return agg


def build_features(
    ticker: str,
    mentions: Sequence[CompanyMention],
    window_start: datetime,
    window_end: datetime,
) -> RiskFeatures:
    agg = aggregate(mentions)

    # Surface the most negative headline in the window — it is what the alert
    # and the dashboard row should actually show a human.
    top = min(mentions, key=lambda m: m.sentiment, default=None)

    return RiskFeatures(
        ticker=ticker,
        window_start=iso(window_start),
        window_end=iso(window_end),
        risk_score=agg.risk(),
        sentiment_score=agg.sentiment,
        neg_count=agg.neg_count,
        pos_count=agg.pos_count,
        total_mentions=len(mentions),
        top_headline=top.title if top else "",
        top_url=top.url if top else "",
    )


def severity_for(risk_score: float, high: float = 0.8, medium: float = 0.6) -> str:
    """Ported verbatim from alerting.py:58-63."""
    if risk_score >= high:
        return "high"
    if risk_score >= medium:
        return "medium"
    return "low"
