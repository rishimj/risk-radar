"""Per-article ingestion pipeline: filter -> dedup -> clean -> clamp -> produce.

The ORDER of these steps is the whole design. Age-filtering happens inside
riskcore.feeds (before anything else), and clamping happens last. Reversing them
would rewrite CNBC's evergreen tail — observed up to 914h old — to "now" and
re-inject month-old articles as breaking news on every single poll, forever.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, List, Optional, Tuple

from riskcore import config
from riskcore.models import NewsArticle, iso, parse_iso, utcnow


@dataclass
class SweepStats:
    fetched: int = 0
    duplicates: int = 0
    clamped: int = 0
    produced: int = 0

    def as_dict(self):
        return {
            "fetched": self.fetched,
            "duplicates": self.duplicates,
            "clamped": self.clamped,
            "produced": self.produced,
        }


def seen_key(article_id: str) -> str:
    return f"seen:news:{article_id}"


def is_new(redis_client, article: NewsArticle, ttl: Optional[int] = None) -> bool:
    """SET NX EX — atomic claim, so a restart mid-sweep can't double-produce.

    This also absorbs the heavy overlap inherent in polling a 1-hour Google News
    window every 2 minutes: the same article is re-fetched ~30 times and
    published exactly once.
    """
    ttl = ttl if ttl is not None else config.SEEN_TTL_SECONDS
    return bool(redis_client.set(seen_key(article.article_id), "1", nx=True, ex=ttl))


def clamp_published(article: NewsArticle, now: Optional[datetime] = None,
                    max_lag_minutes: Optional[int] = None) -> Tuple[NewsArticle, bool]:
    """Pull stale-but-acceptable timestamps forward to now.

    Event-time protection: an article published 40 minutes ago (Google News'
    median age was 28-45min) would otherwise land behind the watermark and be
    dropped by the window as late data. Anything genuinely old was already
    removed by the age filter upstream.
    """
    now = now or utcnow()
    lag = max_lag_minutes if max_lag_minutes is not None else config.CLAMP_AGE_MINUTES
    cutoff = now - timedelta(minutes=lag)

    published = parse_iso(article.published_at)
    if published >= cutoff:
        return article, False

    article.published_at = iso(now)
    return article, True


def process(redis_client, articles: Iterable[NewsArticle],
            now: Optional[datetime] = None) -> Tuple[List[NewsArticle], SweepStats]:
    """Dedup and clamp a fetched sweep, returning what should be produced."""
    now = now or utcnow()
    stats = SweepStats()
    out: List[NewsArticle] = []

    for article in articles:
        stats.fetched += 1

        if not is_new(redis_client, article):
            stats.duplicates += 1
            continue

        article, was_clamped = clamp_published(article, now=now)
        if was_clamped:
            stats.clamped += 1

        out.append(article)

    stats.produced = len(out)
    return out, stats
