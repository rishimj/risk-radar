"""Ingestion pipeline: dedup, clamping, and the ordering that keeps stale news out."""
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import fakeredis
import pytest

from conftest import load_service_module                       # noqa: E402
from riskcore.models import NewsArticle, iso, parse_iso        # noqa: E402

_p = load_service_module("ingestion", "pipeline")
clamp_published, is_new, process, seen_key = (
    _p.clamp_published, _p.is_new, _p.process, _p.seen_key)

NOW = datetime(2026, 7, 27, 20, 0, tzinfo=timezone.utc)


@pytest.fixture
def redis_client():
    return fakeredis.FakeRedis()


def article(minutes_ago: float = 1, url: str = "https://example.com/a") -> NewsArticle:
    return NewsArticle(
        article_id=url.rsplit("/", 1)[-1],
        title="Tesla recalls vehicles",
        url=url,
        source="Reuters",
        published_at=iso(NOW - timedelta(minutes=minutes_ago)),
        feed="gnews:TSLA",
    )


# -- dedup ------------------------------------------------------------------
def test_first_sighting_is_new_then_not(redis_client):
    art = article()
    assert is_new(redis_client, art) is True
    assert is_new(redis_client, art) is False


def test_dedup_key_is_namespaced(redis_client):
    is_new(redis_client, article())
    assert redis_client.exists(seen_key("a"))


def test_dedup_sets_a_ttl(redis_client):
    is_new(redis_client, article(), ttl=100)
    assert 0 < redis_client.ttl(seen_key("a")) <= 100


def test_repeated_sweeps_produce_each_article_once(redis_client):
    """Polling a 1h window every 2min re-fetches the same items ~30 times."""
    arts = [article(url=f"https://example.com/{i}") for i in range(5)]
    first, s1 = process(redis_client, list(arts), now=NOW)
    second, s2 = process(redis_client, list(arts), now=NOW)
    assert s1.produced == 5 and s2.produced == 0
    assert s2.duplicates == 5


# -- clamping ---------------------------------------------------------------
def test_recent_article_is_left_alone():
    art, clamped = clamp_published(article(minutes_ago=3), now=NOW)
    assert clamped is False
    assert parse_iso(art.published_at) == NOW - timedelta(minutes=3)


def test_lagging_article_is_pulled_forward():
    """Google News' median article age was 28-45min; without this the window
    would drop them all as late data."""
    art, clamped = clamp_published(article(minutes_ago=40), now=NOW)
    assert clamped is True
    assert parse_iso(art.published_at) == NOW


def test_clamp_boundary_is_inclusive():
    art, clamped = clamp_published(article(minutes_ago=10), now=NOW)
    assert clamped is False


# -- ordering ---------------------------------------------------------------
def test_clamping_happens_after_dedup_not_before(redis_client):
    """If clamping ran first, the dedup key would still be URL-derived, but the
    *stats* would be wrong — and more importantly a duplicate would consume a
    clamp slot. Pin the observable ordering."""
    art = article(minutes_ago=40)
    process(redis_client, [art], now=NOW)
    _, stats = process(redis_client, [article(minutes_ago=40)], now=NOW)
    assert stats.duplicates == 1
    assert stats.clamped == 0          # duplicate never reached the clamp step


def test_stats_account_for_every_article(redis_client):
    arts = [article(url=f"https://example.com/{i}", minutes_ago=1) for i in range(3)]
    arts.append(article(url="https://example.com/0"))       # dup of the first
    _, stats = process(redis_client, arts, now=NOW)
    assert stats.fetched == 4
    assert stats.duplicates == 1
    assert stats.produced == 3


def test_process_returns_the_articles_it_counted(redis_client):
    arts = [article(url=f"https://example.com/{i}") for i in range(3)]
    out, stats = process(redis_client, arts, now=NOW)
    assert len(out) == stats.produced == 3
    assert all(isinstance(a, NewsArticle) for a in out)
