"""Enriched article -> mentions. Shared by the Flink job and the calibration harness."""
from datetime import datetime, timezone

import pytest

from riskcore.models import iso
from riskcore.risk import explode_mentions

TS = datetime(2026, 7, 27, 12, 0, tzinfo=timezone.utc)


def doc(**over):
    base = {
        "article_id": "a1",
        "title": "Tesla recalls vehicles",
        "url": "https://example.com/a",
        "source": "Reuters",
        "published_at": iso(TS),
        "sentiment": -0.6,
        "companies": [{"ticker": "TSLA", "role": "primary"}],
    }
    base.update(over)
    return base


def test_one_mention_per_ticker():
    out = explode_mentions(doc(companies=[
        {"ticker": "NVDA", "role": "primary"},
        {"ticker": "TSLA", "role": "mentioned"},
    ]))
    assert [m.ticker for m in out] == ["NVDA", "TSLA"]
    assert [m.role for m in out] == ["primary", "mentioned"]


def test_event_ts_is_epoch_millis():
    m = explode_mentions(doc())[0]
    assert m.event_ts == int(TS.timestamp() * 1000)


def test_sentiment_is_carried_onto_every_mention():
    out = explode_mentions(doc(sentiment=-0.75, companies=[
        {"ticker": "NVDA", "role": "primary"},
        {"ticker": "TSLA", "role": "primary"},
    ]))
    assert all(m.sentiment == -0.75 for m in out)


def test_article_with_no_companies_yields_nothing():
    assert explode_mentions(doc(companies=[])) == []
    assert explode_mentions(doc(companies=None)) == []


def test_missing_published_at_yields_nothing():
    bad = doc()
    del bad["published_at"]
    assert explode_mentions(bad) == []


def test_unparseable_published_at_yields_nothing():
    assert explode_mentions(doc(published_at="not-a-date")) == []


def test_company_without_ticker_is_skipped():
    out = explode_mentions(doc(companies=[{"role": "primary"}, {"ticker": "TSLA"}]))
    assert [m.ticker for m in out] == ["TSLA"]


def test_missing_sentiment_defaults_to_neutral():
    assert explode_mentions(doc(sentiment=None))[0].sentiment == 0.0


def test_round_trips_through_json():
    from riskcore.models import CompanyMention
    m = explode_mentions(doc())[0]
    assert CompanyMention.from_json(m.to_json()) == m
