"""Inject a synthetic negative-news event for one ticker.

Real news fires alerts on its own — 672 articles over 6h produced threshold
crossings on all seven tickers — so this is NOT the main demo path. It exists
for the cases live news can't cover on demand: a walkthrough where TSLA must
alert *now*, an end-to-end test, and quiet weekends.

Alerts produced this way are tagged source="simulated" so the history stays
honest about which alerts were real.

The watermark trick is the fiddly part. Windows are keyed by ticker, but
watermarks are stream-global, so filler articles about a DIFFERENT ticker still
advance event time past the crisis window and force it to close. The filler is
stamped far enough ahead to clear the earliest window containing the crisis:

    earliest window end <= crisis_ts + WINDOW_SLIDE      (half-open windows)
    watermark           == max_event_ts - WATERMARK_DELAY

so the filler needs WINDOW_SLIDE + WATERMARK_DELAY + margin ahead of the crisis.
"""
from datetime import timedelta
from typing import Dict, List
import logging

from riskcore import config, kafka
from riskcore.entities import COMPANIES
from riskcore.models import SIMULATED_SOURCE, NewsArticle, iso, utcnow

log = logging.getLogger("simulate")

# Wording chosen to score strongly negative under FinBERT — concrete financial
# events, not merely gloomy adjectives.
CRISIS_TEMPLATES = [
    "{name} shares plunge after company slashes full-year guidance",
    "{name} faces federal probe over accounting irregularities",
    "{name} recalls millions of units in costly safety failure",
    "{name} hit with class-action lawsuit from investors",
    "{name} misses earnings badly as revenue collapses",
    "Analysts downgrade {name} to sell on deteriorating fundamentals",
    "{name} loses major customer contract, outlook cut",
    "{name} CFO resigns abruptly amid restatement concerns",
    "{name} credit rating downgraded to junk status",
    "{name} halts production after critical component defect",
]

FILLER_TEMPLATES = [
    "{name} announces routine executive appointment",
    "{name} schedules quarterly earnings call date",
    "{name} updates corporate governance documentation",
    "{name} publishes annual sustainability report",
    "{name} confirms participation in industry conference",
]

MARGIN_SECONDS = 30


def _filler_ticker(ticker: str) -> str:
    """Any Mag 7 name other than the one under simulation."""
    for candidate in config.MAG7:
        if candidate != ticker:
            return candidate
    return ticker


def build_articles(ticker: str) -> Dict[str, List[NewsArticle]]:
    """Crisis articles at now, filler stamped ahead to advance the watermark."""
    now = utcnow()
    name = COMPANIES[ticker].name

    crisis = [
        NewsArticle(
            article_id=f"sim-{ticker}-{int(now.timestamp())}-{i}",
            title=template.format(name=name),
            url=f"https://riskradar.local/simulated/{ticker}/{int(now.timestamp())}/{i}",
            source=SIMULATED_SOURCE,
            published_at=iso(now),
            feed="simulate",
        )
        for i, template in enumerate(CRISIS_TEMPLATES)
    ]

    advance = timedelta(seconds=(
        config.WINDOW_SLIDE_SECONDS + config.WATERMARK_DELAY_SECONDS + MARGIN_SECONDS
    ))
    filler_ts = now + advance
    filler_name = COMPANIES[_filler_ticker(ticker)].name

    filler = [
        NewsArticle(
            article_id=f"sim-filler-{int(now.timestamp())}-{i}",
            title=template.format(name=filler_name),
            url=f"https://riskradar.local/simulated/filler/{int(now.timestamp())}/{i}",
            source=SIMULATED_SOURCE,
            published_at=iso(filler_ts),
            feed="simulate",
        )
        for i, template in enumerate(FILLER_TEMPLATES)
    ]

    return {"crisis": crisis, "filler": filler}


def run(redis_client, ticker: str) -> Dict:
    """Clear the cooldown, publish the crisis burst, then the watermark filler."""
    from riskcore import alerting

    ticker = ticker.upper()
    if ticker not in COMPANIES:
        raise ValueError(f"unknown ticker {ticker}")

    # Without this a second demo inside the cooldown would silently do nothing.
    alerting.clear_cooldown(redis_client, ticker)

    batches = build_articles(ticker)

    for article in batches["crisis"]:
        kafka.send(config.TOPIC_RAW, article.to_json(), key=article.article_id)
    kafka.flush()

    for article in batches["filler"]:
        kafka.send(config.TOPIC_RAW, article.to_json(), key=article.article_id)
    kafka.flush()

    log.info("simulated %d crisis + %d filler articles for %s",
             len(batches["crisis"]), len(batches["filler"]), ticker)

    return {
        "ticker": ticker,
        "crisis_articles": len(batches["crisis"]),
        "filler_articles": len(batches["filler"]),
        "expected_alert_seconds": [15, 40],
    }
