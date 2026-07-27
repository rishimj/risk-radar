"""Ingestion loop: poll the free feeds, publish fresh articles to news.raw."""
import logging
import signal
import sys
import time

import redis

from riskcore import config, feeds, kafka
from riskcore.models import utcnow

from pipeline import process

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("ingestion")

_running = True


def _stop(signum, frame):
    global _running
    log.info("signal %s received, finishing current sweep", signum)
    _running = False


def sweep(redis_client) -> dict:
    started = time.perf_counter()
    feed_list = feeds.default_feeds(window="1h")

    articles = feeds.fetch_all(feed_list, stagger_seconds=0.4)
    to_send, stats = process(redis_client, articles, now=utcnow())

    for article in to_send:
        kafka.send(config.TOPIC_RAW, article.to_json(), key=article.article_id)
    if to_send:
        kafka.flush()

    elapsed = (time.perf_counter() - started) * 1000
    payload = {**stats.as_dict(), "feeds": len(feed_list), "elapsed_ms": round(elapsed)}
    log.info(
        "sweep fetched=%(fetched)d dup=%(duplicates)d clamped=%(clamped)d "
        "produced=%(produced)d feeds=%(feeds)d in %(elapsed_ms)dms", payload
    )
    return payload


def main() -> int:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    redis_client = redis.from_url(config.REDIS_URL)

    # Fail fast and loudly rather than silently producing nothing.
    for attempt in range(10):
        try:
            redis_client.ping()
            break
        except redis.RedisError as exc:
            log.warning("waiting for redis (%d/10): %s", attempt + 1, exc)
            time.sleep(3)
    else:
        log.error("redis never became reachable")
        return 1

    log.info(
        "ingestion starting: interval=%ss max_age=%sh gnews=%s",
        config.FETCH_INTERVAL_SECONDS, config.MAX_ARTICLE_AGE_HOURS, config.ENABLE_GNEWS,
    )

    while _running:
        cycle_started = time.monotonic()
        try:
            sweep(redis_client)
        except Exception:                              # noqa: BLE001 - never die on one bad sweep
            log.exception("sweep failed; retrying next interval")

        # Sleep the remainder of the interval, in short slices so SIGTERM is snappy.
        deadline = cycle_started + config.FETCH_INTERVAL_SECONDS
        while _running and time.monotonic() < deadline:
            time.sleep(min(1.0, deadline - time.monotonic()))

    kafka.close()
    log.info("ingestion stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
