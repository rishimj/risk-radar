"""Lite stream processor: the Flink job's topology in one small Python process.

Use it instead of the three Flink containers when memory is tight (an M-series
laptop runs the Flink images under Rosetta, since PyFlink ships no arm64 wheel).
It consumes news.raw, and writes news.enriched, Redis features/headlines/
baselines and PostgreSQL alerts exactly as the Flink job does. Run ONE engine at a
time: both would consume news.raw and double-count every window.

What it gives up versus Flink: no checkpointed state. Kafka offsets are only
committed once the enrichment buffer is empty, so no article is lost between
fetch and enrichment, but open windows live in memory; a restart re-opens them
from the next articles onward.

GET :8083/healthz and :8083/stats expose liveness and counters; the webapp's
status pill reads /stats.
"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
import os
import signal
import sys
import threading
import time

import redis
import requests
from kafka import KafkaConsumer

from riskcore import config, db, kafka, stages
from riskcore.baseline import BaselineStore

from engine import StreamEngine, window_sink

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("processor")

PORT = int(os.getenv("PROCESSOR_PORT", "8083"))
GROUP_ID = os.getenv("PROCESSOR_GROUP_ID", "riskradar-processor")
JOB_NAME = "riskradar-news (lite)"

_running = True
_started = time.time()
_engine = None


def _stop(signum, frame):
    global _running
    log.info("signal %s received, stopping", signum)
    _running = False


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):                                  # noqa: N802
        if self.path == "/healthz":
            body = {"status": "ok"}
        elif self.path == "/stats":
            body = {
                "engine": "lite",
                "name": JOB_NAME,
                "state": "RUNNING" if _running and _engine is not None else "STARTING",
                "uptime_s": round(time.time() - _started),
                "watermark_ms": _engine.watermark_ms if _engine else None,
                "buffered": _engine.buffered if _engine else 0,
                "open_windows": _engine.open_windows if _engine else 0,
                **(_engine.stats.as_dict() if _engine else {}),
            }
        else:
            self.send_error(404)
            return
        data = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt, *args):                # keep probes out of the log
        pass


def _wait_for(name, check, attempts=30, delay=3.0) -> bool:
    for attempt in range(attempts):
        try:
            check()
            return True
        except Exception as exc:                       # noqa: BLE001
            log.warning("waiting for %s (%d/%d): %s", name, attempt + 1, attempts, exc)
            time.sleep(delay)
    log.error("%s never became reachable", name)
    return False


def main() -> int:
    global _engine
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    server = ThreadingHTTPServer(("0.0.0.0", PORT), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    rds = redis.from_url(config.REDIS_URL)
    if not _wait_for("redis", rds.ping):
        return 1
    # The Flink job's AlertFanout does this lazily; alerts need the tables.
    _wait_for("postgres", db.ensure_schema, attempts=10)

    store = BaselineStore(rds)
    session = requests.Session()

    _engine = StreamEngine(
        enrich=lambda articles: stages.enrich_batch(session, articles),
        on_enriched=lambda value: kafka.send(config.TOPIC_ENRICHED, value),
        on_headline=lambda doc: stages.write_headline(rds, doc),
        on_window=window_sink(rds, store),
        size_ms=config.WINDOW_SIZE_SECONDS * 1000,
        slide_ms=config.WINDOW_SLIDE_SECONDS * 1000,
        watermark_delay_ms=config.WATERMARK_DELAY_SECONDS * 1000,
        batch_size=config.ENRICH_BATCH_SIZE,
        batch_timeout_ms=config.ENRICH_BATCH_TIMEOUT_MS,
    )

    consumer = KafkaConsumer(
        config.TOPIC_RAW,
        bootstrap_servers=config.KAFKA_BOOTSTRAP.split(","),
        group_id=GROUP_ID,
        # Same starting point as the Flink job's KafkaOffsetsInitializer.latest().
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda b: b.decode("utf-8", errors="replace"),
    )
    log.info("lite processor running: window=%ss/%ss watermark=%ss batch=%d/%dms",
             config.WINDOW_SIZE_SECONDS, config.WINDOW_SLIDE_SECONDS,
             config.WATERMARK_DELAY_SECONDS, config.ENRICH_BATCH_SIZE,
             config.ENRICH_BATCH_TIMEOUT_MS)

    uncommitted = False
    try:
        while _running:
            # Poll no longer than the batch timeout so the flush timer stays honest.
            polled = consumer.poll(timeout_ms=min(200, config.ENRICH_BATCH_TIMEOUT_MS),
                                   max_records=100)
            for records in polled.values():
                for record in records:
                    _engine.offer(record.value, int(time.time() * 1000))
                    uncommitted = True
            _engine.tick(int(time.time() * 1000))

            if uncommitted and _engine.buffered == 0:
                kafka.flush()
                try:
                    consumer.commit()
                    uncommitted = False
                except Exception as exc:               # noqa: BLE001
                    log.warning("offset commit failed: %s", exc)
    finally:
        _engine.tick(2 ** 62)                          # flush whatever is buffered
        kafka.close()
        consumer.close()
        server.shutdown()
        log.info("lite processor stopped: %s", _engine.stats.as_dict())
    return 0


if __name__ == "__main__":
    sys.exit(main())
