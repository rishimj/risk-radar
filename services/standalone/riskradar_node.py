#!/usr/bin/env python3
"""RiskRadar on one small machine: the whole pipeline in a single process.

The compose stack is nine containers (Kafka, Flink, DynamoDB Local, ...) and
needs several GB. This runs the same code paths inside one Python process plus
a Redis, for a shared VM that must stay well under 1.5 GB:

    ingestion thread --(in-process queue, replaces Kafka news.raw)--> pipeline thread
    pipeline thread  = services/processor StreamEngine (the Flink topology's twin)
                       + in-process sentiment (FinBERT if torch is installed,
                         else FinVADER/VADER; same ladder as services/enrichment)
    web              = services/webapp, served by uvicorn on 127.0.0.1
    tables           = SQLite (riskcore.sqlitedb) instead of DynamoDB
    state            = Redis (baselines, features, headlines, rate limits)

What stays identical to the compose stack: the scoring, windowing, baseline,
alerting and every webapp route, because all of them are the same modules.
What differs: no Kafka (so no news.enriched topic), no Flink UI, and window
state is in memory (a restart re-opens windows from the next articles).

Baselines are seeded at startup with THIS process's sentiment model, so the
alert threshold is calibrated against the same model that scores live news.

Configuration is environment only; see deploy/azure/risk-radar.env.example.
"""
from pathlib import Path
import logging
import os
import queue
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[2]

# ---- defaults that make this mode what it is; env can still override ----
os.environ.setdefault("DB_BACKEND", "sqlite")
os.environ.setdefault("BUS", "inproc")
os.environ.setdefault("STACK_LABEL", "FinBERT · Python stream engine · Redis · SQLite")

# Only the webapp (whose modules import each other by bare name) and tools/ go
# on sys.path. The enrichment, processor and ingestion services each have
# their own app.py/main.py, so their helpers are loaded by file path instead.
for sub in ("tools", "services/webapp/src"):
    path = str(ROOT / sub)
    if path not in sys.path:
        sys.path.insert(0, path)


def _load(name: str, relpath: str):
    import importlib.util
    qualified = f"_riskradar_{name}"
    if qualified in sys.modules:
        return sys.modules[qualified]
    spec = importlib.util.spec_from_file_location(qualified, ROOT / relpath)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[qualified] = mod
    spec.loader.exec_module(mod)
    return mod

from riskcore import config, feeds, kafka, stages          # noqa: E402
from riskcore.baseline import BaselineStore                # noqa: E402
from riskcore.entities import detect_companies             # noqa: E402
from riskcore.models import EnrichedArticle, utcnow        # noqa: E402

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("riskradar.node")

HOST = os.getenv("HOST", "127.0.0.1")
PORT = int(os.getenv("PORT", "3001"))
QUEUE_MAX = int(os.getenv("BUS_QUEUE_MAX", "5000"))
SEED_HOURS = int(os.getenv("SEED_HOURS", "24"))
GUEST_PURGE_EVERY_S = 6 * 3600


def check_secret_key() -> None:
    """Refuse to serve the public internet with a guessable session key."""
    key = os.getenv("SECRET_KEY", "")
    if len(key) < 32 or key == "change-me-to-a-long-random-string":
        sys.exit("SECRET_KEY must be set to a random value of at least 32 characters "
                 "(python3 -c 'import secrets; print(secrets.token_hex(32))').")


# ---------------------------------------------------------------------------
# sentiment, in-process (one model, shared by live scoring and seeding)
# ---------------------------------------------------------------------------
class LocalEnricher:
    def __init__(self):
        sentiment = _load("sentiment", "services/enrichment/src/sentiment.py")
        self.engine = sentiment.SentimentEngine()
        self.lock = threading.Lock()                     # one forward pass at a time
        self.stats = {"batches": 0, "articles": 0, "errors": 0}

    def load(self) -> str:
        try:
            import torch
            torch.set_num_threads(int(os.getenv("TORCH_THREADS", "1")))
        except ImportError:
            pass
        return self.engine.load()

    def score(self, texts):
        with self.lock:
            return self.engine.score_batch(list(texts))

    def enrich(self, articles):
        """Same output as POST /v1/enrich/batch, without the HTTP hop."""
        if not articles:
            return []
        try:
            scores = self.score([(a.title or "").strip() for a in articles])
        except Exception:                                # noqa: BLE001
            log.exception("scoring failed; returning neutral")
            self.stats["errors"] += 1
            scores = [0.0] * len(articles)
        out = []
        for article, s in zip(articles, scores):
            try:
                companies = detect_companies(article.title or "", article.summary or "")
            except Exception:                            # noqa: BLE001
                self.stats["errors"] += 1
                companies = []
            out.append(EnrichedArticle(
                article_id=article.article_id, title=article.title, url=article.url,
                source=article.source, published_at=article.published_at,
                sentiment=float(s), companies=companies, model=self.engine.tier,
                feed=article.feed,
            ))
        self.stats["batches"] += 1
        self.stats["articles"] += len(out)
        return out


# ---------------------------------------------------------------------------
# workers
# ---------------------------------------------------------------------------
class Node:
    def __init__(self):
        import redis
        engine = _load("engine", "services/processor/src/engine.py")
        StreamEngine, window_sink = engine.StreamEngine, engine.window_sink

        self.redis = redis.from_url(config.REDIS_URL)
        self.store = BaselineStore(self.redis)
        self.enricher = LocalEnricher()
        self.bus: "queue.Queue[str]" = queue.Queue(maxsize=QUEUE_MAX)
        self.dropped = 0
        self.last_sweep = None

        self.engine = StreamEngine(
            enrich=self.enricher.enrich,
            on_enriched=lambda value: None,       # no news.enriched topic without Kafka
            on_headline=lambda doc: stages.write_headline(self.redis, doc),
            on_window=window_sink(self.redis, self.store),
            size_ms=config.WINDOW_SIZE_SECONDS * 1000,
            slide_ms=config.WINDOW_SLIDE_SECONDS * 1000,
            watermark_delay_ms=config.WATERMARK_DELAY_SECONDS * 1000,
            batch_size=config.ENRICH_BATCH_SIZE,
            batch_timeout_ms=config.ENRICH_BATCH_TIMEOUT_MS,
        )
        kafka.set_local_sink(self._publish)

    # -- the bus -----------------------------------------------------------
    def _publish(self, topic, value, key=None):
        if topic != config.TOPIC_RAW:
            return
        try:
            self.bus.put_nowait(value)
        except queue.Full:
            # Bounded on purpose: a stalled pipeline must not grow memory forever.
            self.dropped += 1

    # -- threads -------------------------------------------------------------
    def pipeline_loop(self):
        while True:
            try:
                value = self.bus.get(timeout=0.2)
                self.engine.offer(value, int(time.time() * 1000))
                while True:                        # drain what is already queued
                    try:
                        self.engine.offer(self.bus.get_nowait(), int(time.time() * 1000))
                    except queue.Empty:
                        break
            except queue.Empty:
                pass
            except Exception:                              # noqa: BLE001
                log.exception("pipeline error")
            try:
                self.engine.tick(int(time.time() * 1000))
            except Exception:                              # noqa: BLE001
                log.exception("pipeline tick error")

    def ingestion_loop(self):
        process = _load("pipeline", "services/ingestion/src/pipeline.py").process
        import store                                       # services/webapp/src
        last_purge = 0.0
        while True:
            started = time.monotonic()
            try:
                feed_list = feeds.default_feeds(window="1h")
                articles = feeds.fetch_all(feed_list, stagger_seconds=0.4)
                to_send, stats = process(self.redis, articles, now=utcnow())
                for article in to_send:
                    kafka.send(config.TOPIC_RAW, article.to_json(), key=article.article_id)
                self.last_sweep = {**stats.as_dict(), "at": time.time()}
                log.info("sweep fetched=%(fetched)d dup=%(duplicates)d produced=%(produced)d",
                         stats.as_dict())
            except Exception:                              # noqa: BLE001
                log.exception("sweep failed; retrying next interval")

            if time.time() - last_purge > GUEST_PURGE_EVERY_S:
                try:
                    store.purge_guests(int(os.getenv("GUEST_MAX_AGE_HOURS", "48")))
                    last_purge = time.time()
                except Exception:                          # noqa: BLE001
                    log.exception("guest purge failed")

            time.sleep(max(5.0, config.FETCH_INTERVAL_SECONDS - (time.monotonic() - started)))

    def seed_baselines(self):
        """Fill any ticker below the sample floor from the last SEED_HOURS of news."""
        from collections import defaultdict
        from replay import enrich_locally, fetch_corpus, score_windows   # tools/

        short = [t for t in config.MAG7 if self.store.sample_count(t) < self.store.min_samples]
        if not short:
            log.info("baselines already seeded")
            return
        log.info("seeding baselines for %s from %dh of news (%s)",
                 ", ".join(short), SEED_HOURS, self.enricher.engine.tier)
        try:
            articles = fetch_corpus(SEED_HOURS, tickers=short)
            docs = enrich_locally(articles, self.enricher.score)
            grouped = defaultdict(list)
            for w in score_windows(docs):
                grouped[w.ticker].append((w.end_epoch, w.risk))
            for ticker in short:
                if grouped.get(ticker):
                    self.store.record_many(ticker, grouped[ticker])
            log.info("seeded: %s", {t: self.store.sample_count(t) for t in short})
        except Exception:                                  # noqa: BLE001
            log.exception("baseline seeding failed; alerting stays silent until warm")

    # -- status pills ----------------------------------------------------------
    def status_providers(self):
        return {
            "kafka": lambda: f"in-process bus, {self.bus.qsize()} queued",
            "flink": lambda: (f"lite engine, {self.engine.stats.windows_fired} windows, "
                              f"{self.engine.stats.stage_errors} errors"),
            "enrichment": lambda: self.enricher.engine.tier,
        }


def main() -> int:
    check_secret_key()

    node = Node()
    tier = node.enricher.load()
    log.info("sentiment tier: %s", tier)

    import app as webapp                                  # services/webapp/src
    webapp.status_providers.update(node.status_providers())
    webapp.status_labels.update({"kafka": "Bus", "flink": "Stream"})

    def start(name, fn):
        threading.Thread(target=fn, name=name, daemon=True).start()

    start("pipeline", node.pipeline_loop)

    def seed_then_ingest():
        node.seed_baselines()
        node.ingestion_loop()
    start("ingestion", seed_then_ingest)

    import uvicorn
    uvicorn.run(
        webapp.app, host=HOST, port=PORT, workers=1,
        # Caddy on the same host is the only client. Trusting X-Forwarded-For
        # from 127.0.0.1 ONLY is what makes per-IP rate limits see real IPs.
        proxy_headers=True, forwarded_allow_ips="127.0.0.1",
        server_header=False, date_header=False,
        limit_concurrency=int(os.getenv("LIMIT_CONCURRENCY", "64")),
        timeout_keep_alive=5,
        log_level=config.LOG_LEVEL.lower(),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
