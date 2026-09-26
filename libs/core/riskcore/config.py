"""Env-backed configuration.

Every constant here is overridable via environment variable so the same image
runs locally and on EC2. Defaults are the ones justified by the calibration
replay documented in the plan.
"""
import os

_TRUE = {"1", "true", "yes", "on"}


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    return default if raw is None else raw.strip().lower() in _TRUE


# ---- infrastructure ----
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
ENRICHMENT_URL = os.getenv("ENRICHMENT_URL", "http://localhost:8082")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# PostgreSQL in every deployment; sqlite:/// URLs work too (used by the tests).
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://riskradar:riskradar@localhost:5432/riskradar")

# "kafka" (default) or "inproc": the standalone runner swaps Kafka for an
# in-process queue (see riskcore.kafka.set_local_sink).
BUS = os.getenv("BUS", "kafka").strip().lower()

TOPIC_RAW = os.getenv("TOPIC_RAW", "news.raw")
TOPIC_ENRICHED = os.getenv("TOPIC_ENRICHED", "news.enriched")

# ---- windowing ----
# 15m/3m, not the draft's 5m/1m: at the measured ~16 articles/ticker/hour a
# 5-minute window holds a median of 2 articles, too thin for any volume signal.
WINDOW_SIZE_SECONDS = _int("WINDOW_SIZE_SECONDS", 900)
WINDOW_SLIDE_SECONDS = _int("WINDOW_SLIDE_SECONDS", 180)
WATERMARK_DELAY_SECONDS = _int("WATERMARK_DELAY_SECONDS", 30)

# ---- enrichment micro-batching (inside the Flink operator) ----
ENRICH_BATCH_SIZE = _int("ENRICH_BATCH_SIZE", 5)
ENRICH_BATCH_TIMEOUT_MS = _int("ENRICH_BATCH_TIMEOUT_MS", 1000)
ENRICH_TIMEOUT_SECONDS = _float("ENRICH_TIMEOUT_SECONDS", 20.0)

# ---- ingestion ----
FETCH_INTERVAL_SECONDS = _int("FETCH_INTERVAL_SECONDS", 120)
MAX_ARTICLE_AGE_HOURS = _int("MAX_ARTICLE_AGE_HOURS", 6)
CLAMP_AGE_MINUTES = _int("CLAMP_AGE_MINUTES", 10)
SEEN_TTL_SECONDS = _int("SEEN_TTL_SECONDS", 604800)
ENABLE_GNEWS = _bool("ENABLE_GNEWS", True)
ENABLE_SEC = _bool("ENABLE_SEC", False)
SEC_CONTACT_EMAIL = os.getenv("SEC_CONTACT_EMAIL", "")

# ---- alerting ----
# There is deliberately no absolute threshold constant. Alerting is relative to
# each ticker's own trailing baseline; a fixed 0.7 was measured firing on 13.5%
# of windows, including single mildly-negative headlines.
ALERT_PERCENTILE = _float("ALERT_PERCENTILE", 99.0)
MIN_MENTIONS_FOR_ALERT = _int("MIN_MENTIONS_FOR_ALERT", 2)
MIN_BASELINE_SAMPLES = _int("MIN_BASELINE_SAMPLES", 50)
BASELINE_WINDOW_HOURS = _int("BASELINE_WINDOW_HOURS", 24)
ALERT_COOLDOWN_MINUTES = _int("ALERT_COOLDOWN_MINUTES", 10)
# Delivery runs on the pipeline thread, so its worst case is kept small.
SLACK_MAX_RETRIES = _int("SLACK_MAX_RETRIES", 2)
SLACK_TIMEOUT_SECONDS = _float("SLACK_TIMEOUT_SECONDS", 5.0)

# ---- severity bands (ported verbatim from alerting.py:58-63) ----
SEVERITY_HIGH = _float("SEVERITY_HIGH", 0.8)
SEVERITY_MEDIUM = _float("SEVERITY_MEDIUM", 0.6)

# ---- universe ----
MAG7 = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA"]

HEADLINES_KEY = "recent:headlines"
HEADLINES_MAX = _int("HEADLINES_MAX", 200)

USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
)
