"""Wire-format dataclasses.

Everything that crosses a Kafka topic or a process boundary is one of these.
JSON round-tripping is explicit because the PyFlink job serialises with plain
strings (Types.STRING()) rather than a Java-side POJO.
"""
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import json
import uuid


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    """RFC3339 with a trailing Z, which is what every consumer here expects."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_iso(value: str) -> datetime:
    """Parse the format `iso()` emits, tolerating the Z suffix."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


# Source string simulate.py stamps on every synthetic article. Windows containing
# one are tagged simulated: their alerts say so, and they never enter a baseline.
SIMULATED_SOURCE = "RiskRadar (simulated)"


def safe_url(value) -> str:
    """Pass through http(s) URLs only.

    Feed links are third-party data that ends up in href attributes. HTML
    escaping does not neutralise `javascript:` or `data:` URLs, so anything
    that is not plainly http(s) becomes "" at the point it enters the system.
    """
    from urllib.parse import urlsplit
    if not isinstance(value, str):
        return ""
    value = value.strip()
    try:
        parts = urlsplit(value)
    except ValueError:
        return ""
    if parts.scheme.lower() in ("http", "https") and parts.netloc:
        return value
    return ""


class _JsonMixin:
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))

    @classmethod
    def from_json(cls, raw: str):
        return cls.from_dict(json.loads(raw))

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        known = {f for f in cls.__dataclass_fields__}  # type: ignore[attr-defined]
        return cls(**{k: v for k, v in data.items() if k in known})


@dataclass
class NewsArticle(_JsonMixin):
    """A deduped, age-filtered headline on its way into news.raw."""
    article_id: str
    title: str
    url: str
    source: str
    published_at: str          # iso8601 Z, already clamped by ingestion
    feed: str = ""
    summary: str = ""

    @staticmethod
    def new_id() -> str:
        return uuid.uuid4().hex


@dataclass
class CompanyMatch(_JsonMixin):
    ticker: str
    role: str                  # "primary" (headline hit) | "mentioned"


@dataclass
class EnrichedArticle(_JsonMixin):
    """news.enriched payload: the article plus sentiment and matched tickers."""
    article_id: str
    title: str
    url: str
    source: str
    published_at: str
    sentiment: float           # [-1, 1]
    companies: List[Dict[str, str]] = field(default_factory=list)
    model: str = ""
    feed: str = ""

    def tickers(self) -> List[str]:
        return [c["ticker"] for c in self.companies]


@dataclass
class CompanyMention(_JsonMixin):
    """One (article x ticker) pair — the unit the sliding window aggregates."""
    ticker: str
    article_id: str
    title: str
    url: str
    source: str
    sentiment: float
    role: str
    event_ts: int              # epoch millis; the window's event-time attribute


@dataclass
class RiskFeatures(_JsonMixin):
    """Output of one sliding window for one ticker."""
    ticker: str
    window_start: str
    window_end: str
    risk_score: float
    sentiment_score: float
    neg_count: int
    pos_count: int
    total_mentions: int
    top_headline: str = ""
    top_url: str = ""
    # Uncapped risk (0 .. 1.875). Alerting compares THIS against the baseline;
    # risk_score is min(1.0, alert_score) for display. Defaults to -1 so a
    # payload written before this field existed falls back to risk_score.
    alert_score: float = -1.0
    # True when any mention in the window came from /api/simulate.
    simulated: bool = False

    def __post_init__(self):
        if self.alert_score is None or self.alert_score < 0:
            self.alert_score = self.risk_score


@dataclass
class Alert(_JsonMixin):
    alert_id: str
    ticker: str
    risk_score: float
    baseline_p: float          # the percentile cut this window cleared
    severity: str              # high | medium | low
    message: str
    window_end: str
    fired_at: str
    source: str = "live"       # "live" | "simulated"
    top_headline: str = ""
    top_url: str = ""
    alert_score: float = 0.0   # the uncapped score that crossed the baseline

    @staticmethod
    def new_id() -> str:
        return uuid.uuid4().hex
