"""Per-ticker adaptive alert thresholds.

Why this exists instead of `if risk > 0.7`: replaying 672 real Mag 7 headlines
showed a fixed 0.7 firing on 13.5% of windows, while every reformulation keyed
off *average* sentiment fired on 0.0%.  There is no constant that sits robustly
between those, because the right cut depends on the sentiment model, the feed
mix, and the ticker (TSLA sees 11 articles/hour, AAPL 32).

So: keep a trailing 24h distribution of window scores per ticker in a Redis
sorted set and fire when the current window lands above that ticker's own Pth
percentile.

The recorded score is RiskFeatures.alert_score, the UNCAPPED risk (0..1.875),
not the 0-1 dashboard gauge. With the cap, real news put ~1.8% of TSLA windows
at exactly 1.0, so p99 == 1.0 and `1.0 > 1.0` could never fire.

Known property — a sustained spike self-damps. Every window feeds the
distribution, including the ones that fired, so a run of extreme windows drags
the ticker's own percentile up to meet them and alerting goes quiet. That is
intended ("unusual for this ticker" is the premise) and it recovers as the
crisis ages out of the 24h window, but it does mean this system reports the
*onset* of bad news, not its persistence. Pinned by
tests/test_calibration.py::test_a_sustained_spike_self_damps.

Cold start is seeded, never waited out.  At a 3-minute slide, MIN_BASELINE_SAMPLES
of 50 is 2.5 hours of warmup, and the obvious fallback during warmup — the
absolute 0.7 — is exactly the constant we measured firing 13.5% of the time.
That would spam alerts through the first hours of every fresh `make demo`.
Instead tools/seed_baseline.py pre-populates from the last 24h of real news, and
until a ticker has enough samples we stay SILENT.  A missed alert in the first
minutes is far cheaper than forty false ones.
"""
from dataclasses import dataclass
from typing import List, Optional, Sequence
import time
import uuid

from . import config


def _key(ticker: str) -> str:
    return f"base:{ticker}"


def _member(ts: float, risk_score: float) -> str:
    """Encode one observation as a unique sorted-set member.

    The uniquifier is required, not cosmetic: sorted-set members are a set, so
    two windows that produce an identical (timestamp, score) pair would collapse
    into one entry and silently under-count the baseline — which in turn keeps
    `threshold()` returning None and suppresses alerting forever.
    """
    return f"{ts:.6f}:{risk_score:.6f}:{uuid.uuid4().hex[:8]}"


def _score_of(member: str) -> Optional[float]:
    parts = member.split(":")
    if len(parts) < 2:
        return None
    try:
        return float(parts[1])
    except ValueError:
        return None


def percentile(values: Sequence[float], p: float) -> float:
    """Linear-interpolated percentile. p in [0, 100]."""
    if not values:
        raise ValueError("percentile of empty sequence")
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (p / 100.0) * (len(ordered) - 1)
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    frac = rank - low
    return ordered[low] + (ordered[high] - ordered[low]) * frac


@dataclass
class BaselineDecision:
    should_alert: bool
    reason: str                 # fired | warming_up | below_baseline | too_few_mentions
    threshold: Optional[float]
    samples: int
    percentile_used: float


class BaselineStore:
    """Trailing-window score distribution, one sorted set per ticker.

    Redis sorted sets are keyed by member and ordered by score.  We want the
    reverse (ordered by *time*, valued by risk), so the member encodes the
    window end and the score is the observation time — that makes trimming by
    age a single ZREMRANGEBYSCORE.  The risk value rides along in the member
    string and is parsed back out.
    """

    def __init__(self, redis_client, window_hours: int = None, percentile_p: float = None,
                 min_samples: int = None):
        self.redis = redis_client
        self.window_hours = window_hours if window_hours is not None else config.BASELINE_WINDOW_HOURS
        self.percentile_p = percentile_p if percentile_p is not None else config.ALERT_PERCENTILE
        self.min_samples = min_samples if min_samples is not None else config.MIN_BASELINE_SAMPLES

    # -- writes ----------------------------------------------------------
    def record(self, ticker: str, risk_score: float, observed_at: Optional[float] = None) -> None:
        """Add one window observation and trim anything older than the window."""
        ts = observed_at if observed_at is not None else time.time()
        cutoff = ts - self.window_hours * 3600
        pipe = self.redis.pipeline()
        pipe.zadd(_key(ticker), {_member(ts, risk_score): ts})
        pipe.zremrangebyscore(_key(ticker), "-inf", cutoff)
        pipe.execute()

    def record_many(self, ticker: str, observations: Sequence[tuple]) -> int:
        """Bulk seed: observations is a sequence of (timestamp, risk_score)."""
        if not observations:
            return 0
        mapping = {_member(ts, score): ts for ts, score in observations}
        newest = max(mapping.values())
        cutoff = newest - self.window_hours * 3600
        pipe = self.redis.pipeline()
        pipe.zadd(_key(ticker), mapping)
        pipe.zremrangebyscore(_key(ticker), "-inf", cutoff)
        pipe.execute()
        return len(mapping)

    # -- reads -----------------------------------------------------------
    def scores(self, ticker: str) -> List[float]:
        cutoff = time.time() - self.window_hours * 3600
        raw = self.redis.zrangebyscore(_key(ticker), cutoff, "+inf")
        out = []
        for item in raw:
            if isinstance(item, bytes):
                item = item.decode()
            value = _score_of(item)
            if value is not None:
                out.append(value)
        return out

    def sample_count(self, ticker: str) -> int:
        return len(self.scores(ticker))

    def threshold(self, ticker: str) -> Optional[float]:
        """Current alert cut for a ticker, or None while warming up."""
        vals = self.scores(ticker)
        if len(vals) < self.min_samples:
            return None
        return percentile(vals, self.percentile_p)

    # -- decision --------------------------------------------------------
    def evaluate(self, ticker: str, risk_score: float, total_mentions: int) -> BaselineDecision:
        if total_mentions < config.MIN_MENTIONS_FOR_ALERT:
            # A single headline must never raise an alert. This is the specific
            # defect of the old formula, pinned by a regression test.
            return BaselineDecision(False, "too_few_mentions", None, 0, self.percentile_p)

        vals = self.scores(ticker)
        if len(vals) < self.min_samples:
            return BaselineDecision(False, "warming_up", None, len(vals), self.percentile_p)

        cut = percentile(vals, self.percentile_p)
        if risk_score > cut:
            return BaselineDecision(True, "fired", cut, len(vals), self.percentile_p)
        return BaselineDecision(False, "below_baseline", cut, len(vals), self.percentile_p)
