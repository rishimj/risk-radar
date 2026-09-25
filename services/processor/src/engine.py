"""The lite stream engine: flink/job/news_job.py's topology in plain Python.

Same stages, same order, same semantics:

    news.raw
      -> micro-batch enrichment (ENRICH_BATCH_SIZE articles OR ENRICH_BATCH_TIMEOUT_MS)
      -> news.enriched + redis headlines + per-ticker mentions
      -> bounded-out-of-orderness watermark (WATERMARK_DELAY_SECONDS)
      -> per-ticker sliding event-time windows (WINDOW_SIZE / WINDOW_SLIDE)
      -> alert evaluation, THEN feature/baseline write

Every outcome-deciding step is delegated to riskcore (stages, risk, windows), the
same code the Flink job calls. What this module owns is only the scheduling that
Flink would otherwise provide, reimplemented to match Flink's rules:

  * Window assignment: riskcore.windows.assign_windows, already pinned to
    Flink's SlidingEventTimeWindows by tests/test_windows.py.
  * Watermark: BoundedOutOfOrdernessWatermarks emits max_ts - delay - 1.
  * Lateness: a window is late once window.maxTimestamp() (end - 1) <= the
    current watermark (allowed lateness 0); late elements are dropped from it.
  * Firing: EventTimeTrigger fires a window when watermark >= end - 1, then
    the window's state is purged.

Idleness is deliberately NOT turned into processing-time advancement. With one
input channel, Flink's `with_idleness` does not move the watermark either, so a
quiet stretch leaves the newest windows open until newer news arrives. Keeping
that identical means simulate.py's watermark filler, and every calibration
number, apply to both engines unchanged.

The engine is I/O-free: callers inject the enrich call and the sinks, and pass
the clock in. That is what makes it testable without Kafka.
"""
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Sequence, Tuple
import json
import logging

from riskcore import stages
from riskcore.models import CompanyMention, EnrichedArticle, NewsArticle, RiskFeatures
from riskcore.risk import build_features, explode_mentions
from riskcore.windows import Window, assign_windows

log = logging.getLogger("processor.engine")

# Flink's initial watermark is Long.MIN_VALUE; anything below every real epoch works.
NO_WATERMARK = -(2 ** 63)


@dataclass
class EngineStats:
    raw_in: int = 0
    batches: int = 0
    enriched: int = 0
    enrich_dropped: int = 0
    mentions: int = 0
    late_dropped: int = 0
    windows_fired: int = 0
    stage_errors: int = 0

    def as_dict(self) -> Dict[str, int]:
        return dict(self.__dict__)


class StreamEngine:
    def __init__(
        self,
        enrich: Callable[[Sequence[NewsArticle]], List[EnrichedArticle]],
        on_enriched: Callable[[str], None],
        on_headline: Callable[[Dict], None],
        on_window: Callable[[RiskFeatures], None],
        *,
        size_ms: int,
        slide_ms: int,
        watermark_delay_ms: int,
        batch_size: int,
        batch_timeout_ms: int,
    ):
        self._enrich = enrich
        self._on_enriched = on_enriched
        self._on_headline = on_headline
        self._on_window = on_window

        self.size_ms = size_ms
        self.slide_ms = slide_ms
        self.watermark_delay_ms = watermark_delay_ms
        self.batch_size = batch_size
        self.batch_timeout_ms = batch_timeout_ms

        self._buffer: List[str] = []
        self._flush_at: Optional[int] = None          # the processing-time "timer"
        self._max_ts: Optional[int] = None
        self._watermark = NO_WATERMARK
        self._windows: Dict[Tuple[str, Window], List[CompanyMention]] = defaultdict(list)
        self.stats = EngineStats()

    # -- introspection ---------------------------------------------------
    @property
    def watermark(self) -> int:
        return self._watermark

    @property
    def watermark_ms(self) -> Optional[int]:
        """The watermark, or None before the first event (for /stats)."""
        return None if self._watermark == NO_WATERMARK else self._watermark

    @property
    def buffered(self) -> int:
        return len(self._buffer)

    @property
    def open_windows(self) -> int:
        return len(self._windows)

    # -- stage 1: micro-batch --------------------------------------------
    def offer(self, raw: str, now_ms: int) -> None:
        """One news.raw record. Mirrors MicroBatchEnrich.process_element."""
        self.stats.raw_in += 1
        self._buffer.append(raw)
        if len(self._buffer) >= self.batch_size:
            self._flush()
        elif self._flush_at is None:
            self._flush_at = now_ms + self.batch_timeout_ms

    def tick(self, now_ms: int) -> None:
        """Advance processing time. Mirrors MicroBatchEnrich.on_timer."""
        if self._flush_at is not None and now_ms >= self._flush_at:
            self._flush()

    def _flush(self) -> None:
        pending, self._buffer, self._flush_at = self._buffer, [], None
        if not pending:
            return
        self.stats.batches += 1

        articles = stages.decode_articles(pending)
        enriched = self._enrich(articles) if articles else []
        self.stats.enriched += len(enriched)
        self.stats.enrich_dropped += len(pending) - len(enriched)

        for article in enriched:
            value = article.to_json()
            self._side_effect("news.enriched", self._on_enriched, value)
            doc = json.loads(value)
            self._side_effect("headline", self._on_headline, doc)
            for mention in explode_mentions(doc):
                self._assign(mention)

        # Flink emits the watermark periodically, so every element of a batch
        # is judged against the watermark from BEFORE the batch; only then does
        # it advance.
        self._advance_watermark()

    # -- stage 2: event time ---------------------------------------------
    def _assign(self, mention: CompanyMention) -> None:
        self.stats.mentions += 1
        ts = mention.event_ts
        self._max_ts = ts if self._max_ts is None else max(self._max_ts, ts)

        placed = False
        for window in assign_windows(ts, self.size_ms, self.slide_ms):
            if window.end_ms - 1 <= self._watermark:
                continue                               # late for this window
            self._windows[(mention.ticker, window)].append(mention)
            placed = True
        if not placed:
            self.stats.late_dropped += 1

    def _advance_watermark(self) -> None:
        if self._max_ts is None:
            return
        candidate = self._max_ts - self.watermark_delay_ms - 1
        if candidate > self._watermark:
            self._watermark = candidate
            self._fire_due()

    # -- stage 3: windows ------------------------------------------------
    def _fire_due(self) -> None:
        due = sorted(
            (k for k in self._windows if k[1].end_ms - 1 <= self._watermark),
            key=lambda k: (k[1].end_ms, k[0]),
        )
        for key in due:
            ticker, window = key
            mentions = self._windows.pop(key)
            feats = build_features(
                ticker, mentions,
                datetime.fromtimestamp(window.start_ms / 1000, tz=timezone.utc),
                datetime.fromtimestamp(window.end_ms / 1000, tz=timezone.utc),
            )
            self.stats.windows_fired += 1
            self._side_effect("window", self._on_window, feats)

    def _side_effect(self, name: str, fn: Callable, arg) -> None:
        """Like each Flink map() operator: log and carry on, never stop the stream."""
        try:
            fn(arg)
        except Exception as exc:                       # noqa: BLE001
            self.stats.stage_errors += 1
            log.error("%s stage failed: %s", name, exc)


def window_sink(redis_client, store) -> Callable[[RiskFeatures], None]:
    """Alert FIRST, then record: see stages.evaluate_and_alert for why."""
    def sink(feats: RiskFeatures) -> None:
        try:
            stages.evaluate_and_alert(redis_client, store, feats)
        except Exception as exc:                       # noqa: BLE001
            log.error("alert fan-out failed: %s", exc)
        stages.write_features(redis_client, store, feats)
    return sink
