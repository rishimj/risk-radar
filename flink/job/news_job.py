"""RiskRadar streaming job.

    news.raw
      -> keyed micro-batch enrichment (5 articles OR a 1s processing-time timer)
      -> news.enriched + redis headlines + per-ticker mentions
      -> watermarks (30s out-of-orderness, 30s idleness)
      -> key_by(ticker), sliding 15m/3m window, risk scoring
      -> redis features + baseline + alert fan-out

The per-record logic (enrichment call, headline row, alert decision, feature
write) lives in riskcore.stages and is shared with the pure-Python "lite"
engine in services/processor, so the two cannot disagree on outcomes.

Three things here are deliberate corrections of the old repo's job, which could
never have run:

1.  NO SinkFunction SUBCLASSES.  `pyflink.datastream.functions.SinkFunction`
    wraps a *Java* object (`__init__(self, sink_func: Union[str, JavaObject])`);
    PyFlink has no user-defined Python sinks.  The old RedisFeatureSink and
    AlertingSink subclassed it without calling super().__init__, so add_sink()
    failed at graph-construction time.  All side effects here live in map()
    operators, which is the supported way.

2.  KeyedProcessFunction, not ProcessFunction, for the micro-batch.  Only
    KeyedProcessFunction has on_timer() — a plain ProcessFunction has no timer
    service, so the old batch never flushed on time, only when the next message
    happened to arrive.

3.  Watermarks are assigned AFTER enrichment, on the mentions stream, by
    re-reading event_ts from the payload.  Assigning at the source would be
    wrong: records emitted from a processing-time timer callback carry no
    timestamp, and the downstream window would reject them.
"""
from typing import Iterable, List, Optional
import json
import logging
import os

from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import (
    CheckpointingMode, KeyedProcessFunction, ProcessWindowFunction, RuntimeContext,
    StreamExecutionEnvironment,
)
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee, KafkaOffsetsInitializer, KafkaRecordSerializationSchema,
    KafkaSink, KafkaSource,
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.common.time import Time

from riskcore import config
from riskcore.models import CompanyMention, RiskFeatures

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("news_job")


# ===========================================================================
# stage 1 — micro-batched enrichment
# ===========================================================================
class MicroBatchEnrich(KeyedProcessFunction):
    """Buffer articles and enrich them in one HTTP call.

    Flushes when the buffer reaches ENRICH_BATCH_SIZE, or when a processing-time
    timer fires ENRICH_BATCH_TIMEOUT_MS after the first buffered article —
    whichever comes first. The timer is what makes latency bounded when traffic
    is thin, which at ~2.6 articles/minute is most of the time.
    """

    def __init__(self):
        self.buffer = None
        self.timer = None
        self.session = None

    def open(self, runtime_context: RuntimeContext):
        self.buffer = runtime_context.get_list_state(
            ListStateDescriptor("enrich_buffer", Types.STRING())
        )
        self.timer = runtime_context.get_state(
            ValueStateDescriptor("flush_timer", Types.LONG())
        )
        import requests
        self.session = requests.Session()

    def process_element(self, value, ctx):
        self.buffer.add(value)
        pending = list(self.buffer.get())

        if len(pending) >= config.ENRICH_BATCH_SIZE:
            self._cancel_timer(ctx)
            yield from self._flush(pending)
            return

        if self.timer.value() is None:
            fire_at = ctx.timer_service().current_processing_time() + config.ENRICH_BATCH_TIMEOUT_MS
            ctx.timer_service().register_processing_time_timer(fire_at)
            self.timer.update(fire_at)

    def on_timer(self, timestamp, ctx):
        pending = list(self.buffer.get())
        self.timer.clear()
        if pending:
            yield from self._flush(pending)

    def _cancel_timer(self, ctx):
        existing = self.timer.value()
        if existing is not None:
            try:
                ctx.timer_service().delete_processing_time_timer(existing)
            except Exception:                          # noqa: BLE001
                pass
            self.timer.clear()

    def _flush(self, pending: List[str]) -> Iterable[str]:
        from riskcore import stages

        self.buffer.clear()
        for enriched in stages.enrich_batch(self.session, stages.decode_articles(pending)):
            yield enriched.to_json()


# ===========================================================================
# stage 2 — fan-out helpers
# ===========================================================================
class HeadlinesWriter:
    """Push enriched articles onto the Redis ring buffer the dashboard reads."""

    def __init__(self):
        self._redis = None

    def __call__(self, value: str) -> str:
        try:
            if self._redis is None:
                import redis
                self._redis = redis.from_url(config.REDIS_URL)
            from riskcore import stages
            stages.write_headline(self._redis, json.loads(value))
        except Exception as exc:                       # noqa: BLE001
            log.error("headline write failed: %s", exc)
        return value


def to_mentions(value: str) -> Iterable[str]:
    """Explode one enriched article into one record per matched ticker.

    The logic itself lives in riskcore.risk so the offline calibration harness
    buckets exactly what this pipeline buckets.
    """
    from riskcore.risk import explode_mentions
    try:
        doc = json.loads(value)
    except Exception:                                  # noqa: BLE001
        return
    for mention in explode_mentions(doc):
        yield mention.to_json()


class MentionTimestampAssigner(TimestampAssigner):
    """Read event time back out of the payload.

    This is why watermarking happens here and not at the source: records emitted
    from a processing-time timer have no attached timestamp, so the only
    reliable event time is the one carried in the JSON.
    """

    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        try:
            return int(json.loads(value)["event_ts"])
        except Exception:                              # noqa: BLE001
            return record_timestamp if record_timestamp > 0 else 0


# ===========================================================================
# stage 3 — windowed risk scoring
# ===========================================================================
class RiskWindow(ProcessWindowFunction):
    """Aggregate one ticker's mentions in one sliding window into RiskFeatures."""

    def process(self, key, context, elements):
        from datetime import datetime, timezone
        from riskcore.risk import build_features

        mentions = []
        for raw in elements:
            try:
                mentions.append(CompanyMention.from_json(raw))
            except Exception:                          # noqa: BLE001
                continue

        if not mentions:
            return

        window = context.window()
        start = datetime.fromtimestamp(window.start / 1000, tz=timezone.utc)
        end = datetime.fromtimestamp(window.end / 1000, tz=timezone.utc)

        yield build_features(key, mentions, start, end).to_json()


class FeatureWriter:
    """Persist features to Redis and feed the ticker's rolling baseline."""

    def __init__(self):
        self._redis = None
        self._store = None

    def __call__(self, value: str) -> str:
        try:
            if self._redis is None:
                import redis
                from riskcore.baseline import BaselineStore
                self._redis = redis.from_url(config.REDIS_URL)
                self._store = BaselineStore(self._redis)

            from riskcore import stages
            stages.write_features(self._redis, self._store, RiskFeatures.from_json(value))
        except Exception as exc:                       # noqa: BLE001
            log.error("feature write failed: %s", exc)
        return value


class AlertFanout:
    """Decide against the ticker's own baseline, then fan out to subscribers."""

    def __init__(self):
        self._redis = None
        self._store = None
        self._ready = False

    def __call__(self, value: str) -> str:
        try:
            if self._redis is None:
                import redis
                from riskcore.baseline import BaselineStore
                from riskcore import db
                self._redis = redis.from_url(config.REDIS_URL)
                self._store = BaselineStore(self._redis)
                try:
                    db.ensure_tables()
                except Exception as exc:               # noqa: BLE001
                    log.warning("ensure_tables: %s", exc)

            from riskcore import stages
            stages.evaluate_and_alert(self._redis, self._store, RiskFeatures.from_json(value))
        except Exception as exc:                       # noqa: BLE001
            log.error("alert fan-out failed: %s", exc)
        return value


# ===========================================================================
# topology
# ===========================================================================
def build_pipeline(env: StreamExecutionEnvironment) -> None:
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(config.KAFKA_BOOTSTRAP)
        .set_topics(config.TOPIC_RAW)
        .set_group_id("riskradar-flink")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw = env.from_source(
        source, WatermarkStrategy.no_watermarks(), "news.raw"
    ).uid("kafka-source")

    # Single key: parallelism is 1 and we want one shared batch buffer. key_by is
    # required regardless — timers only exist on keyed streams.
    enriched = (
        raw.key_by(lambda _: "batch", key_type=Types.STRING())
           .process(MicroBatchEnrich(), output_type=Types.STRING())
           .name("micro-batch enrich")
           .uid("enrich")
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(config.KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(config.TOPIC_ENRICHED)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    enriched.sink_to(sink).name("news.enriched").uid("kafka-sink")

    enriched.map(HeadlinesWriter(), output_type=Types.STRING()) \
            .name("redis headlines").uid("headlines")

    mentions = enriched.flat_map(to_mentions, output_type=Types.STRING()) \
                       .name("to mentions").uid("mentions")

    watermarked = mentions.assign_timestamps_and_watermarks(
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(config.WATERMARK_DELAY_SECONDS))
        # Without idleness the window stalls whenever a quiet stretch stops
        # advancing the watermark — and quiet stretches are normal here.
        .with_idleness(Duration.of_seconds(30))
        # MUST be the last call in the chain. PyFlink keeps a Python assigner
        # on the Python wrapper only, and with_idleness() returns a NEW wrapper
        # around the Java strategy, silently dropping it. In the other order the
        # job fell back to Kafka record timestamps: windows were keyed on
        # produce time rather than published_at, disagreeing with the replay
        # harness, and simulate.py's watermark filler could never close a
        # window. Pinned by tests/test_flink_job.py.
        .with_timestamp_assigner(MentionTimestampAssigner())
    ).name("watermarks").uid("watermarks")

    features = (
        watermarked
        .key_by(lambda v: json.loads(v)["ticker"], key_type=Types.STRING())
        .window(SlidingEventTimeWindows.of(
            Time.seconds(config.WINDOW_SIZE_SECONDS),
            Time.seconds(config.WINDOW_SLIDE_SECONDS),
        ))
        .process(RiskWindow(), output_type=Types.STRING())
        .name(f"risk window {config.WINDOW_SIZE_SECONDS}s/{config.WINDOW_SLIDE_SECONDS}s")
        .uid("risk-window")
    )

    # Alert BEFORE the baseline write, not after. FeatureWriter.record() folds
    # this window's own risk score into the very distribution AlertFanout then
    # compares it against — a window cannot be an outlier relative to a sample
    # set it is already a member of. With a 900s/180s slide one spike emits 5
    # overlapping max-risk windows, which was enough to drag p99 to exactly 1.0;
    # since risk_score is capped at 1.0 and the test is a strict `>`, the ticker
    # then became permanently unable to alert. Evaluating first compares against
    # the trailing baseline, which is what "adaptive baseline" was meant to mean.
    (features
        .map(AlertFanout(), output_type=Types.STRING()).name("alert fan-out").uid("alerts")
        .map(FeatureWriter(), output_type=Types.STRING()).name("redis features").uid("features"))


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)                     # matches the single-partition topics

    env.enable_checkpointing(30_000, CheckpointingMode.EXACTLY_ONCE)
    checkpoint_dir = os.getenv("CHECKPOINT_DIR", "file:///tmp/flink-checkpoints")
    env.get_checkpoint_config().set_checkpoint_storage_dir(checkpoint_dir)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(10_000)

    build_pipeline(env)
    env.execute("riskradar-news")


if __name__ == "__main__":
    main()
