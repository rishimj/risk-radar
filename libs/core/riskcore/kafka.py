"""Thin Kafka producer helper.

Replaces the old repo's Go gateway entirely — ingestion and the webapp's
simulate endpoint are the only producers, and both just need "send a JSON line".
kafka-python is used rather than confluent-kafka because it is pure Python and
so installs cleanly on both arm64 and amd64 without a librdkafka build.
"""
from typing import Callable, Iterable, Optional
import json
import logging
import threading

from kafka import KafkaProducer
from kafka.errors import KafkaError

from . import config

log = logging.getLogger(__name__)

_producer: Optional[KafkaProducer] = None
_lock = threading.Lock()

# When set, every send goes here instead of Kafka: (topic, value, key) -> None.
# services/standalone uses it to run the whole pipeline in one process.
_local_sink: Optional[Callable[[str, str, Optional[str]], None]] = None


def set_local_sink(sink: Optional[Callable[[str, str, Optional[str]], None]]) -> None:
    global _local_sink
    _local_sink = sink


def producer() -> KafkaProducer:
    global _producer
    with _lock:
        if _producer is None:
            _producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_BOOTSTRAP.split(","),
                value_serializer=lambda v: v.encode() if isinstance(v, str) else v,
                key_serializer=lambda k: k.encode() if isinstance(k, str) else k,
                acks="all",
                retries=5,
                linger_ms=50,
                max_block_ms=20000,
            )
        return _producer


def send(topic: str, value: str, key: Optional[str] = None) -> None:
    if _local_sink is not None:
        _local_sink(topic, value, key)
        return
    producer().send(topic, value=value, key=key)


def send_many(topic: str, values: Iterable[str]) -> int:
    if _local_sink is not None:
        n = 0
        for v in values:
            _local_sink(topic, v, None)
            n += 1
        return n
    p = producer()
    n = 0
    for v in values:
        p.send(topic, value=v)
        n += 1
    p.flush(timeout=30)
    return n


def flush(timeout: float = 30.0) -> None:
    if _local_sink is None and _producer is not None:
        _producer.flush(timeout=timeout)


def close() -> None:
    global _producer
    with _lock:
        if _producer is not None:
            try:
                _producer.flush(timeout=10)
                _producer.close(timeout=10)
            except KafkaError as exc:
                log.warning("kafka close: %s", exc)
            _producer = None
