"""Adaptive baseline: percentile maths, cold-start silence, trailing trim."""
import time

import fakeredis
import pytest

from riskcore.baseline import BaselineStore, percentile


@pytest.fixture
def redis_client():
    return fakeredis.FakeRedis()


@pytest.fixture
def store(redis_client):
    return BaselineStore(redis_client, window_hours=24, percentile_p=99, min_samples=50)


# -- percentile -------------------------------------------------------------
def test_percentile_endpoints():
    vals = list(range(101))          # 0..100
    assert percentile(vals, 0) == 0
    assert percentile(vals, 100) == 100
    assert percentile(vals, 50) == pytest.approx(50)


def test_percentile_interpolates():
    assert percentile([0.0, 1.0], 50) == pytest.approx(0.5)


def test_percentile_single_value():
    assert percentile([0.42], 99) == 0.42


def test_percentile_of_empty_raises():
    with pytest.raises(ValueError):
        percentile([], 99)


# -- cold start -------------------------------------------------------------
def test_stays_silent_while_warming_up(store):
    """The whole point: no fallback to a known-bad absolute threshold."""
    for _ in range(10):
        store.record("TSLA", 0.4)
    decision = store.evaluate("TSLA", 0.99, total_mentions=5)
    assert decision.should_alert is False
    assert decision.reason == "warming_up"
    assert decision.threshold is None


def test_threshold_is_none_until_min_samples(store):
    for _ in range(49):
        store.record("TSLA", 0.4)
    assert store.threshold("TSLA") is None
    store.record("TSLA", 0.4)
    assert store.threshold("TSLA") is not None


# -- firing -----------------------------------------------------------------
def test_fires_above_own_baseline(store):
    for i in range(200):
        store.record("TSLA", 0.30 + (i % 10) * 0.01)     # 0.30 .. 0.39
    decision = store.evaluate("TSLA", 0.95, total_mentions=4)
    assert decision.should_alert is True
    assert decision.reason == "fired"
    assert decision.threshold < 0.95


def test_does_not_fire_inside_the_normal_range(store):
    for i in range(200):
        store.record("TSLA", 0.30 + (i % 10) * 0.01)
    decision = store.evaluate("TSLA", 0.35, total_mentions=4)
    assert decision.should_alert is False
    assert decision.reason == "below_baseline"


def test_baseline_is_per_ticker(store):
    """A quiet ticker and a noisy one must not share a cut."""
    for _ in range(200):
        store.record("AAPL", 0.90)      # habitually high
        store.record("TSLA", 0.10)      # habitually low
    assert store.evaluate("AAPL", 0.60, total_mentions=4).should_alert is False
    assert store.evaluate("TSLA", 0.60, total_mentions=4).should_alert is True


def test_single_mention_never_alerts(store):
    """Regression for the old formula's defining defect."""
    for _ in range(200):
        store.record("TSLA", 0.10)
    decision = store.evaluate("TSLA", 1.0, total_mentions=1)
    assert decision.should_alert is False
    assert decision.reason == "too_few_mentions"


# -- trimming ---------------------------------------------------------------
def test_observations_older_than_the_window_are_dropped(store):
    now = time.time()
    store.record_many("TSLA", [(now - 30 * 3600, 0.5)] * 1)   # 30h old
    store.record("TSLA", 0.6, observed_at=now)
    assert store.sample_count("TSLA") == 1


def test_identical_observations_do_not_collapse(store):
    """Regression: sorted-set members are a SET.

    Encoding an observation as just "<ts>:<score>" meant two windows producing
    the same pair collapsed into one entry, silently under-counting the baseline
    — which keeps threshold() at None and suppresses alerting forever.
    """
    now = time.time()
    for _ in range(60):
        store.record("TSLA", 0.4, observed_at=now)     # identical ts AND score
    assert store.sample_count("TSLA") == 60
    assert store.threshold("TSLA") is not None


def test_record_many_bulk_seeds(store):
    now = time.time()
    n = store.record_many("TSLA", [(now - i * 60, 0.2 + i * 0.001) for i in range(120)])
    assert n == 120
    assert store.sample_count("TSLA") == 120
    assert store.threshold("TSLA") is not None


def test_saturated_baseline_still_fires_on_a_worse_window(store):
    """Regression for the open decision in the 2026-07-27 handoff.

    A freshly seeded TSLA baseline held 4/223 samples at the 1.0 cap, so p99 was
    exactly 1.0 and a maxed-out window (1.0 > 1.0) could never fire. Baselines
    now store the uncapped alert score, so the capped dips sit at their true
    values (here 1.2-1.3) and a real crisis (~1.8) clears them.
    """
    from riskcore.risk import Aggregation
    for i in range(219):
        store.record("TSLA", 0.30 + (i % 40) * 0.015)    # ordinary windows
    for v in (1.2, 1.25, 1.28, 1.3):
        store.record("TSLA", v)                           # dips that hit the cap
    crisis = Aggregation()
    for _ in range(10):
        crisis.add(-0.926)                                # the measured FinBERT crisis
    assert crisis.risk() == 1.0
    decision = store.evaluate("TSLA", crisis.alert_score(), total_mentions=10)
    assert decision.should_alert is True
    assert decision.threshold < crisis.alert_score()
