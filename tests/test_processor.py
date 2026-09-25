"""The lite engine must schedule exactly like the Flink job, or the two lie differently.

Outcome logic is shared (riskcore.stages), so these tests pin the part the lite
engine reimplements: micro-batch flushing, the bounded-out-of-orderness
watermark, lateness, and sliding-window firing.
"""
from datetime import datetime, timedelta, timezone
from typing import List

import fakeredis
import pytest

from riskcore.baseline import BaselineStore
from riskcore.models import EnrichedArticle, NewsArticle, RiskFeatures, iso
from riskcore.windows import assign_windows

from conftest import load_service_module

engine_mod = load_service_module("processor", "engine")
StreamEngine = engine_mod.StreamEngine

T0 = datetime(2026, 7, 27, 12, 0, tzinfo=timezone.utc)
SIZE, SLIDE, DELAY = 900_000, 180_000, 30_000


def raw(minutes: float, ticker: str = "TSLA", sentiment: float = -0.5, aid: str = None) -> str:
    aid = aid or f"{ticker}-{minutes}"
    return NewsArticle(
        article_id=aid, title=f"{ticker}|{sentiment}", url=f"https://x/{aid}",
        source="t", published_at=iso(T0 + timedelta(minutes=minutes)),
    ).to_json()


def fake_enrich(articles) -> List[EnrichedArticle]:
    """Encodes ticker|sentiment in the title so tests control both."""
    out = []
    for a in articles:
        ticker, sentiment = a.title.split("|")
        out.append(EnrichedArticle(
            article_id=a.article_id, title=a.title, url=a.url, source=a.source,
            published_at=a.published_at, sentiment=float(sentiment),
            companies=[{"ticker": ticker, "role": "primary"}], model="fake",
        ))
    return out


class Harness:
    def __init__(self, batch_size=5, timeout_ms=1000, enrich=fake_enrich):
        self.enriched: List[str] = []
        self.headlines: List[dict] = []
        self.windows: List[RiskFeatures] = []
        self.calls = 0

        def counting_enrich(articles):
            self.calls += 1
            return enrich(articles)

        self.engine = StreamEngine(
            enrich=counting_enrich,
            on_enriched=self.enriched.append,
            on_headline=self.headlines.append,
            on_window=self.windows.append,
            size_ms=SIZE, slide_ms=SLIDE, watermark_delay_ms=DELAY,
            batch_size=batch_size, batch_timeout_ms=timeout_ms,
        )

    def feed(self, *values, now=0):
        for v in values:
            self.engine.offer(v, now)
        self.engine.tick(10 ** 15)                     # expire the flush timer


# -- stage 1: micro-batching ------------------------------------------------
def test_flushes_on_batch_size_without_waiting_for_the_timer():
    h = Harness(batch_size=3)
    for i in range(3):
        h.engine.offer(raw(i), now_ms=0)
    assert h.calls == 1 and len(h.enriched) == 3
    assert h.engine.buffered == 0


def test_flushes_on_timeout_when_traffic_is_thin():
    h = Harness(batch_size=5, timeout_ms=1000)
    h.engine.offer(raw(0), now_ms=10_000)
    h.engine.tick(10_999)
    assert h.calls == 0
    h.engine.tick(11_000)
    assert h.calls == 1 and len(h.enriched) == 1


def test_timer_is_armed_by_the_first_buffered_article_not_the_last():
    h = Harness(batch_size=5, timeout_ms=1000)
    h.engine.offer(raw(0), now_ms=0)
    h.engine.offer(raw(1), now_ms=900)
    h.engine.tick(1000)
    assert h.calls == 1 and len(h.enriched) == 2


def test_enrichment_outage_drops_the_batch_but_keeps_running():
    h = Harness(batch_size=2, enrich=lambda a: [])
    h.feed(raw(0), raw(1))
    assert h.enriched == [] and h.engine.stats.enrich_dropped == 2
    h.engine._enrich = fake_enrich
    h.feed(raw(2), raw(3))
    assert len(h.enriched) == 2


def test_every_enriched_article_reaches_topic_and_headlines():
    h = Harness(batch_size=2)
    h.feed(raw(0), raw(1))
    assert len(h.enriched) == len(h.headlines) == 2


def test_headline_row_is_what_the_dashboard_reads():
    from riskcore import stages
    doc = {"article_id": "a", "title": "t", "url": "u", "source": "s",
           "published_at": "p", "sentiment": -0.2, "companies": [], "model": "m"}
    assert set(stages.headline_row(doc)) == {"article_id", "title", "url", "source",
                                             "published_at", "sentiment", "companies"}


def test_a_failing_sink_does_not_stop_the_stream():
    h = Harness(batch_size=1)

    def boom(_):
        raise RuntimeError("redis down")

    h.engine._on_headline = boom
    h.feed(raw(0))
    h.feed(raw(1))
    assert len(h.enriched) == 2 and h.engine.stats.stage_errors == 2


# -- stage 2/3: watermark and windows ---------------------------------------
def test_watermark_is_max_event_time_minus_delay_minus_one():
    h = Harness(batch_size=1)
    h.feed(raw(10))
    ts = int((T0 + timedelta(minutes=10)).timestamp() * 1000)
    assert h.engine.watermark == ts - DELAY - 1


def test_watermark_never_moves_backwards():
    h = Harness(batch_size=1)
    h.feed(raw(10))
    wm = h.engine.watermark
    h.feed(raw(5))
    assert h.engine.watermark == wm


def test_no_window_fires_until_the_watermark_passes_its_end():
    h = Harness(batch_size=1)
    h.feed(raw(0))
    assert h.windows == []
    assert h.engine.open_windows == SIZE // SLIDE


def test_windows_fire_once_event_time_moves_past_them():
    """The simulate.py trick: later news for ANY ticker closes TSLA's windows."""
    h = Harness(batch_size=1)
    h.feed(raw(0, "TSLA", -0.9))
    h.feed(raw(30, "NVDA", 0.1))                       # 30 min later
    tsla = [w for w in h.windows if w.ticker == "TSLA"]
    assert len(tsla) == SIZE // SLIDE                  # every window containing it
    assert all(w.total_mentions == 1 for w in tsla)


def test_fired_windows_match_flinks_assignment():
    h = Harness(batch_size=1)
    h.feed(raw(0))
    h.feed(raw(60, "NVDA"))
    ts = int(T0.timestamp() * 1000)
    expected = [iso(datetime.fromtimestamp(w.end_ms / 1000, tz=timezone.utc))
                for w in assign_windows(ts, SIZE, SLIDE)]
    assert sorted(w.window_end for w in h.windows if w.ticker == "TSLA") == expected


def test_windows_fire_in_event_time_order():
    h = Harness(batch_size=1)
    h.feed(raw(0))
    h.feed(raw(60, "NVDA"))
    ends = [w.window_end for w in h.windows]
    assert ends == sorted(ends)


def test_fired_window_state_is_purged():
    h = Harness(batch_size=1)
    h.feed(raw(0))
    h.feed(raw(60, "NVDA"))
    tsla_open = [k for k in h.engine._windows if k[0] == "TSLA"]
    assert tsla_open == []


def test_out_of_order_within_the_delay_is_kept():
    h = Harness(batch_size=1)
    h.feed(raw(10))
    h.feed(raw(9.7))                                   # 18s late < 30s delay
    assert h.engine.stats.late_dropped == 0


def test_element_late_for_every_window_is_dropped():
    h = Harness(batch_size=1)
    h.feed(raw(60))
    h.feed(raw(0))                                     # an hour behind the watermark
    assert h.engine.stats.late_dropped == 1


def test_element_late_for_some_windows_still_lands_in_the_open_ones():
    h = Harness(batch_size=1)
    h.feed(raw(0))
    h.feed(raw(4))                    # watermark now just under 3:30 -> [.., 3:00) closed
    fired_before = len(h.windows)
    h.feed(raw(1))                    # its window ending 3:00 is closed; later ones open
    assert h.engine.stats.late_dropped == 0
    h.feed(raw(60, "NVDA"))
    ends_with_two = [w for w in h.windows[fired_before:]
                     if w.ticker == "TSLA" and w.total_mentions >= 2]
    assert ends_with_two


def test_a_batch_is_judged_against_the_watermark_from_before_it():
    """Flink emits watermarks periodically, not per element within a batch."""
    h = Harness(batch_size=2)
    h.feed(raw(60), raw(0))           # same batch: the 0-minute article is NOT late
    assert h.engine.stats.late_dropped == 0


def test_windows_are_per_ticker():
    h = Harness(batch_size=1)
    h.feed(raw(0, "TSLA", -0.9))
    h.feed(raw(0.5, "AAPL", 0.9))
    h.feed(raw(60, "NVDA"))
    assert {w.ticker for w in h.windows} >= {"TSLA", "AAPL"}
    assert all(w.total_mentions == 1 for w in h.windows if w.ticker in {"TSLA", "AAPL"})


# -- window sink: alert BEFORE baseline write -------------------------------
def test_window_sink_evaluates_before_recording(monkeypatch):
    """Regression for handoff bug #6, now enforced for the lite engine too."""
    from riskcore import stages

    r = fakeredis.FakeRedis()
    store = BaselineStore(r, window_hours=24, percentile_p=99, min_samples=50)
    for _ in range(100):
        store.record("TSLA", 0.3)

    seen = {}

    def fake_fan_out(alert):
        seen["samples_at_decision"] = store.sample_count("TSLA")

    monkeypatch.setattr("riskcore.alerting.fan_out", fake_fan_out)
    feats = RiskFeatures(ticker="TSLA", window_start="a", window_end="b",
                         risk_score=1.0, sentiment_score=-0.9, neg_count=10,
                         pos_count=0, total_mentions=10, alert_score=1.8)
    engine_mod.window_sink(r, store)(feats)

    assert seen["samples_at_decision"] == 100          # not yet folded in
    assert store.sample_count("TSLA") == 101           # folded in afterwards
    assert r.get("feat:TSLA:latest") is not None


def test_simulated_crisis_alerts_end_to_end_through_the_engine(monkeypatch):
    """simulate.py's exact article shapes, through the engine, into an alert.

    The baseline is saturated the way the handoff measured it (p99 == 1.0 on the
    capped gauge), which is the state in which nothing could ever fire before.
    """
    r = fakeredis.FakeRedis()
    store = BaselineStore(r, window_hours=24, percentile_p=99, min_samples=50)
    for i in range(219):
        store.record("TSLA", 0.30 + (i % 40) * 0.015)
    for _ in range(4):
        store.record("TSLA", 1.0)                      # old capped samples at the cut

    fired = []
    monkeypatch.setattr("riskcore.alerting.fan_out", fired.append)

    h = Harness(batch_size=5)
    h.engine._on_window = engine_mod.window_sink(r, store)
    crisis = [raw(0, "TSLA", -0.926, aid=f"c{i}") for i in range(10)]
    filler_min = (SLIDE + DELAY) / 60_000 + 0.5
    filler = [raw(filler_min, "AAPL", 0.1, aid=f"f{i}") for i in range(5)]
    h.feed(*crisis)
    h.feed(*filler)

    assert len(fired) == 1                             # cooldown collapses the 5 windows
    assert fired[0].ticker == "TSLA" and fired[0].risk_score == 1.0
