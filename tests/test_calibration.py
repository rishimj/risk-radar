"""The replay harness and the alert-policy simulation it drives.

These matter because the harness is what threshold decisions are made from. If
it buckets differently than the job, every calibration number is a lie.
"""
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import fakeredis
import pytest

TOOLS = Path(__file__).resolve().parents[1] / "tools"
sys.path.insert(0, str(TOOLS))

from riskcore.models import iso                      # noqa: E402
from riskcore.windows import Window                  # noqa: E402
from calibrate import simulate                       # noqa: E402
from replay import WindowScore, score_windows        # noqa: E402

T0 = datetime(2026, 7, 27, 12, 0, tzinfo=timezone.utc)
MIN = 60_000


def doc(minutes: float, sentiment: float, ticker: str = "TSLA", aid: str = "a"):
    return {
        "article_id": f"{aid}{minutes}",
        "title": f"{ticker} headline {minutes}",
        "url": f"https://example.com/{aid}{minutes}",
        "source": "Test",
        "published_at": iso(T0 + timedelta(minutes=minutes)),
        "sentiment": sentiment,
        "companies": [{"ticker": ticker, "role": "primary"}],
    }


def ws(ticker, end_ms, risk, mentions=3):
    return WindowScore(ticker=ticker, window=Window(end_ms - 15 * MIN, end_ms),
                       risk=risk, mentions=mentions, neg=mentions, pos=0,
                       top_headline="h")


# -- windowing --------------------------------------------------------------
def test_score_windows_uses_the_configured_geometry():
    out = score_windows([doc(0, -0.5)], size_s=900, slide_s=180)
    assert len(out) == 5                      # 900/180
    assert all(w.window.size_ms == 900_000 for w in out)


def test_article_counts_toward_every_window_containing_it():
    out = score_windows([doc(0, -0.5)], size_s=900, slide_s=180)
    assert all(w.mentions == 1 for w in out)


def test_windows_are_split_per_ticker():
    docs = [doc(0, -0.5, "TSLA"), doc(0, -0.5, "NVDA", aid="b")]
    out = score_windows(docs, size_s=900, slide_s=180)
    assert {w.ticker for w in out} == {"TSLA", "NVDA"}


def test_multi_ticker_article_lands_in_both_tickers_windows():
    d = doc(0, -0.8)
    d["companies"] = [{"ticker": "TSLA", "role": "primary"},
                      {"ticker": "NVDA", "role": "mentioned"}]
    out = score_windows([d], size_s=900, slide_s=180)
    assert {w.ticker for w in out} == {"TSLA", "NVDA"}


def test_negative_news_scores_higher_than_positive():
    neg = score_windows([doc(0, -0.9), doc(1, -0.8), doc(2, -0.85)], 900, 180)
    pos = score_windows([doc(0, 0.9), doc(1, 0.8), doc(2, 0.85)], 900, 180)
    assert max(w.risk for w in neg) > max(w.risk for w in pos)


def test_empty_corpus_yields_no_windows():
    assert score_windows([]) == []


def test_articles_with_no_companies_are_ignored():
    d = doc(0, -0.5)
    d["companies"] = []
    assert score_windows([d]) == []


# -- policy simulation ------------------------------------------------------
def test_no_alerts_before_min_samples():
    scores = [ws("TSLA", (i + 1) * 3 * MIN, 0.2) for i in range(20)]
    scores.append(ws("TSLA", 100 * 3 * MIN, 1.0))
    report = simulate(scores, p=99, min_samples=50, min_mentions=2, cooldown_s=600)
    assert report["TSLA"]["crossings"] == 0


def test_spike_after_warmup_fires():
    scores = [ws("TSLA", (i + 1) * 3 * MIN, 0.2) for i in range(60)]
    scores.append(ws("TSLA", 200 * 3 * MIN, 1.0))
    report = simulate(scores, p=99, min_samples=50, min_mentions=2, cooldown_s=600)
    assert report["TSLA"]["crossings"] == 1
    assert len(report["TSLA"]["alerts"]) == 1


def test_single_mention_window_cannot_fire():
    scores = [ws("TSLA", (i + 1) * 3 * MIN, 0.2) for i in range(60)]
    scores.append(ws("TSLA", 200 * 3 * MIN, 1.0, mentions=1))
    report = simulate(scores, p=99, min_samples=50, min_mentions=2, cooldown_s=600)
    assert report["TSLA"]["crossings"] == 0


def test_cooldown_collapses_a_burst_into_one_alert():
    scores = [ws("TSLA", (i + 1) * 3 * MIN, 0.2) for i in range(60)]
    # five consecutive high windows, 3 min apart, inside a 10 min cooldown
    scores += [ws("TSLA", (200 + i) * 3 * MIN, 1.0) for i in range(5)]
    report = simulate(scores, p=99, min_samples=50, min_mentions=2, cooldown_s=600)
    assert len(report["TSLA"]["alerts"]) == 1
    assert report["TSLA"]["crossings"] >= 1


def test_a_sustained_spike_self_damps():
    """An adaptive baseline stops firing on a condition that becomes the norm.

    Every window feeds the distribution, including the ones that fired, so a run
    of extreme windows drags the ticker's own percentile up to meet them. Five
    consecutive 1.0s produce far fewer than five crossings.

    This is intended: 'unusual for this ticker' is the whole premise, and the
    24h trailing window means it recovers once the crisis ages out. Cooldown and
    this effect are belt-and-braces against alert storms.
    """
    scores = [ws("TSLA", (i + 1) * 3 * MIN, 0.2) for i in range(60)]
    scores += [ws("TSLA", (200 + i) * 3 * MIN, 1.0) for i in range(5)]
    report = simulate(scores, p=99, min_samples=50, min_mentions=2, cooldown_s=0)
    assert 1 <= report["TSLA"]["crossings"] < 5


def test_a_lower_percentile_fires_more():
    scores = [ws("TSLA", (i + 1) * 3 * MIN, 0.1 + (i % 20) * 0.01) for i in range(150)]
    hot = simulate(scores, 90, 50, 2, 0)["TSLA"]["crossings"]
    cold = simulate(scores, 99.5, 50, 2, 0)["TSLA"]["crossings"]
    assert hot >= cold


def test_each_ticker_is_judged_against_its_own_history():
    scores = [ws("AAPL", (i + 1) * 3 * MIN, 0.9) for i in range(60)]
    scores += [ws("TSLA", (i + 1) * 3 * MIN, 0.1) for i in range(60)]
    scores.append(ws("AAPL", 500 * 3 * MIN, 0.6))     # low for AAPL
    scores.append(ws("TSLA", 500 * 3 * MIN, 0.6))     # high for TSLA
    report = simulate(scores, p=99, min_samples=50, min_mentions=2, cooldown_s=0)
    assert report["AAPL"]["crossings"] == 0
    assert report["TSLA"]["crossings"] == 1


def test_report_marks_tickers_that_never_warmed_up():
    scores = [ws("TSLA", (i + 1) * 3 * MIN, 0.2) for i in range(10)]
    report = simulate(scores, p=99, min_samples=50, min_mentions=2, cooldown_s=600)
    assert report["TSLA"]["warmed_up"] is False
