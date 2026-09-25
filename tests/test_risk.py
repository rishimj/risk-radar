"""Risk scoring: the ported math, plus regressions for the defects it had."""
from datetime import datetime, timezone

import pytest

from riskcore.models import CompanyMention
from riskcore.risk import Aggregation, build_features, score_sentiments, severity_for


def test_empty_window_is_zero_risk():
    assert Aggregation().risk() == 0.0
    assert score_sentiments([]) == 0.0


def test_ewm_seeds_on_first_value_then_decays():
    agg = Aggregation()
    agg.add(-1.0)
    assert agg.sentiment == -1.0            # first value seeds, not blended with 0
    agg.add(1.0)
    # 0.1 * 1.0 + 0.9 * -1.0
    assert agg.sentiment == pytest.approx(-0.8, abs=1e-6)


def test_neutral_sentiment_counts_in_neither_bucket():
    agg = Aggregation()
    agg.add(0.0)
    assert (agg.neg_count, agg.pos_count, agg.total_mentions) == (0, 0, 0)
    assert agg.count == 1


def test_volume_multiplier_caps_at_1_5():
    strong = Aggregation()
    for _ in range(50):
        strong.add(-0.5)
    # cap means 50 mentions scores no higher than 6 would on the volume term
    six = Aggregation()
    for _ in range(6):
        six.add(-0.5)
    assert strong.risk() == pytest.approx(six.risk(), abs=1e-3)


def test_risk_is_capped_at_one():
    agg = Aggregation()
    for _ in range(20):
        agg.add(-1.0)
    assert agg.risk() == 1.0


def test_positive_news_scores_low_risk():
    assert score_sentiments([0.9, 0.8, 0.85]) < 0.2


@pytest.mark.parametrize("score,expected", [
    (0.95, "high"), (0.80, "high"),          # boundary is inclusive
    (0.79, "medium"), (0.60, "medium"),      # boundary is inclusive
    (0.59, "low"), (0.0, "low"),
])
def test_severity_bands(score, expected):
    assert severity_for(score) == expected


# ---------------------------------------------------------------------------
# Regression: the specific miscalibration that made the old formula unusable.
# ---------------------------------------------------------------------------
def test_neutral_news_still_scores_half_risk():
    """Documents WHY a fixed threshold cannot be used on this gauge.

    (1 - 0)/2 == 0.5, so perfectly neutral news sits halfway to a 0.7 cut before
    anything bad has happened. This is not a bug to fix in the score -- the score
    is a display gauge -- it is the reason alerting is relative (see baseline.py).
    """
    agg = Aggregation()
    agg.add(0.0)
    assert agg.risk() == pytest.approx(0.5, abs=1e-3)


def test_single_mildly_negative_headline_would_clear_the_old_threshold():
    """The measured failure mode: one mildly negative headline clears 0.70.

        sentiment_risk = (1 - (-0.30)) / 2 = 0.650
        volume_multiplier (n=1)            = 1.000
        negative_bias (100% negative)      = 1.250
                                             -----
                                             0.8125

    Pinned so that if anyone reintroduces an absolute threshold on this value,
    this test explains what breaks.
    """
    assert score_sentiments([-0.30]) == pytest.approx(0.8125, abs=1e-3)
    assert score_sentiments([-0.30]) > 0.70


def test_build_features_surfaces_most_negative_headline():
    ts = datetime(2026, 7, 27, 12, 0, tzinfo=timezone.utc)
    mentions = [
        CompanyMention("TSLA", "a", "Tesla beats delivery estimates", "u1", "s", 0.6, "primary", 1),
        CompanyMention("TSLA", "b", "Tesla recalls 400k vehicles", "u2", "s", -0.9, "primary", 2),
        CompanyMention("TSLA", "c", "Tesla opens new plant", "u3", "s", 0.2, "primary", 3),
    ]
    feats = build_features("TSLA", mentions, ts, ts)
    assert feats.top_headline == "Tesla recalls 400k vehicles"
    assert feats.top_url == "u2"
    assert feats.total_mentions == 3
    assert feats.neg_count == 1 and feats.pos_count == 2


# -- saturation: the reason no alert could ever fire ------------------------
def test_alert_score_is_the_uncapped_formula():
    agg = Aggregation()
    for _ in range(20):
        agg.add(-1.0)
    # 1.0 sentiment risk x 1.5 volume x 1.25 all-negative bias
    assert agg.alert_score() == pytest.approx(1.875)
    assert agg.risk() == 1.0


def test_alert_score_equals_risk_below_the_cap():
    agg = Aggregation()
    agg.add(0.4)
    agg.add(0.2)
    assert agg.alert_score() == agg.risk() < 1.0


def test_alert_score_ranks_windows_the_cap_would_tie():
    """A 3-headline dip and a 10-headline crisis both read 1.0 on the gauge.

    Only the uncapped score can tell them apart, which is what lets a real
    crisis clear a p99 that mild dips have already pushed to the cap.
    """
    dip = Aggregation()
    for _ in range(3):
        dip.add(-0.9)
    crisis = Aggregation()
    for _ in range(10):
        crisis.add(-0.93)
    assert dip.risk() == crisis.risk() == 1.0
    assert crisis.alert_score() > dip.alert_score() > 1.0


def test_build_features_carries_both_scores():
    ms = [CompanyMention(ticker="TSLA", article_id=str(i), title="t", url="u",
                         source="s", sentiment=-0.95, role="primary", event_ts=0)
          for i in range(8)]
    t = datetime(2026, 7, 27, tzinfo=timezone.utc)
    feats = build_features("TSLA", ms, t, t)
    assert feats.risk_score == 1.0
    assert feats.alert_score > 1.0


def test_features_written_before_alert_score_existed_fall_back():
    from riskcore.models import RiskFeatures
    old = RiskFeatures.from_dict({
        "ticker": "TSLA", "window_start": "a", "window_end": "b", "risk_score": 0.7,
        "sentiment_score": -0.2, "neg_count": 2, "pos_count": 0, "total_mentions": 2,
    })
    assert old.alert_score == 0.7
