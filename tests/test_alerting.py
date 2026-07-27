"""Alert construction, cooldown, and per-user fan-out scoping."""
from datetime import datetime, timezone
from unittest.mock import patch

import fakeredis
import pytest

from riskcore import alerting
from riskcore.models import Alert, RiskFeatures
from riskcore.alerting import (
    build_alert, clear_cooldown, in_cooldown, mark_sent, slack_payload,
)


@pytest.fixture
def redis_client():
    return fakeredis.FakeRedis()


@pytest.fixture
def features():
    return RiskFeatures(
        ticker="TSLA",
        window_start="2026-07-27T12:00:00Z",
        window_end="2026-07-27T12:15:00Z",
        risk_score=0.92,
        sentiment_score=-0.7,
        neg_count=4,
        pos_count=0,
        total_mentions=4,
        top_headline="Tesla recalls 400k vehicles",
        top_url="https://example.com/a",
    )


# -- cooldown ---------------------------------------------------------------
def test_cooldown_blocks_then_clears(redis_client):
    assert in_cooldown(redis_client, "TSLA") is False
    mark_sent(redis_client, "TSLA")
    assert in_cooldown(redis_client, "TSLA") is True
    clear_cooldown(redis_client, "TSLA")
    assert in_cooldown(redis_client, "TSLA") is False


def test_cooldown_is_per_ticker(redis_client):
    mark_sent(redis_client, "TSLA")
    assert in_cooldown(redis_client, "NVDA") is False


def test_cooldown_expires(redis_client):
    mark_sent(redis_client, "TSLA", minutes=1)
    assert redis_client.ttl("alert:last_sent:TSLA") <= 60


# -- construction -----------------------------------------------------------
def test_build_alert_classifies_severity(features):
    assert build_alert(features, 0.4).severity == "high"
    features.risk_score = 0.65
    assert build_alert(features, 0.4).severity == "medium"
    features.risk_score = 0.2
    assert build_alert(features, 0.4).severity == "low"


def test_alert_records_the_baseline_it_cleared(features):
    alert = build_alert(features, 0.413)
    assert alert.baseline_p == 0.413
    assert "baseline" in alert.message


def test_simulated_alerts_are_tagged(features):
    assert build_alert(features, 0.4, source="simulated").source == "simulated"
    assert build_alert(features, 0.4).source == "live"


def test_slack_payload_carries_headline_and_link(features):
    payload = slack_payload(build_alert(features, 0.4))
    assert "TSLA" in payload["text"]
    body = str(payload["attachments"][0])
    assert "Tesla recalls 400k vehicles" in body
    assert payload["attachments"][0]["title_link"] == "https://example.com/a"


def test_simulated_alerts_are_visibly_marked_in_slack(features):
    payload = slack_payload(build_alert(features, 0.4, source="simulated"))
    assert "simulated" in payload["attachments"][0]["footer"]


# -- slack retry ------------------------------------------------------------
class _Resp:
    def __init__(self, status, text):
        self.status_code, self.text = status, text


def test_slack_success_requires_status_200_and_body_ok():
    with patch("riskcore.alerting.requests.post", return_value=_Resp(200, "ok")) as post:
        assert alerting.send_to_slack("http://hook", _alert()) is True
        assert post.call_count == 1


def test_slack_200_with_wrong_body_is_a_failure():
    with patch("riskcore.alerting.requests.post", return_value=_Resp(200, "invalid_payload")), \
         patch("riskcore.alerting.time.sleep"):
        assert alerting.send_to_slack("http://hook", _alert()) is False


def test_slack_retries_then_succeeds():
    responses = [_Resp(500, "err"), _Resp(200, "ok")]
    with patch("riskcore.alerting.requests.post", side_effect=responses), \
         patch("riskcore.alerting.time.sleep") as sleep:
        assert alerting.send_to_slack("http://hook", _alert()) is True
        sleep.assert_called_once_with(1)          # 2 ** 0


def test_slack_exhausts_retries_with_exponential_backoff():
    with patch("riskcore.alerting.requests.post", side_effect=RuntimeError("boom")), \
         patch("riskcore.alerting.time.sleep") as sleep:
        assert alerting.send_to_slack("http://hook", _alert(), max_retries=3) is False
        assert [c.args[0] for c in sleep.call_args_list] == [1, 2]


# -- fan-out scoping --------------------------------------------------------
def test_fan_out_only_delivers_to_subscribers_of_that_ticker(features):
    alert = build_alert(features, 0.4)
    calls = []

    subs = [
        {"user_id": "u1", "slack_webhook_url": "http://hook/u1"},
        {"user_id": "u2", "slack_webhook_url": ""},          # no slack configured
    ]
    with patch("riskcore.alerting.persist_alert") as persist, \
         patch("riskcore.alerting.subscribers_for", return_value=subs), \
         patch("riskcore.alerting.record_delivery", side_effect=lambda a, u, s: calls.append((u["user_id"], s))), \
         patch("riskcore.alerting.send_to_slack", return_value=True):
        stats = alerting.fan_out(alert)

    persist.assert_called_once()
    assert stats == {"subscribers": 2, "sent": 1, "failed": 0, "no_webhook": 1}
    assert ("u1", "sent") in calls
    assert ("u2", "no_webhook") in calls


def test_alert_is_persisted_even_with_zero_subscribers(features):
    """The old code returned early when no webhook was set, losing the alert."""
    alert = build_alert(features, 0.4)
    with patch("riskcore.alerting.persist_alert") as persist, \
         patch("riskcore.alerting.subscribers_for", return_value=[]):
        stats = alerting.fan_out(alert)
    persist.assert_called_once()
    assert stats["subscribers"] == 0


def test_failed_slack_still_records_a_delivery_row(features):
    alert = build_alert(features, 0.4)
    calls = []
    with patch("riskcore.alerting.persist_alert"), \
         patch("riskcore.alerting.subscribers_for",
               return_value=[{"user_id": "u1", "slack_webhook_url": "http://hook"}]), \
         patch("riskcore.alerting.record_delivery", side_effect=lambda a, u, s: calls.append(s)), \
         patch("riskcore.alerting.send_to_slack", return_value=False):
        stats = alerting.fan_out(alert)
    assert calls == ["failed"]
    assert stats["failed"] == 1


def _alert() -> Alert:
    return Alert(
        alert_id="x", ticker="TSLA", risk_score=0.9, baseline_p=0.4, severity="high",
        message="m", window_end="2026-07-27T12:15:00Z", fired_at="2026-07-27T12:15:30Z",
    )
