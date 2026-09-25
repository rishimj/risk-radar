"""Alert construction, cooldown, and multi-user Slack fan-out.

Ported from the old repo's alerting.py, with two deliberate changes:

1. Synchronous `requests`, not `aiohttp`/asyncio.  This runs inside a PyFlink
   map operator; spinning an event loop per record there is a good way to
   deadlock the Python harness.  Retry semantics are identical: 3 attempts,
   `2 ** attempt` backoff, success requires HTTP 200 AND a body of "ok".

2. Fan-out instead of one global webhook.  The old code returned early when the
   webhook was the literal string "disabled" (news_processing_job.py:311), which
   meant no alert was recorded at all.  Here the alert row is always written
   first, then a delivery row per subscriber — so a user with no Slack still
   sees their alert history in the app.
"""
from typing import Dict, Iterable, List, Optional, Tuple
import logging
import time

import requests
from boto3.dynamodb.conditions import Key

from . import config, db
from .models import Alert, RiskFeatures, iso, utcnow
from .risk import severity_for

log = logging.getLogger(__name__)

_EMOJI = {"high": "🔴", "medium": "🟡", "low": "🟢"}
_COLOR = {"high": "#d7263d", "medium": "#f6a609", "low": "#2a9d8f"}


# --------------------------------------------------------------------------
# cooldown
# --------------------------------------------------------------------------
# Real and simulated alerts keep SEPARATE cooldowns: a visitor's simulation must
# neither suppress a real alert for 10 minutes nor be able to clear a real one.
def _cooldown_key(ticker: str, simulated: bool = False) -> str:
    return f"alert:last_sent:{'sim:' if simulated else ''}{ticker}"


def in_cooldown(redis_client, ticker: str, simulated: bool = False) -> bool:
    return bool(redis_client.exists(_cooldown_key(ticker, simulated)))


def mark_sent(redis_client, ticker: str, minutes: Optional[int] = None,
              simulated: bool = False) -> None:
    ttl = (minutes if minutes is not None else config.ALERT_COOLDOWN_MINUTES) * 60
    redis_client.setex(_cooldown_key(ticker, simulated), ttl, str(time.time()))


def clear_cooldown(redis_client, ticker: str, simulated: bool = False) -> None:
    """Used by the simulate endpoint (simulated=True) so a repeat demo fires."""
    redis_client.delete(_cooldown_key(ticker, simulated))


# --------------------------------------------------------------------------
# alert construction
# --------------------------------------------------------------------------
def build_alert(features: RiskFeatures, threshold: float, source: str = "live") -> Alert:
    sev = severity_for(features.risk_score, config.SEVERITY_HIGH, config.SEVERITY_MEDIUM)
    label = {"high": "CRITICAL", "medium": "WARNING", "low": "NOTICE"}[sev]
    message = (
        f"{label}: negative news risk for {features.ticker} "
        f"(risk {features.risk_score:.3f}; score {features.alert_score:.3f} "
        f"vs baseline {threshold:.3f})"
    )
    return Alert(
        alert_id=Alert.new_id(),
        ticker=features.ticker,
        risk_score=features.risk_score,
        baseline_p=round(threshold, 3),
        severity=sev,
        message=message,
        window_end=features.window_end,
        fired_at=iso(utcnow()),
        source=source,
        top_headline=features.top_headline,
        top_url=features.top_url,
    )


def slack_payload(alert: Alert) -> Dict:
    fields = [
        {"title": "Risk score", "value": f"{alert.risk_score:.3f}", "short": True},
        {"title": "Baseline (p{:.0f})".format(config.ALERT_PERCENTILE),
         "value": f"{alert.baseline_p:.3f}", "short": True},
    ]
    if alert.top_headline:
        fields.append({"title": "Top headline", "value": alert.top_headline, "short": False})

    attachment = {
        "color": _COLOR.get(alert.severity, "#888888"),
        "fields": fields,
        "footer": "RiskRadar" + (" · simulated" if alert.source == "simulated" else ""),
        "ts": int(time.time()),
    }
    if alert.top_url:
        attachment["title_link"] = alert.top_url

    return {
        "text": f"{_EMOJI.get(alert.severity, '⚪')} {alert.message}",
        "attachments": [attachment],
    }


# --------------------------------------------------------------------------
# slack delivery
# --------------------------------------------------------------------------
def send_to_slack(webhook_url: str, alert: Alert,
                  max_retries: Optional[int] = None) -> bool:
    retries = max_retries if max_retries is not None else config.SLACK_MAX_RETRIES
    payload = slack_payload(alert)

    for attempt in range(retries):
        try:
            resp = requests.post(
                webhook_url, json=payload, timeout=config.SLACK_TIMEOUT_SECONDS
            )
            if resp.status_code == 200 and resp.text.strip() == "ok":
                return True
            log.warning(
                "slack rejected alert %s (attempt %d/%d): %s %s",
                alert.ticker, attempt + 1, retries, resp.status_code, resp.text[:120],
            )
        except Exception as exc:                      # noqa: BLE001 - never kill the job
            log.error("slack error for %s (attempt %d/%d): %s",
                      alert.ticker, attempt + 1, retries, exc)

        if attempt < retries - 1:
            time.sleep(2 ** attempt)

    log.error("giving up on slack for %s after %d attempts", alert.ticker, retries)
    return False


# --------------------------------------------------------------------------
# subscriber lookup (cached — this runs per window per ticker)
# --------------------------------------------------------------------------
_sub_cache: Dict[str, Tuple[float, List[Dict]]] = {}
_SUB_TTL = 30.0


def subscribers_for(ticker: str, use_cache: bool = True) -> List[Dict]:
    """Users watching `ticker`, via the watchlists ticker-index GSI."""
    now = time.time()
    if use_cache:
        hit = _sub_cache.get(ticker)
        if hit and now - hit[0] < _SUB_TTL:
            return hit[1]

    try:
        resp = db.table("watchlists").query(
            IndexName="ticker-index",
            KeyConditionExpression=Key("ticker").eq(ticker),
        )
        rows = resp.get("Items", [])
    except Exception as exc:                          # noqa: BLE001
        log.error("subscriber lookup failed for %s: %s", ticker, exc)
        return []

    users: List[Dict] = []
    for row in rows:
        uid = row.get("user_id")
        if not uid:
            continue
        try:
            u = db.table("users").get_item(Key={"user_id": uid}).get("Item")
        except Exception:                             # noqa: BLE001
            u = None
        if u:
            users.append(u)

    _sub_cache[ticker] = (now, users)
    return users


def invalidate_subscriber_cache(ticker: Optional[str] = None) -> None:
    if ticker is None:
        _sub_cache.clear()
    else:
        _sub_cache.pop(ticker, None)


# --------------------------------------------------------------------------
# persistence + fan-out
# --------------------------------------------------------------------------
def persist_alert(alert: Alert) -> None:
    item = alert.to_dict()
    item["fired_key"] = f"{alert.fired_at}#{alert.alert_id}"
    item["gsi_all"] = "ALERT"
    # DynamoDB rejects float; store risk as a string-safe Decimal-ish value.
    item["risk_score"] = str(alert.risk_score)
    item["baseline_p"] = str(alert.baseline_p)
    db.table("alerts").put_item(Item=item)


def record_delivery(alert: Alert, user: Dict, status: str) -> None:
    db.table("deliveries").put_item(Item={
        "alert_id": alert.alert_id,
        "user_id": user["user_id"],
        "ticker": alert.ticker,
        "status": status,   # sent | failed | no_webhook | unverified | simulated
        "fired_at": alert.fired_at,
        "delivered_at": iso(utcnow()),
    })


def unverify_webhook(user_id: str) -> None:
    """Stop delivering to a hook that failed until its owner re-tests it."""
    try:
        db.table("users").update_item(
            Key={"user_id": user_id},
            UpdateExpression="SET slack_verified_at = :v",
            ExpressionAttributeValues={":v": ""},
        )
        invalidate_subscriber_cache()
    except Exception as exc:                          # noqa: BLE001
        log.warning("could not unverify webhook for %s: %s", user_id, exc)


def fan_out(alert: Alert) -> Dict[str, int]:
    """Write the alert, then deliver to each subscriber.

    The alert row is written unconditionally — even with zero subscribers, or
    subscribers with no Slack webhook — so history stays complete.

    Slack delivery is deliberately narrow, because it runs on the pipeline
    thread and is the one thing that reaches outside this server:
      * simulated alerts are never sent. Anyone can press Simulate (including
        anonymous guests), and it must not become a way to post into other
        people's Slack workspaces.
      * only hooks whose owner completed a successful test are used, so a
        bogus hook cannot stall the pipeline with retries.
      * a verified hook that fails is un-verified, so it is not retried on
        every future alert.
    """
    persist_alert(alert)

    stats = {"subscribers": 0, "sent": 0, "failed": 0, "no_webhook": 0,
             "unverified": 0, "simulated": 0}
    for user in subscribers_for(alert.ticker):
        stats["subscribers"] += 1
        hook = (user.get("slack_webhook_url") or "").strip()
        if not hook:
            status = "no_webhook"
        elif alert.source == "simulated":
            status = "simulated"
        elif not user.get("slack_verified_at"):
            status = "unverified"
        else:
            ok = send_to_slack(hook, alert)
            status = "sent" if ok else "failed"
            if not ok:
                unverify_webhook(user["user_id"])
        record_delivery(alert, user, status)
        stats[status] += 1

    log.info("alert %s %s -> %s", alert.ticker, alert.alert_id[:8], stats)
    return stats
