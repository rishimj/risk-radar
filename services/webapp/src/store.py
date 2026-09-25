"""DynamoDB reads/writes for users, watchlists, and alert history."""
from typing import Dict, List, Optional
import logging
import uuid

from boto3.dynamodb.conditions import Key

from riskcore import db
from riskcore.models import iso, utcnow

log = logging.getLogger("store")


# ---------------------------------------------------------------------------
# users
# ---------------------------------------------------------------------------
# Guest demo accounts live under a reserved domain nobody can sign up with.
GUEST_DOMAIN = "guest.riskradar.invalid"
GUEST_WATCHLIST = ["AAPL", "NVDA", "TSLA"]


def create_user(email: str, password_hash: str, guest: bool = False) -> Dict:
    user = {
        "user_id": uuid.uuid4().hex,
        "email": email.strip().lower(),
        "password_hash": password_hash,
        "created_at": iso(utcnow()),
        "slack_webhook_url": "",
        "slack_verified_at": "",
        "is_guest": guest,
    }
    db.table("users").put_item(Item=user)
    return user


def create_guest() -> Dict:
    """A throwaway account with no password, so recruiters can try it in one click.

    The password hash is deliberately not a bcrypt string, so verify_password
    always fails: a guest session can only come from the cookie issued here.
    """
    uid = uuid.uuid4().hex[:12]
    user = create_user(f"guest-{uid}@{GUEST_DOMAIN}", "!guest-no-password", guest=True)
    set_watchlist(user["user_id"], GUEST_WATCHLIST)
    return user


def purge_guests(max_age_hours: int = 48) -> int:
    """Delete guest accounts (and their watchlists) older than max_age_hours."""
    from datetime import timedelta
    cutoff = iso(utcnow() - timedelta(hours=max_age_hours))
    users = db.table("users")
    removed = 0
    items, kwargs = [], {}
    while True:                                        # DynamoDB pages scans at 1 MB
        resp = users.scan(**kwargs)
        items.extend(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    for item in items:
        if not item.get("is_guest") or item.get("created_at", "") >= cutoff:
            continue
        for ticker in watchlist(item["user_id"]):
            db.table("watchlists").delete_item(Key={"user_id": item["user_id"], "ticker": ticker})
        users.delete_item(Key={"user_id": item["user_id"]})
        removed += 1
    if removed:
        log.info("purged %d guest accounts older than %dh", removed, max_age_hours)
    return removed


def user_by_email(email: str) -> Optional[Dict]:
    resp = db.table("users").query(
        IndexName="email-index",
        KeyConditionExpression=Key("email").eq(email.strip().lower()),
        Limit=1,
    )
    items = resp.get("Items", [])
    return items[0] if items else None


def user_by_id(user_id: str) -> Optional[Dict]:
    return db.table("users").get_item(Key={"user_id": user_id}).get("Item")


def set_slack_webhook(user_id: str, url: str, verified: bool = False) -> None:
    db.table("users").update_item(
        Key={"user_id": user_id},
        UpdateExpression="SET slack_webhook_url = :u, slack_verified_at = :v",
        ExpressionAttributeValues={
            ":u": url.strip(),
            ":v": iso(utcnow()) if verified else "",
        },
    )


# ---------------------------------------------------------------------------
# watchlists
# ---------------------------------------------------------------------------
def watchlist(user_id: str) -> List[str]:
    resp = db.table("watchlists").query(
        KeyConditionExpression=Key("user_id").eq(user_id)
    )
    return sorted(item["ticker"] for item in resp.get("Items", []))


def set_watchlist(user_id: str, tickers: List[str]) -> List[str]:
    """Replace the whole list. Small enough that a diff isn't worth it."""
    current = set(watchlist(user_id))
    wanted = {t.upper() for t in tickers}

    table = db.table("watchlists")
    for ticker in wanted - current:
        table.put_item(Item={
            "user_id": user_id, "ticker": ticker, "added_at": iso(utcnow()),
        })
    for ticker in current - wanted:
        table.delete_item(Key={"user_id": user_id, "ticker": ticker})

    # The alert fan-out caches subscriber lookups for ~30s; drop the entries we
    # just invalidated so a fresh signup doesn't wait for the TTL.
    try:
        from riskcore.alerting import invalidate_subscriber_cache
        for ticker in wanted ^ current:
            invalidate_subscriber_cache(ticker)
    except Exception:                                  # noqa: BLE001
        pass

    return sorted(wanted)


# ---------------------------------------------------------------------------
# alerts
# ---------------------------------------------------------------------------
def alerts_for_ticker(ticker: str, limit: int = 20) -> List[Dict]:
    resp = db.table("alerts").query(
        KeyConditionExpression=Key("ticker").eq(ticker),
        ScanIndexForward=False,          # newest first
        Limit=limit,
    )
    return resp.get("Items", [])


def recent_alerts(limit: int = 20) -> List[Dict]:
    """Global feed for the landing page, via the constant-PK index."""
    try:
        resp = db.table("alerts").query(
            IndexName="recent-index",
            KeyConditionExpression=Key("gsi_all").eq("ALERT"),
            ScanIndexForward=False,
            Limit=limit,
        )
        return resp.get("Items", [])
    except Exception as exc:                           # noqa: BLE001
        log.warning("recent alerts query failed: %s", exc)
        return []


def alerts_for_user(user_id: str, limit: int = 25) -> List[Dict]:
    """A user's history = alerts on their tickers, annotated with delivery state."""
    tickers = watchlist(user_id)
    if not tickers:
        return []

    collected: List[Dict] = []
    for ticker in tickers:
        collected.extend(alerts_for_ticker(ticker, limit=limit))

    collected.sort(key=lambda a: a.get("fired_at", ""), reverse=True)
    collected = collected[:limit]

    deliveries = db.table("deliveries")
    for alert in collected:
        try:
            row = deliveries.get_item(
                Key={"alert_id": alert["alert_id"], "user_id": user_id}
            ).get("Item")
            alert["delivery_status"] = row.get("status") if row else "not_delivered"
        except Exception:                              # noqa: BLE001
            alert["delivery_status"] = "unknown"
    return collected
