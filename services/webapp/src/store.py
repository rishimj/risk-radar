"""Users, watchlists and alert history, in PostgreSQL."""
from datetime import timedelta
from typing import Dict, List, Optional
import logging
import uuid

from sqlalchemy import and_, delete, func, insert, select, update

from riskcore import db
from riskcore.models import utcnow

log = logging.getLogger("store")

U, W, A, D = db.users, db.watchlists, db.alerts, db.deliveries

# ---------------------------------------------------------------------------
# users
# ---------------------------------------------------------------------------
# Guest demo accounts live under a reserved domain nobody can sign up with.
GUEST_DOMAIN = "guest.riskradar.invalid"
GUEST_WATCHLIST = ["AAPL", "NVDA", "TSLA"]


class DuplicateEmail(Exception):
    pass


def _user(row) -> Optional[Dict]:
    if row is None:
        return None
    user = db.to_dict(row)
    user["slack_verified_at"] = user["slack_verified_at"] or ""
    return user


def create_user(email: str, password_hash: str, guest: bool = False) -> Dict:
    values = {
        "user_id": uuid.uuid4().hex,
        "email": email.strip().lower(),
        "password_hash": password_hash,
        "created_at": utcnow(),
        "slack_webhook_url": "",
        "slack_verified_at": None,
        "is_guest": guest,
    }
    try:
        with db.begin() as conn:
            conn.execute(insert(U).values(**values))
    except Exception as exc:                           # noqa: BLE001
        if db.is_unique_violation(exc):
            raise DuplicateEmail(values["email"]) from None
        raise
    return user_by_id(values["user_id"])


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
    """Delete guests older than max_age_hours. Watchlists and deliveries go with
    them through ON DELETE CASCADE."""
    cutoff = utcnow() - timedelta(hours=max_age_hours)
    with db.begin() as conn:
        removed = conn.execute(
            delete(U).where(and_(U.c.is_guest.is_(True), U.c.created_at < cutoff))
        ).rowcount
    if removed:
        log.info("purged %d guest accounts older than %dh", removed, max_age_hours)
    return removed or 0


def user_by_email(email: str) -> Optional[Dict]:
    with db.begin() as conn:
        row = conn.execute(select(U).where(U.c.email == email.strip().lower())).first()
    return _user(row)


def user_by_id(user_id: str) -> Optional[Dict]:
    with db.begin() as conn:
        row = conn.execute(select(U).where(U.c.user_id == user_id)).first()
    return _user(row)


def set_slack_webhook(user_id: str, url: str, verified: bool = False) -> None:
    with db.begin() as conn:
        conn.execute(update(U).where(U.c.user_id == user_id).values(
            slack_webhook_url=url.strip(),
            slack_verified_at=utcnow() if verified else None,
        ))


def user_count() -> int:
    with db.begin() as conn:
        return conn.execute(select(func.count()).select_from(U)).scalar_one()


# ---------------------------------------------------------------------------
# watchlists
# ---------------------------------------------------------------------------
def watchlist(user_id: str) -> List[str]:
    with db.begin() as conn:
        rows = conn.execute(select(W.c.ticker).where(W.c.user_id == user_id).order_by(W.c.ticker))
        return [r.ticker for r in rows]


def set_watchlist(user_id: str, tickers: List[str]) -> List[str]:
    """Replace the whole list in one transaction."""
    wanted = sorted({t.upper() for t in tickers})
    with db.begin() as conn:
        current = {r.ticker for r in conn.execute(select(W.c.ticker).where(W.c.user_id == user_id))}
        stale = current - set(wanted)
        if stale:
            conn.execute(delete(W).where(and_(W.c.user_id == user_id, W.c.ticker.in_(stale))))
        fresh = [t for t in wanted if t not in current]
        if fresh:
            now = utcnow()
            conn.execute(insert(W), [{"user_id": user_id, "ticker": t, "added_at": now} for t in fresh])

    # The alert fan-out caches subscriber lookups for ~30s; drop the entries we
    # just changed so a fresh signup doesn't wait for the TTL.
    try:
        from riskcore.alerting import invalidate_subscriber_cache
        for ticker in set(wanted) ^ current:
            invalidate_subscriber_cache(ticker)
    except Exception:                                  # noqa: BLE001
        pass
    return wanted


# ---------------------------------------------------------------------------
# alerts
# ---------------------------------------------------------------------------
def _alert(row) -> Dict:
    return db.to_dict(row)


def alerts_for_ticker(ticker: str, limit: int = 20) -> List[Dict]:
    with db.begin() as conn:
        rows = conn.execute(select(A).where(A.c.ticker == ticker)
                            .order_by(A.c.fired_at.desc()).limit(limit))
        return [_alert(r) for r in rows]


def recent_alerts(limit: int = 20) -> List[Dict]:
    """Global feed for the landing page."""
    try:
        with db.begin() as conn:
            rows = conn.execute(select(A).order_by(A.c.fired_at.desc()).limit(limit))
            return [_alert(r) for r in rows]
    except Exception as exc:                           # noqa: BLE001
        log.warning("recent alerts query failed: %s", exc)
        return []


def alerts_for_user(user_id: str, limit: int = 25) -> List[Dict]:
    """Alerts on the user's tickers, newest first, with this user's delivery status.

    One query: alerts JOIN the user's watchlist, LEFT JOIN their delivery rows.
    """
    status = func.coalesce(D.c.status, "not_delivered").label("delivery_status")
    stmt = (
        select(A, status)
        .join(W, and_(W.c.ticker == A.c.ticker, W.c.user_id == user_id))
        .outerjoin(D, and_(D.c.alert_id == A.c.alert_id, D.c.user_id == user_id))
        .order_by(A.c.fired_at.desc())
        .limit(limit)
    )
    with db.begin() as conn:
        return [_alert(r) for r in conn.execute(stmt)]
