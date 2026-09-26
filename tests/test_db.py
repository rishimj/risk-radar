"""The PostgreSQL data layer: constraints, cascades and the queries built on them.

Runs on SQLite and on a real PostgreSQL (see conftest.database).
"""
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select

from riskcore import alerting
from riskcore.models import Alert, iso

from conftest import load_service_module

store = load_service_module("webapp", "store")

T0 = datetime(2026, 9, 26, 12, 0, tzinfo=timezone.utc)


def alert(ticker="TSLA", minutes=0, source="live", aid=None):
    return Alert(
        alert_id=aid or f"a{ticker}{minutes}{source}", ticker=ticker, risk_score=1.0,
        baseline_p=1.1, severity="high", message="m", window_end=iso(T0),
        fired_at=iso(T0 + timedelta(minutes=minutes)), source=source, alert_score=1.4,
    )


def count(database, table):
    with database.begin() as conn:
        return conn.execute(select(func.count()).select_from(table)).scalar_one()


def test_schema_has_the_expected_tables(database):
    assert {t.name for t in database.metadata.sorted_tables} == {
        "users", "watchlists", "alerts", "deliveries"}


def test_deleting_a_user_cascades_to_watchlists_and_deliveries(database):
    u = store.create_user("c@example.com", "h")
    store.set_watchlist(u["user_id"], ["TSLA", "AAPL"])
    a = alert()
    alerting.persist_alert(a)
    alerting.record_delivery(a, u, "no_webhook")
    with database.begin() as conn:
        conn.execute(database.users.delete().where(database.users.c.user_id == u["user_id"]))
    assert count(database, database.watchlists) == 0
    assert count(database, database.deliveries) == 0
    assert count(database, database.alerts) == 1              # history is kept


def test_a_delivery_must_reference_a_real_alert_and_user(database):
    u = store.create_user("fk@example.com", "h")
    with pytest.raises(Exception):
        alerting.record_delivery(alert(aid="missing"), u, "sent")


def test_alert_history_is_one_join_with_this_users_delivery_status(database):
    me = store.create_user("me@example.com", "h")
    other = store.create_user("other@example.com", "h")
    store.set_watchlist(me["user_id"], ["TSLA"])
    store.set_watchlist(other["user_id"], ["TSLA", "NVDA"])
    old, new, nvda = alert(minutes=1), alert(minutes=5), alert("NVDA", minutes=9)
    for a in (old, new, nvda):
        alerting.persist_alert(a)
    alerting.record_delivery(new, me, "sent")
    alerting.record_delivery(new, other, "failed")

    rows = store.alerts_for_user(me["user_id"])
    assert [r["alert_id"] for r in rows] == [new.alert_id, old.alert_id]   # newest first, TSLA only
    assert [r["delivery_status"] for r in rows] == ["sent", "not_delivered"]
    assert rows[0]["alert_score"] == pytest.approx(1.4)
    assert rows[0]["fired_at"].endswith("Z")


def test_subscribers_are_found_with_a_join(database):
    a = store.create_user("a@example.com", "h")
    b = store.create_user("b@example.com", "h")
    store.set_watchlist(a["user_id"], ["TSLA"])
    store.set_watchlist(b["user_id"], ["AAPL"])
    subs = alerting.subscribers_for("TSLA", use_cache=False)
    assert [s["email"] for s in subs] == ["a@example.com"]


def test_set_watchlist_replaces_in_one_transaction(database):
    u = store.create_user("w@example.com", "h")
    store.set_watchlist(u["user_id"], ["TSLA", "AAPL"])
    assert store.set_watchlist(u["user_id"], ["aapl", "NVDA"]) == ["AAPL", "NVDA"]
    assert store.watchlist(u["user_id"]) == ["AAPL", "NVDA"]


def test_slack_verification_round_trips_as_an_iso_string(database):
    u = store.create_user("s@example.com", "h")
    assert store.user_by_id(u["user_id"])["slack_verified_at"] == ""
    store.set_slack_webhook(u["user_id"], "https://hooks.slack.com/services/T/B/x", verified=True)
    assert store.user_by_id(u["user_id"])["slack_verified_at"].endswith("Z")
    alerting.unverify_webhook(u["user_id"])
    assert store.user_by_id(u["user_id"])["slack_verified_at"] == ""
