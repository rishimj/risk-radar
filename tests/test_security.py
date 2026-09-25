"""Public-internet protections: CSRF, headers, rate limits, guests, input limits.

Each test names the abuse it prevents. These run against the real app with
fakeredis and a real table backend (SQLite always, DynamoDB Local if up).
"""
import fakeredis
import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient        # noqa: E402

from conftest import load_service_module          # noqa: E402

webapp = load_service_module("webapp", "app")
guard = webapp.guard                               # the instance the app imported


@pytest.fixture
def client(dynamo, monkeypatch):
    fake = fakeredis.FakeRedis()
    monkeypatch.setattr(webapp, "rds", fake)
    from riskcore.baseline import BaselineStore
    monkeypatch.setattr(webapp, "baselines", BaselineStore(fake))
    monkeypatch.setattr(webapp.simulate_mod.kafka, "send", lambda *a, **k: None)
    monkeypatch.setattr(webapp.simulate_mod.kafka, "flush", lambda *a, **k: None)
    with TestClient(webapp.app) as c:
        c.fake_redis = fake
        yield c


def signup(client, email="a@example.com", password="hunter2hunter2", **kw):
    return client.post("/signup", data={"email": email, "password": password},
                       follow_redirects=False, **kw)


# -- CSRF ---------------------------------------------------------------------
def test_cross_site_form_post_is_refused(client):
    resp = signup(client, headers={"Origin": "https://evil.example"})
    assert resp.status_code == 403


def test_cross_site_fetch_metadata_is_refused(client):
    resp = signup(client, headers={"Sec-Fetch-Site": "cross-site"})
    assert resp.status_code == 403


def test_cross_site_referer_is_refused_when_origin_is_absent(client):
    resp = signup(client, headers={"Referer": "https://evil.example/page"})
    assert resp.status_code == 403


def test_same_origin_post_is_allowed(client):
    resp = signup(client, headers={"Origin": "http://testserver",
                                   "Sec-Fetch-Site": "same-origin"})
    assert resp.status_code == 303


def test_api_writes_must_be_json(client):
    """A cross-site HTML form can only send form/text encodings."""
    signup(client)
    resp = client.post("/api/watchlist", content="tickers=TSLA",
                       headers={"Content-Type": "application/x-www-form-urlencoded"})
    assert resp.status_code == 415


# -- response hardening -----------------------------------------------------
def test_security_headers_on_every_page(client):
    resp = client.get("/")
    csp = resp.headers["content-security-policy"]
    assert "script-src 'self'" in csp and "unsafe-inline" not in csp.split("script-src")[1].split(";")[0]
    assert "frame-ancestors 'none'" in csp
    assert resp.headers["x-frame-options"] == "DENY"
    assert resp.headers["x-content-type-options"] == "nosniff"


def test_pages_contain_no_inline_script(client):
    """The CSP forbids inline script, so any left over would silently break a page."""
    signup(client)
    client.post("/api/watchlist", json={"tickers": ["TSLA"]})
    for path in ("/", "/dashboard", "/settings", "/onboarding", "/login", "/signup"):
        html = client.get(path).text
        for chunk in html.split("<script")[1:]:
            assert chunk.split(">", 1)[0].strip().startswith("src="), (path, chunk[:80])


def test_api_docs_are_not_exposed(client):
    for path in ("/docs", "/redoc", "/openapi.json"):
        assert client.get(path).status_code == 404


def test_oversized_bodies_are_refused(client):
    resp = client.post("/login", content="x" * (guard.MAX_BODY_BYTES + 1),
                       headers={"Content-Type": "application/x-www-form-urlencoded"})
    assert resp.status_code == 413


# -- rate limits -----------------------------------------------------------
def test_signup_is_rate_limited_per_ip(client):
    codes = [signup(client, email=f"u{i}@example.com").status_code
             for i in range(guard.SIGNUP_PER_IP.count + 1)]
    assert codes[:-1] == [303] * guard.SIGNUP_PER_IP.count
    assert codes[-1] == 429


def test_login_is_rate_limited(client):
    codes = [client.post("/login", data={"email": "x@example.com", "password": "wrongwrong"})
             .status_code for _ in range(guard.LOGIN_PER_IP.count + 1)]
    assert codes[-1] == 429 and set(codes[:-1]) == {401}


def test_daily_account_cap_holds_across_ips(client, monkeypatch):
    monkeypatch.setattr(guard, "ACCOUNTS_PER_DAY", guard.Limit("accounts_day", 2, 86400))
    monkeypatch.setattr(guard, "SIGNUP_PER_IP", guard.Limit("signup_ip", 100, 3600))
    codes = [signup(client, email=f"u{i}@example.com").status_code for i in range(3)]
    assert codes == [303, 303, 429]


def test_simulate_is_limited_site_wide(client):
    """Two different users cannot overlap simulations."""
    for email in ("a@example.com", "b@example.com"):
        client.cookies.clear()
        signup(client, email=email)
        client.post("/api/watchlist", json={"tickers": ["TSLA"]})
        resp = client.post("/api/simulate", json={"ticker": "TSLA"})
        if email == "a@example.com":
            assert resp.status_code == 200
        else:
            assert resp.status_code == 429 and "Someone just ran" in resp.json()["detail"]


def test_api_has_a_per_ip_ceiling(client, monkeypatch):
    monkeypatch.setattr(guard, "API_PER_IP", guard.Limit("api_ip", 3, 60))
    codes = [client.get("/api/headlines").status_code for _ in range(4)]
    assert codes == [200, 200, 200, 429]


# -- input validation ---------------------------------------------------------
@pytest.mark.parametrize("email", ["notanemail", "a@b", "x" * 300 + "@example.com",
                                   "someone@guest.riskradar.invalid"])
def test_bad_or_reserved_emails_are_rejected(client, email):
    assert signup(client, email=email).status_code in (400, 422)


@pytest.mark.parametrize("url", [
    "https://hooks.slack.com.evil.example/services/T1/B1/x",
    "https://hooks.slack.com/services/T1/B1/x?redirect=https://evil.example",
    "https://hooks.slack.com/services/../../x",
    "http://hooks.slack.com/services/T1/B1/x",
    "https://evil.example/https://hooks.slack.com/services/T1/B1/x",
])
def test_slack_webhook_must_be_exactly_slack(client, url):
    signup(client)
    assert client.post("/api/slack", json={"webhook_url": url}).status_code == 400


def test_watchlist_size_is_bounded(client):
    signup(client)
    resp = client.post("/api/watchlist", json={"tickers": ["TSLA"] * 50})
    assert resp.status_code == 422


def test_simulate_errors_do_not_leak_internals(client, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("kafka at 10.0.0.5:9092 exploded")
    monkeypatch.setattr(webapp.simulate_mod, "run", boom)
    signup(client)
    client.post("/api/watchlist", json={"tickers": ["TSLA"]})
    resp = client.post("/api/simulate", json={"ticker": "TSLA"})
    assert resp.status_code == 500 and "10.0.0.5" not in resp.text


# -- guest demo ---------------------------------------------------------------
def test_guest_demo_lands_on_a_working_dashboard(client):
    resp = client.post("/demo", follow_redirects=False)
    assert resp.status_code == 303 and resp.headers["location"] == "/dashboard"
    assert client.get("/dashboard").status_code == 200
    assert len(client.get("/api/risk").json()["risk"]) >= 1


def test_guest_demo_requires_post(client):
    """Link crawlers follow GETs; they must not mint accounts."""
    assert client.get("/demo").status_code == 405


def test_guests_cannot_use_slack(client):
    """Otherwise anonymous visitors could make the server post to Slack."""
    client.post("/demo")
    ok_url = "https://hooks.slack.com/services/T1/B1/abc"
    assert client.post("/api/slack", json={"webhook_url": ok_url}).status_code == 403
    assert client.post("/api/slack/test").status_code == 403


def test_guest_accounts_cannot_be_logged_into_with_a_password(client, dynamo):
    import store as store_mod
    guest = store_mod.create_guest()
    for pw in ("", "!guest-no-password", "password"):
        resp = client.post("/login", data={"email": guest["email"], "password": pw or "x"})
        assert resp.status_code == 401


def test_old_guests_are_purged(dynamo):
    import store as store_mod
    old = store_mod.create_guest()
    real = store_mod.create_user("keep@example.com", "hash")
    dynamo.table("users").update_item(
        Key={"user_id": old["user_id"]}, UpdateExpression="SET created_at = :c",
        ExpressionAttributeValues={":c": "2020-01-01T00:00:00Z"})
    fresh = store_mod.create_guest()
    assert store_mod.purge_guests(48) == 1
    assert store_mod.user_by_id(old["user_id"]) is None
    assert store_mod.watchlist(old["user_id"]) == []
    assert store_mod.user_by_id(fresh["user_id"]) is not None
    assert store_mod.user_by_id(real["user_id"]) is not None


# -- XSS through feed data ----------------------------------------------------
def test_landing_never_renders_a_javascript_href(client):
    import json
    client.fake_redis.lpush("recent:headlines", json.dumps({
        "title": "<img src=x onerror=alert(1)>", "url": "javascript:alert(document.cookie)",
        "source": "x", "sentiment": -0.5, "companies": []}))
    html = client.get("/").text
    assert "javascript:alert" not in html
    assert "<img src=x" not in html


def test_landing_shows_real_news_only(client):
    import json
    from riskcore.models import SIMULATED_SOURCE
    client.fake_redis.lpush("recent:headlines", json.dumps({
        "title": "Real headline", "url": "https://x/1", "source": "Reuters", "companies": []}))
    client.fake_redis.lpush("recent:headlines", json.dumps({
        "title": "Synthetic crisis", "url": "https://x/2", "source": SIMULATED_SOURCE,
        "companies": []}))
    html = client.get("/").text
    assert "Real headline" in html and "Synthetic crisis" not in html


def test_null_origin_is_refused(client):
    """A sandboxed cross-site iframe sends Origin: null and no Referer."""
    assert signup(client, headers={"Origin": "null"}).status_code == 403


def test_alert_history_limit_is_clamped(client, monkeypatch):
    seen = {}
    real = webapp.store.alerts_for_user

    def spy(uid, limit=25):
        seen["limit"] = limit
        return real(uid, limit=limit)
    monkeypatch.setattr(webapp.store, "alerts_for_user", spy)
    signup(client)
    client.post("/api/watchlist", json={"tickers": ["TSLA"]})
    client.get("/api/alerts?limit=100000")
    assert seen["limit"] == 50
    client.get("/api/alerts?limit=-3")
    assert seen["limit"] == 1
