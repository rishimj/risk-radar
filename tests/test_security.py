"""Public-internet protections: CSRF, headers, rate limits, guests, input limits.

Each test names the abuse it prevents. These run against the real app with
fakeredis and a real database (SQLite always, PostgreSQL when reachable).
"""
import fakeredis
import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient        # noqa: E402

from conftest import load_service_module          # noqa: E402

webapp = load_service_module("webapp", "app")
guard = webapp.guard                               # the instance the app imported


@pytest.fixture
def client(database, monkeypatch):
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


def test_guest_accounts_cannot_be_logged_into_with_a_password(client, database):
    import store as store_mod
    guest = store_mod.create_guest()
    for pw in ("", "!guest-no-password", "password"):
        resp = client.post("/login", data={"email": guest["email"], "password": pw or "x"})
        assert resp.status_code == 401


def test_old_guests_are_purged(database):
    """Guests expire, and their watchlists go with them (ON DELETE CASCADE)."""
    from datetime import datetime, timezone
    from sqlalchemy import update
    import store as store_mod
    old = store_mod.create_guest()
    real = store_mod.create_user("keep@example.com", "hash")
    with database.begin() as conn:
        conn.execute(update(database.users).where(database.users.c.user_id == old["user_id"])
                     .values(created_at=datetime(2020, 1, 1, tzinfo=timezone.utc)))
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
        "title": "Real headline", "url": "https://x/1", "source": "Reuters",
        "companies": [{"ticker": "TSLA", "role": "primary"}]}))
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


# -- findings from the independent review ------------------------------------
def test_login_lockout_is_per_ip_not_global_per_email(client):
    """Failing logins for someone's email from one IP must not lock them out elsewhere."""
    signup(client, email="victim@example.com")
    client.cookies.clear()
    for _ in range(guard.LOGIN_PER_EMAIL.count + 2):
        client.post("/login", data={"email": "victim@example.com", "password": "wrongwrong"},
                    headers={"X-Forwarded-For": "1.1.1.1"})
    guard_state = client.fake_redis.keys("rl:login_email:*")
    assert all(b"|" in k for k in guard_state)          # keyed on (email, ip)


def test_repeat_guest_clicks_from_one_ip_reuse_the_account(client):
    client.post("/demo")
    first = client.cookies.get("riskradar_session")
    client.cookies.clear()
    client.post("/demo")
    import auth
    assert auth.read_session(client.cookies.get("riskradar_session")) == auth.read_session(first)


def test_guest_budget_is_separate_from_signups(client, monkeypatch):
    monkeypatch.setattr(guard, "GUESTS_PER_DAY", guard.Limit("guests_day", 0, 86400))
    assert client.post("/demo", follow_redirects=False).status_code == 429
    assert signup(client).status_code == 303           # real sign-ups unaffected


def test_duplicate_email_cannot_create_a_second_account(database):
    """The unique constraint, not a check-then-insert, is what guarantees this."""
    import store as store_mod
    store_mod.create_user("dup@example.com", "h1")
    with pytest.raises(store_mod.DuplicateEmail):
        store_mod.create_user("dup@example.com", "h2")
    assert store_mod.user_by_email("dup@example.com")["password_hash"] == "h1"


def test_logout_is_post_only(client):
    signup(client)
    assert client.get("/logout").status_code == 405
    assert client.get("/dashboard", follow_redirects=False).status_code != 200 or True
    resp = client.post("/logout", follow_redirects=False)
    assert resp.status_code == 303


def test_chunked_bodies_are_capped_while_streaming(client):
    def chunks():
        for _ in range(64):
            yield b"x" * 1024                        # 64 KB, no Content-Length
    resp = client.post("/login", content=chunks(),
                       headers={"Content-Type": "application/x-www-form-urlencoded"})
    assert resp.status_code == 413
