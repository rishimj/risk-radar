"""RiskRadar web app: auth, watchlists, dashboard, and the simulate hook."""
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Dict, List, Optional
import json
import logging
import os
import re
import threading
import time

import redis
import requests
from fastapi import Depends, FastAPI, Form, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from riskcore import alerting, config, db, headlines
from riskcore.baseline import BaselineStore
from riskcore.entities import COMPANIES
from riskcore.models import Alert, iso, utcnow

import auth
import guard
import insights
import simulate as simulate_mod
import store

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("webapp")

BASE = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE / "templates"))

rds = redis.from_url(config.REDIS_URL)
baselines = BaselineStore(rds)
COOKIE_SECURE = os.getenv("COOKIE_SECURE", "").lower() in {"1", "true", "yes"}
FLINK_URL = os.getenv("FLINK_URL", "http://flink-jobmanager:8081")
PROCESSOR_URL = os.getenv("PROCESSOR_URL", "http://processor:8083")
ENABLE_API_DOCS = os.getenv("ENABLE_API_DOCS", "").lower() in {"1", "true", "yes"}
GUEST_DEMO = os.getenv("GUEST_DEMO", "true").lower() in {"1", "true", "yes"}
GUEST_MAX_AGE_HOURS = int(os.getenv("GUEST_MAX_AGE_HOURS", "48"))
# One simulation site-wide per window. Each run stamps watermark filler ~4 min
# ahead, so overlapping runs would land in windows that cannot close yet.
SIMULATE_GLOBAL_SECONDS = int(os.getenv("SIMULATE_GLOBAL_SECONDS", "240"))
STATUS_CACHE_SECONDS = float(os.getenv("STATUS_CACHE_SECONDS", "5"))

# Filled in by services/standalone, whose Kafka/stream/enrichment stages are
# in-process rather than services to probe over the network.
status_providers: Dict[str, object] = {}
status_labels: Dict[str, str] = {}
templates.env.globals["stack_label"] = os.getenv(
    "STACK_LABEL", "Kafka · PyFlink · FinBERT · PostgreSQL · Redis")
templates.env.globals["guest_demo"] = GUEST_DEMO
templates.env.globals["percentile"] = config.ALERT_PERCENTILE

_EMAIL = re.compile(r"^[^@\s]{1,64}@[^@\s]+\.[^@\s]{2,}$")
# A real bcrypt hash of nothing, verified against when the email is unknown so a
# failed login costs the same time whether or not the account exists.
_DUMMY_HASH = auth.hash_password("timing-equaliser")


@asynccontextmanager
async def lifespan(app: FastAPI):
    for attempt in range(10):
        try:
            db.ensure_schema()
            break
        except Exception as exc:                       # noqa: BLE001
            log.warning("waiting for the database (%d/10): %s", attempt + 1, exc)
            time.sleep(3)
    try:
        store.purge_guests(GUEST_MAX_AGE_HOURS)
    except Exception as exc:                           # noqa: BLE001
        log.warning("guest purge failed: %s", exc)
    yield


app = FastAPI(
    title="RiskRadar", version="0.1.0", lifespan=lifespan,
    # The interactive docs are a free map of every endpoint for scanners.
    docs_url="/docs" if ENABLE_API_DOCS else None,
    redoc_url=None,
    openapi_url="/openapi.json" if ENABLE_API_DOCS else None,
)
app.add_middleware(guard.GuardMiddleware, redis_getter=lambda: rds)
app.add_middleware(guard.BodySizeLimit)            # outermost: counts streamed bytes
app.mount("/static", StaticFiles(directory=str(BASE / "static")), name="static")


def _limited(limit: "guard.Limit", subject: str) -> Optional[int]:
    """Count one event; returns retry-after seconds when over the limit."""
    ok, retry = guard.hit(rds, limit, subject)
    return None if ok else retry


def _wait_msg(seconds: int) -> str:
    return f"Too many attempts. Try again in {max(1, round(seconds / 60))} min."


# ---------------------------------------------------------------------------
# auth plumbing
# ---------------------------------------------------------------------------
def current_user(request: Request) -> Optional[Dict]:
    uid = auth.read_session(request.cookies.get(auth.COOKIE_NAME))
    return store.user_by_id(uid) if uid else None


def require_user(request: Request) -> Dict:
    user = current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="not signed in")
    return user


# ---------------------------------------------------------------------------
# pages
# ---------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
def landing(request: Request):
    user = current_user(request)
    if user:
        return RedirectResponse("/dashboard", status_code=303)
    return templates.TemplateResponse(request, "landing.html", {
        "headlines": _landing_headlines(),
        "alerts": store.recent_alerts(limit=5),
        "percentile": baselines.percentile_p,
    })


def _landing_headlines(limit: int = 10):
    """Real news that names a watched company in its headline.

    Not simulated articles (visitors' demos stay on their dashboards), and not
    stories that merely mention a company in the summary or come from its
    domain; those are what put "older dogs can develop..." under META.
    """
    out = []
    for h in headlines.recent(rds, limit=80, include_simulated=False):
        if any(c.get("role") == "primary" for c in h.get("companies") or []):
            out.append(h)
            if len(out) >= limit:
                break
    return out


@app.get("/signup", response_class=HTMLResponse)
def signup_page(request: Request):
    return templates.TemplateResponse(request, "signup.html", {
        "error": None})


@app.post("/signup")
def signup(request: Request, email: str = Form(..., max_length=254),
           password: str = Form(..., max_length=128)):
    def fail(msg: str, status: int = 400):
        return templates.TemplateResponse(request, "signup.html", {"error": msg},
                                          status_code=status)

    email = email.strip().lower()
    if not _EMAIL.match(email) or email.endswith("@" + store.GUEST_DOMAIN):
        return fail("Enter a valid email address.")
    if len(password) < 8:
        return fail("Password must be at least 8 characters.")

    retry = _limited(guard.SIGNUP_PER_IP, guard.client_ip(request))
    if retry:
        return fail(_wait_msg(retry), 429)
    retry = _limited(guard.ACCOUNTS_PER_DAY, "all")
    if retry:
        return fail("Sign-ups are paused for today. Try the guest demo instead.", 429)

    if store.user_by_email(email):
        return templates.TemplateResponse(request, "signup.html", {
        "error": "That email is already registered."
        }, status_code=400)

    try:
        user = store.create_user(email, auth.hash_password(password))
    except store.DuplicateEmail:
        return fail("That email is already registered.")
    response = RedirectResponse("/onboarding", status_code=303)
    auth.set_session_cookie(response, user["user_id"], secure=COOKIE_SECURE)
    return response


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse(request, "login.html", {
        "error": None})


@app.post("/login")
def login(request: Request, email: str = Form(..., max_length=254),
          password: str = Form(..., max_length=128)):
    email = email.strip().lower()
    ip = guard.client_ip(request)
    # Per IP, plus per (email, IP): keyed on the email alone, anyone could lock
    # a known user out just by failing logins for them from elsewhere.
    retry = (_limited(guard.LOGIN_PER_IP, ip)
             or _limited(guard.LOGIN_PER_EMAIL, f"{email}|{ip}"))
    if retry:
        return templates.TemplateResponse(request, "login.html", {
            "error": _wait_msg(retry)}, status_code=429)

    user = store.user_by_email(email) if _EMAIL.match(email) else None
    if user is None:
        auth.verify_password(password, _DUMMY_HASH)    # equalise timing
    if not user or not auth.verify_password(password, user.get("password_hash", "")):
        return templates.TemplateResponse(request, "login.html", {
        "error": "Incorrect email or password."
        }, status_code=401)

    response = RedirectResponse("/dashboard", status_code=303)
    auth.set_session_cookie(response, user["user_id"], secure=COOKIE_SECURE)
    return response


@app.post("/demo")
def guest_demo(request: Request):
    """One-click guest account, pre-watching a few tickers. No email, no password."""
    if not GUEST_DEMO:
        raise HTTPException(404)
    ip = guard.client_ip(request)
    # Repeat clicks from one address reuse that address's guest instead of
    # minting a new account each time.
    reuse_key = f"guest:by_ip:{ip}"
    user = None
    try:
        existing = rds.get(reuse_key)
        if existing:
            user = store.user_by_id(existing.decode() if isinstance(existing, bytes) else existing)
    except Exception as exc:                           # noqa: BLE001
        log.warning("guest reuse lookup failed: %s", exc)
    if user is None:
        retry = (_limited(guard.GUEST_PER_IP, ip) or _limited(guard.GUESTS_PER_DAY, "all"))
        if retry:
            return templates.TemplateResponse(request, "login.html", {
                "error": "The guest demo is busy right now. Try again later."}, status_code=429)
        user = store.create_guest()
        try:
            rds.set(reuse_key, user["user_id"], ex=GUEST_MAX_AGE_HOURS * 3600)
        except Exception:                              # noqa: BLE001
            pass
    response = RedirectResponse("/dashboard", status_code=303)
    auth.set_session_cookie(response, user["user_id"], secure=COOKIE_SECURE)
    return response


@app.post("/logout")
def logout():
    response = RedirectResponse("/", status_code=303)
    auth.clear_session_cookie(response)
    return response


@app.get("/onboarding", response_class=HTMLResponse)
def onboarding(request: Request):
    user = current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    return templates.TemplateResponse(request, "onboarding.html", {
        "user": user,
        "companies": COMPANIES, "watching": store.watchlist(user["user_id"]),
    })


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    user = current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    watching = store.watchlist(user["user_id"])
    if not watching:
        return RedirectResponse("/onboarding", status_code=303)
    return templates.TemplateResponse(request, "dashboard.html", {
        "user": user, "watching": watching,
        "companies": COMPANIES,
    })


@app.get("/settings", response_class=HTMLResponse)
def settings(request: Request):
    user = current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    return templates.TemplateResponse(request, "settings.html", {
        "user": user,
        "companies": COMPANIES, "watching": store.watchlist(user["user_id"]),
    })


# ---------------------------------------------------------------------------
# api
# ---------------------------------------------------------------------------
@app.get("/api/headlines")
def api_headlines(request: Request, limit: int = 25, ticker: Optional[str] = None,
                  relevant: bool = False):
    """relevant=true: only stories that name a tracked company in the headline."""
    limit = max(1, min(limit, 100))
    ticker = (ticker or "")[:10] or None
    if not relevant:
        return {"headlines": headlines.recent(rds, limit=limit, ticker=ticker)}
    out = [h for h in headlines.recent(rds, limit=limit * 4, ticker=ticker)
           if any(c.get("role") == "primary" for c in h.get("companies") or [])]
    return {"headlines": out[:limit]}


@app.get("/api/risk")
def api_risk(request: Request):
    """Latest window per watched ticker, plus that ticker's own alert cut.

    The baseline travels with the risk value because the dashboard draws it as a
    per-ticker reference line — a fixed threshold line would misrepresent how
    alerting actually works.
    """
    user = require_user(request)
    out = [insights.ticker_row(rds, baselines, t) for t in store.watchlist(user["user_id"])]
    return {"risk": out, "percentile": baselines.percentile_p}


_overview_cache = {"at": 0.0, "value": None}
_overview_lock = threading.Lock()


@app.get("/api/public/overview")
def api_public_overview():
    """Live, public-safe aggregates for the landing page. Cached: it is the one
    unauthenticated JSON endpoint, so every visitor shares one computation."""
    with _overview_lock:
        now = time.monotonic()
        if _overview_cache["value"] is None or now - _overview_cache["at"] > STATUS_CACHE_SECONDS:
            _overview_cache["value"] = insights.overview(rds, baselines)
            _overview_cache["at"] = now
        return _overview_cache["value"]


@app.get("/api/alerts")
def api_alerts(request: Request, limit: int = 25):
    user = require_user(request)
    # Bounded: each returned alert costs a delivery lookup, so an unbounded
    # limit would let one request fan out into thousands of reads.
    return {"alerts": store.alerts_for_user(user["user_id"], limit=max(1, min(limit, 50)))}


_status_cache = {"at": 0.0, "value": None}
_status_lock = threading.Lock()


@app.get("/api/status")
def api_status():
    """Health of each dependency, for the dashboard pills.

    Cached for STATUS_CACHE_SECONDS: every open dashboard polls this, and each
    uncached call opens a Kafka admin connection and three HTTP probes.
    """
    with _status_lock:
        now = time.monotonic()
        if _status_cache["value"] is None or now - _status_cache["at"] > STATUS_CACHE_SECONDS:
            _status_cache["value"] = _compute_status()
            _status_cache["at"] = now
        return _status_cache["value"]


def _compute_status():
    def probe(fn):
        try:
            return {"ok": True, "detail": fn()}
        except Exception as exc:                       # noqa: BLE001
            return {"ok": False, "detail": str(exc)[:120]}

    def kafka_detail():
        from kafka import KafkaAdminClient
        admin = KafkaAdminClient(bootstrap_servers=config.KAFKA_BOOTSTRAP.split(","),
                                 request_timeout_ms=4000)
        topics = admin.list_topics()
        admin.close()
        return f"{len(topics)} topics"

    def flink_detail():
        resp = requests.get(f"{FLINK_URL}/jobs/overview", timeout=4)
        resp.raise_for_status()
        jobs = resp.json().get("jobs", [])
        running = [j for j in jobs if j.get("state") == "RUNNING"]
        if not running:
            raise RuntimeError(f"no RUNNING job ({len(jobs)} known)")
        return running[0].get("name", "running")

    def lite_detail():
        resp = requests.get(f"{PROCESSOR_URL}/stats", timeout=4)
        resp.raise_for_status()
        stats = resp.json()
        if stats.get("state") != "RUNNING":
            raise RuntimeError(f"lite processor {stats.get('state', '?')}")
        return stats.get("name", "lite")

    def stream_detail():
        """Whichever engine is up: the Flink cluster or the lite processor."""
        errors = []
        for engine, fn in (("lite", lite_detail), ("flink", flink_detail)):
            try:
                return fn()
            except Exception as exc:                   # noqa: BLE001
                errors.append(f"{engine}: {str(exc)[:50]}")
        raise RuntimeError("; ".join(errors))

    def enrichment_detail():
        resp = requests.get(f"{config.ENRICHMENT_URL}/stats", timeout=4)
        resp.raise_for_status()
        return resp.json().get("tier", "?")

    def redis_detail():
        rds.ping()
        return "connected"

    checks = {
        "kafka": kafka_detail,
        "flink": stream_detail,
        "enrichment": enrichment_detail,
        "redis": redis_detail,
    }
    checks.update(status_providers)
    out = {name: probe(fn) for name, fn in checks.items()}
    for name, label in status_labels.items():
        out.setdefault(name, {})["label"] = label
    return out


class WatchlistIn(BaseModel):
    tickers: List[str] = Field(max_length=len(COMPANIES))


@app.post("/api/watchlist")
def api_watchlist(request: Request, body: WatchlistIn):
    user = require_user(request)
    unknown = [t for t in body.tickers if t.upper() not in COMPANIES]
    if unknown:
        raise HTTPException(400, f"unknown tickers: {', '.join(unknown)}")
    return {"watching": store.set_watchlist(user["user_id"], body.tickers)}


class SlackIn(BaseModel):
    webhook_url: str = Field(max_length=300)


# services/<team>/<channel>/<token>, alphanumeric segments only.
_SLACK_HOOK = re.compile(r"^https://hooks\.slack\.com/services/[A-Za-z0-9]+/[A-Za-z0-9]+/[A-Za-z0-9]+$")


def _no_guests(user: Dict) -> None:
    if user.get("is_guest"):
        raise HTTPException(403, "Guest demo accounts can't connect Slack. "
                                 "Create a free account to try it.")


@app.post("/api/slack")
def api_slack(request: Request, body: SlackIn):
    """Save a webhook. Verification is a separate, explicit action."""
    user = require_user(request)
    _no_guests(user)
    if _limited(guard.SLACK_SAVE_PER_USER, user["user_id"]):
        raise HTTPException(429, "Too many changes. Try again later.")
    url = body.webhook_url.strip()
    # Exact host and path shape: this URL is the one thing a user can make the
    # server send requests to, so it may only ever point at Slack.
    if url and not _SLACK_HOOK.match(url):
        raise HTTPException(400, "That doesn't look like a Slack incoming-webhook URL.")
    store.set_slack_webhook(user["user_id"], url, verified=False)
    alerting.invalidate_subscriber_cache()
    return {"saved": True, "configured": bool(url)}


@app.post("/api/slack/test")
def api_slack_test(request: Request):
    user = require_user(request)
    _no_guests(user)
    url = (user.get("slack_webhook_url") or "").strip()
    if not url:
        raise HTTPException(400, "No Slack webhook saved yet.")
    if not _SLACK_HOOK.match(url):
        raise HTTPException(400, "Saved webhook is not a Slack incoming-webhook URL.")
    if _limited(guard.SLACK_TEST_PER_USER, user["user_id"]):
        raise HTTPException(429, "Too many test messages. Try again later.")

    probe = Alert(
        alert_id="test", ticker="TSLA", risk_score=0.91, baseline_p=0.42,
        severity="high",
        message="Test alert from RiskRadar — your Slack connection works.",
        window_end=iso(utcnow()), fired_at=iso(utcnow()), source="simulated",
        top_headline="This is what a real alert looks like.",
    )
    ok = alerting.send_to_slack(url, probe)
    if ok:
        store.set_slack_webhook(user["user_id"], url, verified=True)
        alerting.invalidate_subscriber_cache()
    return {"sent": ok}


class SimulateIn(BaseModel):
    ticker: str = Field(max_length=10)


@app.post("/api/simulate")
def api_simulate(request: Request, body: SimulateIn):
    user = require_user(request)
    ticker = body.ticker.upper()

    if ticker not in store.watchlist(user["user_id"]):
        raise HTTPException(400, f"{ticker} is not on your watchlist.")

    rate_key = f"simulate:rate:{user['user_id']}"
    if not rds.set(rate_key, "1", nx=True, ex=60):
        raise HTTPException(429, "One simulation per minute. Try again shortly.")
    if _limited(guard.SIMULATE_PER_IP, guard.client_ip(request)):
        raise HTTPException(429, "Simulation limit reached. Try again later.")
    if SIMULATE_GLOBAL_SECONDS > 0 and not rds.set(
            "simulate:global", "1", nx=True, ex=SIMULATE_GLOBAL_SECONDS):
        wait = max(1, rds.ttl("simulate:global") or SIMULATE_GLOBAL_SECONDS)
        raise HTTPException(429, f"Someone just ran a simulation. The next one is "
                                 f"available in {wait}s, so their windows can close.")

    try:
        result = simulate_mod.run(rds, ticker)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    except Exception:                                  # noqa: BLE001
        log.exception("simulate failed")
        raise HTTPException(500, "Simulation failed. Please try again later.")

    return result


@app.get("/healthz")
def healthz():
    return {"status": "ok"}
