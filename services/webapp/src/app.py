"""RiskRadar web app: auth, watchlists, dashboard, and the simulate hook."""
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Dict, List, Optional
import json
import logging
import os
import time

import redis
import requests
from fastapi import Depends, FastAPI, Form, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from riskcore import alerting, config, db, headlines
from riskcore.baseline import BaselineStore
from riskcore.entities import COMPANIES
from riskcore.models import Alert, iso, utcnow

import auth
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    for attempt in range(10):
        try:
            db.ensure_tables()
            break
        except Exception as exc:                       # noqa: BLE001
            log.warning("waiting for dynamodb (%d/10): %s", attempt + 1, exc)
            time.sleep(3)
    yield


app = FastAPI(title="RiskRadar", version="0.1.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(BASE / "static")), name="static")


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
        "headlines": headlines.recent(rds, limit=12),
        "alerts": store.recent_alerts(limit=5),
    })


@app.get("/signup", response_class=HTMLResponse)
def signup_page(request: Request):
    return templates.TemplateResponse(request, "signup.html", {
        "error": None})


@app.post("/signup")
def signup(request: Request, email: str = Form(...), password: str = Form(...)):
    email = email.strip().lower()
    if len(password) < 8:
        return templates.TemplateResponse(request, "signup.html", {
        "error": "Password must be at least 8 characters."
        }, status_code=400)
    if store.user_by_email(email):
        return templates.TemplateResponse(request, "signup.html", {
        "error": "That email is already registered."
        }, status_code=400)

    user = store.create_user(email, auth.hash_password(password))
    response = RedirectResponse("/onboarding", status_code=303)
    auth.set_session_cookie(response, user["user_id"], secure=COOKIE_SECURE)
    return response


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse(request, "login.html", {
        "error": None})


@app.post("/login")
def login(request: Request, email: str = Form(...), password: str = Form(...)):
    user = store.user_by_email(email)
    if not user or not auth.verify_password(password, user.get("password_hash", "")):
        return templates.TemplateResponse(request, "login.html", {
        "error": "Incorrect email or password."
        }, status_code=401)

    response = RedirectResponse("/dashboard", status_code=303)
    auth.set_session_cookie(response, user["user_id"], secure=COOKIE_SECURE)
    return response


@app.get("/logout")
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
def api_headlines(request: Request, limit: int = 25, ticker: Optional[str] = None):
    return {"headlines": headlines.recent(rds, limit=min(limit, 100), ticker=ticker)}


@app.get("/api/risk")
def api_risk(request: Request):
    """Latest window per watched ticker, plus that ticker's own alert cut.

    The baseline travels with the risk value because the dashboard draws it as a
    per-ticker reference line — a fixed threshold line would misrepresent how
    alerting actually works.
    """
    user = require_user(request)
    out = []
    for ticker in store.watchlist(user["user_id"]):
        raw = rds.get(f"feat:{ticker}:latest")
        feats = json.loads(raw) if raw else None
        threshold = baselines.threshold(ticker)
        samples = baselines.sample_count(ticker)
        out.append({
            "ticker": ticker,
            "name": COMPANIES[ticker].name if ticker in COMPANIES else ticker,
            "risk_score": feats.get("risk_score") if feats else None,
            # What the baseline cut is in the units of (uncapped); risk_score
            # is the 0-1 gauge. Older payloads lack it and fall back.
            "alert_score": (feats.get("alert_score", feats.get("risk_score"))
                            if feats else None),
            "sentiment_score": feats.get("sentiment_score") if feats else None,
            "total_mentions": feats.get("total_mentions") if feats else 0,
            "window_end": feats.get("window_end") if feats else None,
            "top_headline": feats.get("top_headline") if feats else "",
            "top_url": feats.get("top_url") if feats else "",
            "baseline": threshold,
            "baseline_samples": samples,
            "baseline_ready": threshold is not None,
            "min_samples": baselines.min_samples,
        })
    return {"risk": out, "percentile": baselines.percentile_p}


@app.get("/api/alerts")
def api_alerts(request: Request, limit: int = 25):
    user = require_user(request)
    return {"alerts": store.alerts_for_user(user["user_id"], limit=limit)}


@app.get("/api/status")
def api_status():
    """Health of each dependency, for the dashboard pills."""
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

    return {
        "kafka": probe(kafka_detail),
        "flink": probe(stream_detail),
        "enrichment": probe(enrichment_detail),
        "redis": probe(redis_detail),
    }


class WatchlistIn(BaseModel):
    tickers: List[str]


@app.post("/api/watchlist")
def api_watchlist(request: Request, body: WatchlistIn):
    user = require_user(request)
    unknown = [t for t in body.tickers if t.upper() not in COMPANIES]
    if unknown:
        raise HTTPException(400, f"unknown tickers: {', '.join(unknown)}")
    return {"watching": store.set_watchlist(user["user_id"], body.tickers)}


class SlackIn(BaseModel):
    webhook_url: str


@app.post("/api/slack")
def api_slack(request: Request, body: SlackIn):
    """Save a webhook. Verification is a separate, explicit action."""
    user = require_user(request)
    url = body.webhook_url.strip()
    if url and not url.startswith("https://hooks.slack.com/"):
        raise HTTPException(400, "That doesn't look like a Slack incoming-webhook URL.")
    store.set_slack_webhook(user["user_id"], url, verified=False)
    alerting.invalidate_subscriber_cache()
    return {"saved": True, "configured": bool(url)}


@app.post("/api/slack/test")
def api_slack_test(request: Request):
    user = require_user(request)
    url = (user.get("slack_webhook_url") or "").strip()
    if not url:
        raise HTTPException(400, "No Slack webhook saved yet.")

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
    ticker: str


@app.post("/api/simulate")
def api_simulate(request: Request, body: SimulateIn):
    user = require_user(request)
    ticker = body.ticker.upper()

    if ticker not in store.watchlist(user["user_id"]):
        raise HTTPException(400, f"{ticker} is not on your watchlist.")

    rate_key = f"simulate:rate:{user['user_id']}"
    if not rds.set(rate_key, "1", nx=True, ex=60):
        raise HTTPException(429, "One simulation per minute. Try again shortly.")

    try:
        result = simulate_mod.run(rds, ticker)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    except Exception as exc:                           # noqa: BLE001
        log.exception("simulate failed")
        raise HTTPException(500, f"Simulation failed: {exc}")

    return result


@app.get("/healthz")
def healthz():
    return {"status": "ok"}
