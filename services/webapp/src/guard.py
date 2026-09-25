"""Abuse controls for running on the public internet.

Everything here is about one question: what can an anonymous visitor (or a
bot, which starts scanning within minutes of a site going live) make this
server do, and how often?

  * Rate limits: fixed windows counted in Redis, keyed by client IP, user or
    email. Limits fail OPEN if Redis errors, because Redis being down already
    breaks the app; they are not a security boundary against a Redis outage.
  * Cross-site request forgery: unsafe methods must come from this origin
    (Origin/Referer/Sec-Fetch-Site), and /api/* writes must be JSON, which a
    cross-site HTML form cannot send without a CORS preflight we never grant.
  * Response headers: a strict CSP (scripts only from this origin), no
    framing, no MIME sniffing.
  * Request size: bodies over MAX_BODY_BYTES are refused before parsing.

Client IPs come from request.client.host. Behind Caddy that is only correct
because uvicorn runs with proxy_headers and forwarded_allow_ips=127.0.0.1
(see services/standalone); never trust X-Forwarded-For from anywhere else.
"""
from dataclasses import dataclass
from typing import Optional, Tuple
from urllib.parse import urlsplit
import logging
import os

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, PlainTextResponse

log = logging.getLogger("guard")


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class Limit:
    name: str
    count: int
    window_s: int


# Per-client ceilings. Generous for a person clicking around, useless to a bot.
SIGNUP_PER_IP = Limit("signup_ip", _int("RL_SIGNUP_PER_IP_HOUR", 5), 3600)
GUEST_PER_IP = Limit("guest_ip", _int("RL_GUEST_PER_IP_HOUR", 10), 3600)
LOGIN_PER_IP = Limit("login_ip", _int("RL_LOGIN_PER_IP_5MIN", 10), 300)
LOGIN_PER_EMAIL = Limit("login_email", _int("RL_LOGIN_PER_EMAIL_15MIN", 10), 900)
SIMULATE_PER_IP = Limit("sim_ip", _int("RL_SIMULATE_PER_IP_HOUR", 10), 3600)
SLACK_SAVE_PER_USER = Limit("slack_save", _int("RL_SLACK_SAVE_PER_USER_HOUR", 20), 3600)
SLACK_TEST_PER_USER = Limit("slack_test", _int("RL_SLACK_TEST_PER_USER_HOUR", 5), 3600)
API_PER_IP = Limit("api_ip", _int("RL_API_PER_IP_MIN", 300), 60)

# Site-wide ceiling on new accounts (real + guest) per day. Bounds database
# growth and bcrypt CPU no matter how many IPs a bot rotates through.
ACCOUNTS_PER_DAY = Limit("accounts_day", _int("RL_ACCOUNTS_PER_DAY", 300), 86400)

MAX_BODY_BYTES = _int("MAX_BODY_BYTES", 16 * 1024)

CSP = (
    "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; "
    "img-src 'self' data:; connect-src 'self'; font-src 'self'; object-src 'none'; "
    "base-uri 'none'; form-action 'self'; frame-ancestors 'none'"
)


def hit(redis_client, limit: Limit, subject: str) -> Tuple[bool, int]:
    """Count one event. Returns (allowed, retry_after_seconds)."""
    key = f"rl:{limit.name}:{subject}"
    try:
        pipe = redis_client.pipeline()
        pipe.set(key, 0, ex=limit.window_s, nx=True)   # opens the window once
        pipe.incr(key)
        pipe.ttl(key)
        _, count, ttl = pipe.execute()
    except Exception as exc:                           # noqa: BLE001 - fail open, see module doc
        log.warning("rate limiter unavailable: %s", exc)
        return True, 0
    if int(count) > limit.count:
        return False, max(int(ttl), 1)
    return True, 0


def client_ip(request) -> str:
    return request.client.host if request.client else "unknown"


def _same_origin(value: Optional[str], host: str) -> bool:
    try:
        return urlsplit(value).netloc.lower() == host.lower()
    except ValueError:
        return False


class GuardMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, redis_getter):
        super().__init__(app)
        self._redis = redis_getter

    async def dispatch(self, request, call_next):
        method = request.method.upper()
        path = request.url.path

        # 1. body size, before anything parses it
        length = request.headers.get("content-length")
        if length is not None:
            try:
                too_big = int(length) > MAX_BODY_BYTES
            except ValueError:
                too_big = True
            if too_big:
                return PlainTextResponse("Request too large.", status_code=413)

        if method not in ("GET", "HEAD", "OPTIONS"):
            # 2. CSRF: the request must not come from another site
            host = request.headers.get("host", "")
            origin = request.headers.get("origin")
            referer = request.headers.get("referer")
            fetch_site = request.headers.get("sec-fetch-site")
            if fetch_site and fetch_site not in ("same-origin", "none"):
                return PlainTextResponse("Cross-site request refused.", status_code=403)
            # "null" is what a sandboxed cross-site iframe sends; a same-origin
            # request never does, so it is refused like any foreign origin.
            if origin is not None and not _same_origin(origin, host):
                return PlainTextResponse("Cross-site request refused.", status_code=403)
            if origin is None and referer and not _same_origin(referer, host):
                return PlainTextResponse("Cross-site request refused.", status_code=403)

            # 3. /api writes are JSON only. A bodyless POST has no content type;
            #    that is fine, as a cross-site one was already refused above.
            if path.startswith("/api/"):
                ctype = request.headers.get("content-type", "").split(";")[0].strip().lower()
                has_body = request.headers.get("content-length", "0") != "0" or \
                    "transfer-encoding" in request.headers
                if (ctype or has_body) and ctype != "application/json":
                    return JSONResponse({"detail": "Content-Type must be application/json."},
                                        status_code=415)

        # 4. coarse per-IP ceiling on the API
        if path.startswith("/api/"):
            ok, retry = hit(self._redis(), API_PER_IP, client_ip(request))
            if not ok:
                return JSONResponse({"detail": "Too many requests."}, status_code=429,
                                    headers={"Retry-After": str(retry)})

        response = await call_next(request)

        h = response.headers
        h.setdefault("Content-Security-Policy", CSP)
        h.setdefault("X-Content-Type-Options", "nosniff")
        h.setdefault("X-Frame-Options", "DENY")
        h.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        h.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
        h.setdefault("Cross-Origin-Opener-Policy", "same-origin")
        if path.startswith("/api/"):
            h.setdefault("Cache-Control", "no-store")
        return response
