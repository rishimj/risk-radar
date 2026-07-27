"""Password hashing and signed session cookies.

Deliberately small: bcrypt for passwords, an itsdangerous-signed cookie for
sessions. No JWT, no server-side session store — there is nothing here worth
the extra moving parts.
"""
from typing import Optional
import os

import bcrypt
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

SECRET_KEY = os.getenv("SECRET_KEY", "")
COOKIE_NAME = "riskradar_session"
MAX_AGE_SECONDS = int(os.getenv("SESSION_MAX_AGE", str(14 * 86400)))

if not SECRET_KEY or SECRET_KEY == "change-me-to-a-long-random-string":
    # Loud, but don't refuse to boot — a local demo shouldn't be blocked by this.
    import logging
    logging.getLogger("auth").warning(
        "SECRET_KEY is unset or still the placeholder; sessions are not secure"
    )
    SECRET_KEY = SECRET_KEY or "insecure-dev-key"

_signer = URLSafeTimedSerializer(SECRET_KEY, salt="riskradar-session")


def _prepare(raw: str) -> bytes:
    """bcrypt hard-errors past 72 *bytes*; truncate on the byte string, not the
    str, so a multi-byte password doesn't slip past the limit."""
    return raw.encode("utf-8")[:72]


def hash_password(raw: str) -> str:
    return bcrypt.hashpw(_prepare(raw), bcrypt.gensalt()).decode()


def verify_password(raw: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(_prepare(raw), hashed.encode())
    except (ValueError, TypeError):                    # malformed stored hash
        return False


def issue_session(user_id: str) -> str:
    return _signer.dumps({"uid": user_id})


def read_session(token: Optional[str]) -> Optional[str]:
    if not token:
        return None
    try:
        data = _signer.loads(token, max_age=MAX_AGE_SECONDS)
    except (BadSignature, SignatureExpired):
        return None
    return data.get("uid")


def set_session_cookie(response, user_id: str, secure: bool = False) -> None:
    response.set_cookie(
        COOKIE_NAME, issue_session(user_id),
        max_age=MAX_AGE_SECONDS, httponly=True, samesite="lax", secure=secure,
    )


def clear_session_cookie(response) -> None:
    response.delete_cookie(COOKIE_NAME)
