"""Relational storage: PostgreSQL in every deployment, SQLite for fast local tests.

One SQLAlchemy Core schema serves both. Configure with DATABASE_URL:

    postgresql+psycopg://user:pass@host:5432/riskradar       (compose, EC2)
    postgresql+psycopg://riskradar@/riskradar?host=/srv/risk-radar/run
                                                             (single node, unix socket)
    sqlite:///path/to/file.db                                (tests / scratch)

The data is relational, so the schema says so: foreign keys with cascades
(deleting a guest removes its watchlist and delivery rows in one statement), a
unique email, and indexes that match every query the app makes. Listing a
user's alerts with their delivery status is one join.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Optional
import logging
import threading

from sqlalchemy import (
    Boolean, Column, DateTime, Float, ForeignKey, Index, MetaData, String, Table, Text,
    create_engine, event,
)
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError

from . import config

log = logging.getLogger(__name__)

metadata = MetaData()

users = Table(
    "users", metadata,
    Column("user_id", String(32), primary_key=True),
    Column("email", String(254), nullable=False, unique=True),
    Column("password_hash", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("slack_webhook_url", Text, nullable=False, server_default=""),
    Column("slack_verified_at", DateTime(timezone=True), nullable=True),
    Column("is_guest", Boolean, nullable=False, server_default="false"),
)
Index("users_guest_created_idx", users.c.is_guest, users.c.created_at)

watchlists = Table(
    "watchlists", metadata,
    Column("user_id", String(32), ForeignKey("users.user_id", ondelete="CASCADE"), primary_key=True),
    Column("ticker", String(10), primary_key=True),
    Column("added_at", DateTime(timezone=True), nullable=False),
)
# The alert fan-out: given a ticker, who is watching it?
Index("watchlists_ticker_idx", watchlists.c.ticker)

alerts = Table(
    "alerts", metadata,
    Column("alert_id", String(32), primary_key=True),
    Column("ticker", String(10), nullable=False),
    Column("fired_at", DateTime(timezone=True), nullable=False),
    Column("window_end", DateTime(timezone=True), nullable=True),
    Column("risk_score", Float, nullable=False),
    Column("alert_score", Float, nullable=False, server_default="0"),
    Column("baseline_p", Float, nullable=False),
    Column("severity", String(16), nullable=False),
    Column("message", Text, nullable=False),
    Column("source", String(16), nullable=False, server_default="live"),
    Column("top_headline", Text, nullable=False, server_default=""),
    Column("top_url", Text, nullable=False, server_default=""),
)
Index("alerts_ticker_fired_idx", alerts.c.ticker, alerts.c.fired_at.desc())
Index("alerts_fired_idx", alerts.c.fired_at.desc())

deliveries = Table(
    "deliveries", metadata,
    Column("alert_id", String(32), ForeignKey("alerts.alert_id", ondelete="CASCADE"), primary_key=True),
    Column("user_id", String(32), ForeignKey("users.user_id", ondelete="CASCADE"), primary_key=True),
    Column("status", String(16), nullable=False),     # sent | failed | no_webhook | unverified | simulated
    Column("delivered_at", DateTime(timezone=True), nullable=False),
)


# ---------------------------------------------------------------------------
# engine
# ---------------------------------------------------------------------------
_engine: Optional[Engine] = None
_engine_url: Optional[str] = None
_lock = threading.Lock()


def _make_engine(url: str) -> Engine:
    if url.startswith("sqlite"):
        eng = create_engine(url, connect_args={"check_same_thread": False, "timeout": 15})

        @event.listens_for(eng, "connect")
        def _sqlite_pragmas(dbapi_conn, _):               # noqa: ANN001
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA foreign_keys=ON")         # SQLite ignores FKs unless asked
            cur.execute("PRAGMA journal_mode=WAL")
            cur.close()
        return eng
    # Small pool: one process, a handful of threads. pre_ping survives a
    # database restart without a failed request.
    return create_engine(url, pool_size=5, max_overflow=5, pool_pre_ping=True, pool_recycle=1800)


def engine() -> Engine:
    """The process-wide engine for config.DATABASE_URL (rebuilt if it changes)."""
    global _engine, _engine_url
    with _lock:
        if _engine is None or _engine_url != config.DATABASE_URL:
            if _engine is not None:
                _engine.dispose()
            _engine = _make_engine(config.DATABASE_URL)
            _engine_url = config.DATABASE_URL
        return _engine


@contextmanager
def begin() -> Iterator[Connection]:
    """A transaction: committed on success, rolled back on any exception."""
    with engine().begin() as conn:
        yield conn


def ensure_schema() -> None:
    """Create any missing tables and indexes. Idempotent; safe from every process."""
    metadata.create_all(engine())


def is_unique_violation(exc: Exception) -> bool:
    return isinstance(exc, IntegrityError)


# ---------------------------------------------------------------------------
# row helpers
# ---------------------------------------------------------------------------
def utc(dt: Optional[datetime]) -> Optional[datetime]:
    """SQLite hands back naive datetimes; everything stored here is UTC."""
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def to_dict(row) -> Dict[str, Any]:
    """A result row as a plain dict, with datetimes as the ISO strings the UI uses."""
    from .models import iso
    out = {}
    for key, value in row._mapping.items():
        out[key] = iso(utc(value)) if isinstance(value, datetime) else value
    return out
