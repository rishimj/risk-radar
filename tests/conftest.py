"""Shared fixtures.

Database-backed tests run twice: against SQLite (always, a fresh file per test)
and against a real PostgreSQL when one is reachable. The Postgres runs use a
dedicated database (TEST_DATABASE_URL, default riskradar_test), never the app's,
and every table is emptied between tests.

Start one locally with:
    docker compose up -d postgres
    docker compose exec postgres createdb -U riskradar riskradar_test
"""
import os
import socket
import sys
from pathlib import Path
from urllib.parse import urlparse

import pytest

ROOT = Path(__file__).resolve().parents[1]

os.environ.setdefault("SECRET_KEY", "test-secret-key-for-sessions")
TEST_PG_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+psycopg://riskradar:riskradar@localhost:5432/riskradar_test")

# The database driver must not send loopback traffic through an outbound proxy.
_host = urlparse(TEST_PG_URL.replace("+psycopg", "")).hostname or "localhost"
_no_proxy = os.environ.get("NO_PROXY", "")
if _host not in _no_proxy:
    os.environ["NO_PROXY"] = f"{_no_proxy},{_host}".strip(",")
    os.environ["no_proxy"] = os.environ["NO_PROXY"]


def load_service_module(service: str, module: str):
    """Import services/<service>/src/<module>.py under a collision-proof name.

    Both the enrichment and webapp services have an `app.py`; a plain
    `import app` gives whichever ran first for the whole session.
    """
    import importlib.util

    src = ROOT / "services" / service / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))

    qualified = f"_svc_{service}_{module}"
    if qualified in sys.modules:
        return sys.modules[qualified]

    spec = importlib.util.spec_from_file_location(qualified, src / f"{module}.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[qualified] = mod
    spec.loader.exec_module(mod)
    return mod


def _reachable(url: str, timeout: float = 1.0) -> bool:
    parsed = urlparse(url.replace("+psycopg", ""))
    try:
        with socket.create_connection((parsed.hostname or "localhost", parsed.port or 5432),
                                      timeout=timeout):
            return True
    except OSError:
        return False


PG_AVAILABLE = _reachable(TEST_PG_URL)


@pytest.fixture(params=["sqlite", "postgres"])
def database(request, tmp_path, monkeypatch):
    """The full schema on each backend, empty at the start and end of every test."""
    from riskcore import config, db

    if request.param == "sqlite":
        url = f"sqlite:///{tmp_path / 'test.db'}"
    else:
        if not PG_AVAILABLE:
            pytest.skip(f"no PostgreSQL at {TEST_PG_URL} (see tests/conftest.py)")
        url = TEST_PG_URL
    monkeypatch.setattr(config, "DATABASE_URL", url)
    db.ensure_schema()
    _truncate(db)
    yield db
    _truncate(db)


def _truncate(db) -> None:
    """Delete every row, children first (the FKs would refuse otherwise)."""
    with db.begin() as conn:
        for table in reversed(db.metadata.sorted_tables):
            conn.execute(table.delete())
