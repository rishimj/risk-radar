"""Shared fixtures.

DynamoDB-backed tests run against a real **DynamoDB Local** if one is reachable
(the same image compose runs), and skip otherwise — so `make test` still works
on a laptop with nothing running, and `make test` inside the stack exercises the
genuine boto3 path rather than a mock.

Start one locally with:
    docker compose up -d dynamodb-local
or point DYNAMO_ENDPOINT_URL at any running instance.
"""
import os
import socket
import sys
from pathlib import Path
from urllib.parse import urlparse

import pytest

ROOT = Path(__file__).resolve().parents[1]

DDB_ENDPOINT = os.environ.setdefault("DYNAMO_ENDPOINT_URL", "http://localhost:8000")

# Overwrite rather than setdefault: a developer (or a CI runner) may already have
# real AWS credentials exported, and DynamoDB Local partitions its data by access
# key — so inheriting an ambient key makes tests read a different namespace than
# they wrote.
# Alphanumeric only: DynamoDB Local derives its storage namespace from the access
# key and rejects one containing a hyphen with UnrecognizedClientException.
os.environ["AWS_ACCESS_KEY_ID"] = "riskradartest"
os.environ["AWS_SECRET_ACCESS_KEY"] = "riskradartest"
os.environ.pop("AWS_SESSION_TOKEN", None)
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["AWS_REGION"] = "us-east-1"
os.environ.setdefault("SECRET_KEY", "test-secret-key-for-sessions")

# boto3 must not send loopback traffic through an outbound proxy.
_host = urlparse(DDB_ENDPOINT).hostname or "localhost"
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
    parsed = urlparse(url)
    try:
        with socket.create_connection(
            (parsed.hostname or "localhost", parsed.port or 8000), timeout=timeout
        ):
            return True
    except OSError:
        return False


DDB_AVAILABLE = _reachable(DDB_ENDPOINT)


@pytest.fixture
def dynamo():
    """All four tables, emptied between tests so cases stay independent."""
    if not DDB_AVAILABLE:
        pytest.skip(f"no DynamoDB Local at {DDB_ENDPOINT} (run: docker compose up -d dynamodb-local)")

    from riskcore import db

    db._resource = None
    db.ensure_tables()

    _truncate(db)
    yield db
    _truncate(db)


def _truncate(db) -> None:
    """Delete every item, keeping the tables (recreating them is far slower)."""
    for name, spec in db.TABLES.items():
        table = db.table(name)
        keys = [k["AttributeName"] for k in spec["KeySchema"]]
        scanned = table.scan(ProjectionExpression=", ".join(f"#{k}" for k in keys),
                             ExpressionAttributeNames={f"#{k}": k for k in keys})
        with table.batch_writer() as batch:
            for item in scanned.get("Items", []):
                batch.delete_item(Key={k: item[k] for k in keys})
