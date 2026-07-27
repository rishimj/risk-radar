"""DynamoDB access.

The ONLY difference between local and AWS is DYNAMO_ENDPOINT_URL: set it and
boto3 talks to dynamodb-local, unset it and boto3 uses the EC2 instance role.
"""
from typing import Any, Dict, List, Optional
import logging

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from . import config

log = logging.getLogger(__name__)

TABLES = {
    "users": {
        "KeySchema": [{"AttributeName": "user_id", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "user_id", "AttributeType": "S"},
            {"AttributeName": "email", "AttributeType": "S"},
        ],
        "GlobalSecondaryIndexes": [{
            "IndexName": "email-index",
            "KeySchema": [{"AttributeName": "email", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"},
        }],
    },
    "watchlists": {
        "KeySchema": [
            {"AttributeName": "user_id", "KeyType": "HASH"},
            {"AttributeName": "ticker", "KeyType": "RANGE"},
        ],
        "AttributeDefinitions": [
            {"AttributeName": "user_id", "AttributeType": "S"},
            {"AttributeName": "ticker", "AttributeType": "S"},
        ],
        # The fan-out query: given a ticker, who is watching it?
        "GlobalSecondaryIndexes": [{
            "IndexName": "ticker-index",
            "KeySchema": [
                {"AttributeName": "ticker", "KeyType": "HASH"},
                {"AttributeName": "user_id", "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": "ALL"},
        }],
    },
    "alerts": {
        "KeySchema": [
            {"AttributeName": "ticker", "KeyType": "HASH"},
            {"AttributeName": "fired_key", "KeyType": "RANGE"},   # "<fired_at>#<alert_id>"
        ],
        "AttributeDefinitions": [
            {"AttributeName": "ticker", "AttributeType": "S"},
            {"AttributeName": "fired_key", "AttributeType": "S"},
            {"AttributeName": "gsi_all", "AttributeType": "S"},
            {"AttributeName": "fired_at", "AttributeType": "S"},
        ],
        # Constant-PK index so the landing page can read the global recent feed
        # without scanning.
        "GlobalSecondaryIndexes": [{
            "IndexName": "recent-index",
            "KeySchema": [
                {"AttributeName": "gsi_all", "KeyType": "HASH"},
                {"AttributeName": "fired_at", "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": "ALL"},
        }],
    },
    "deliveries": {
        "KeySchema": [
            {"AttributeName": "alert_id", "KeyType": "HASH"},
            {"AttributeName": "user_id", "KeyType": "RANGE"},
        ],
        "AttributeDefinitions": [
            {"AttributeName": "alert_id", "AttributeType": "S"},
            {"AttributeName": "user_id", "AttributeType": "S"},
        ],
    },
}

_resource = None


def resource():
    global _resource
    if _resource is None:
        kwargs: Dict[str, Any] = {
            "region_name": config.AWS_REGION,
            "config": Config(retries={"max_attempts": 5, "mode": "standard"}),
        }
        if config.DYNAMO_ENDPOINT_URL:
            kwargs["endpoint_url"] = config.DYNAMO_ENDPOINT_URL
        _resource = boto3.resource("dynamodb", **kwargs)
    return _resource


def table(name: str):
    return resource().Table(name)


def ensure_tables() -> List[str]:
    """Idempotently create all four tables. Safe to call from every container."""
    created = []
    client = resource().meta.client
    existing = set()
    try:
        existing = set(client.list_tables().get("TableNames", []))
    except ClientError as exc:
        log.warning("could not list tables: %s", exc)

    for name, spec in TABLES.items():
        if name in existing:
            continue
        params = dict(spec)
        params["TableName"] = name
        params["BillingMode"] = "PAY_PER_REQUEST"
        try:
            client.create_table(**params)
            client.get_waiter("table_exists").wait(
                TableName=name, WaiterConfig={"Delay": 1, "MaxAttempts": 30}
            )
            created.append(name)
            log.info("created table %s", name)
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("ResourceInUseException", "TableAlreadyExistsException"):
                continue
            raise
    return created
