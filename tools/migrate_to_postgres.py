#!/usr/bin/env python3
"""Migrate RiskRadar's users, watchlists and alert history into PostgreSQL.

Source: the legacy single-node store, a SQLite file holding DynamoDB-shaped
JSON documents (one `doc` column per row; tables users, watchlists, alerts,
deliveries). Target: DATABASE_URL, using the relational schema in
riskcore/db.py.

    DATABASE_URL=postgresql+psycopg://... python tools/migrate_to_postgres.py \\
        --from-sqlite /srv/risk-radar/data/riskradar.db

Safe to re-run: every insert is ON CONFLICT DO NOTHING, so a second run copies
nothing new. Rows whose foreign keys point at nothing (a delivery for a deleted
user, say) are skipped and counted rather than failing the whole migration.
The whole copy is one transaction: it lands completely or not at all.
"""
from typing import Dict, Iterable, List, Optional, Tuple
import argparse
import json
import sqlite3
import sys

from riskcore import config, db
from riskcore.models import parse_iso

TABLE_ORDER = ["users", "watchlists", "alerts", "deliveries"]    # parents first


def _ts(value) -> Optional[object]:
    if not value:
        return None
    try:
        return parse_iso(str(value))
    except ValueError:
        return None


def _float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def read_legacy_sqlite(path: str) -> Dict[str, List[dict]]:
    # A normal (read-write) open, though nothing is written: SQLite needs it to
    # replay a write-ahead log left behind by an unclean shutdown. mode=ro can
    # silently miss those pages.
    conn = sqlite3.connect(path)
    try:
        present = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        out = {}
        for name in TABLE_ORDER:
            out[name] = ([json.loads(r[0]) for r in conn.execute(f"SELECT doc FROM {name}")]
                         if name in present else [])
        return out
    finally:
        conn.close()


def convert(docs: Dict[str, List[dict]]) -> Tuple[Dict[str, List[dict]], Dict[str, int]]:
    """Legacy documents -> rows for the relational schema, dropping orphans."""
    skipped = {name: 0 for name in TABLE_ORDER}

    users, emails = [], set()
    for d in docs["users"]:
        email = str(d.get("email", "")).strip().lower()
        if not d.get("user_id") or not email or email in emails or not _ts(d.get("created_at")):
            skipped["users"] += 1
            continue
        emails.add(email)
        users.append({
            "user_id": d["user_id"], "email": email,
            "password_hash": d.get("password_hash", ""),
            "created_at": _ts(d.get("created_at")),
            "slack_webhook_url": d.get("slack_webhook_url") or "",
            "slack_verified_at": _ts(d.get("slack_verified_at")),
            "is_guest": bool(d.get("is_guest", False)),
        })
    user_ids = {u["user_id"] for u in users}

    watchlists = []
    for d in docs["watchlists"]:
        if d.get("user_id") not in user_ids or not d.get("ticker"):
            skipped["watchlists"] += 1
            continue
        watchlists.append({"user_id": d["user_id"], "ticker": d["ticker"],
                           "added_at": _ts(d.get("added_at")) or _ts(d.get("created_at"))
                           or users[0]["created_at"]})

    alerts = []
    for d in docs["alerts"]:
        if not d.get("alert_id") or not _ts(d.get("fired_at")):
            skipped["alerts"] += 1
            continue
        risk = _float(d.get("risk_score"))
        alerts.append({
            "alert_id": d["alert_id"], "ticker": d.get("ticker", ""),
            "fired_at": _ts(d.get("fired_at")), "window_end": _ts(d.get("window_end")),
            "risk_score": risk, "alert_score": _float(d.get("alert_score"), risk) or risk,
            "baseline_p": _float(d.get("baseline_p")), "severity": d.get("severity", "low"),
            "message": d.get("message", ""), "source": d.get("source", "live"),
            "top_headline": d.get("top_headline") or "", "top_url": d.get("top_url") or "",
        })
    alert_ids = {a["alert_id"] for a in alerts}

    deliveries = []
    for d in docs["deliveries"]:
        if d.get("alert_id") not in alert_ids or d.get("user_id") not in user_ids:
            skipped["deliveries"] += 1
            continue
        deliveries.append({"alert_id": d["alert_id"], "user_id": d["user_id"],
                           "status": d.get("status", "unknown"),
                           "delivered_at": _ts(d.get("delivered_at")) or _ts(d.get("fired_at"))})

    return {"users": users, "watchlists": watchlists, "alerts": alerts,
            "deliveries": deliveries}, skipped


def write(rows: Dict[str, List[dict]]) -> Dict[str, int]:
    """Insert everything in one transaction, ignoring rows that already exist."""
    db.ensure_schema()
    dialect = db.engine().dialect.name
    if dialect == "postgresql":
        from sqlalchemy.dialects.postgresql import insert
    else:
        from sqlalchemy.dialects.sqlite import insert
    from sqlalchemy import func, select
    tables = {t.name: t for t in db.metadata.sorted_tables}
    inserted = {}
    with db.begin() as conn:
        for name in TABLE_ORDER:
            # Counted, not read from rowcount: bulk-insert rowcounts differ by driver.
            before = conn.execute(select(func.count()).select_from(tables[name])).scalar_one()
            if rows[name]:
                conn.execute(insert(tables[name]).on_conflict_do_nothing(), rows[name])
            after = conn.execute(select(func.count()).select_from(tables[name])).scalar_one()
            inserted[name] = after - before
    return inserted


def main(argv: Optional[Iterable[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--from-sqlite", required=True, help="legacy SQLite file")
    ap.add_argument("--dry-run", action="store_true", help="read and convert, write nothing")
    args = ap.parse_args(argv)

    docs = read_legacy_sqlite(args.from_sqlite)
    rows, skipped = convert(docs)
    print(f"source {args.from_sqlite}")
    print(f"target {config.DATABASE_URL.split('@')[-1]}")
    print(f"  {'table':<11}{'read':>7}{'valid':>7}{'skipped':>9}{'inserted':>10}")
    inserted = {n: 0 for n in TABLE_ORDER} if args.dry_run else write(rows)
    for name in TABLE_ORDER:
        print(f"  {name:<11}{len(docs[name]):>7}{len(rows[name]):>7}{skipped[name]:>9}"
              f"{inserted[name]:>10}")
    print("dry run: nothing written" if args.dry_run else "done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
