"""tools/migrate_to_postgres.py: the legacy SQLite document store -> relational schema."""
import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))

import migrate_to_postgres as mig                      # noqa: E402

from conftest import load_service_module               # noqa: E402

store = load_service_module("webapp", "store")


def legacy_file(path):
    """The exact on-disk shape the previous release wrote: (pk, sk, doc JSON)."""
    conn = sqlite3.connect(path)
    for t in mig.TABLE_ORDER:
        conn.execute(f"CREATE TABLE {t} (pk TEXT, sk TEXT DEFAULT '', doc TEXT, PRIMARY KEY (pk, sk))")
    put = lambda t, pk, sk, d: conn.execute(f"INSERT INTO {t} VALUES (?,?,?)", (pk, sk, json.dumps(d)))
    put("users", "u1", "", {"user_id": "u1", "email": "Real@Example.com", "password_hash": "$2b$x",
                            "created_at": "2026-09-25T10:00:00Z", "slack_webhook_url": "",
                            "slack_verified_at": "", "is_guest": False})
    put("users", "u2", "", {"user_id": "u2", "email": "guest-1@guest.riskradar.invalid",
                            "password_hash": "!guest", "created_at": "2026-09-25T11:00:00Z",
                            "slack_webhook_url": "", "slack_verified_at": "", "is_guest": True})
    put("watchlists", "u1", "TSLA", {"user_id": "u1", "ticker": "TSLA", "added_at": "2026-09-25T10:01:00Z"})
    put("watchlists", "ghost", "AAPL", {"user_id": "ghost", "ticker": "AAPL", "added_at": "2026-09-25T10:01:00Z"})
    put("alerts", "TSLA", "k1", {"alert_id": "a1", "ticker": "TSLA", "fired_at": "2026-09-25T12:00:00.5Z",
                                 "window_end": "2026-09-25T12:00:00Z", "risk_score": "1.0",
                                 "alert_score": "1.806", "baseline_p": "1.214", "severity": "high",
                                 "message": "CRITICAL", "source": "simulated",
                                 "top_headline": "Tesla misses", "top_url": "https://x/1",
                                 "fired_key": "k1", "gsi_all": "ALERT"})
    put("alerts", "AAPL", "k2", {"alert_id": "a2", "ticker": "AAPL", "fired_at": "2026-09-25T13:00:00Z",
                                 "window_end": "2026-09-25T13:00:00Z", "risk_score": "0.97",
                                 "baseline_p": "0.91", "severity": "high", "message": "old alert, no alert_score"})
    put("deliveries", "a1", "u1", {"alert_id": "a1", "user_id": "u1", "status": "no_webhook",
                                   "fired_at": "2026-09-25T12:00:00Z", "delivered_at": "2026-09-25T12:00:01Z"})
    conn.commit()
    conn.close()


def test_legacy_store_migrates_with_orphans_skipped(database, tmp_path, capsys):
    src = tmp_path / "legacy.db"
    legacy_file(src)
    assert mig.main(["--from-sqlite", str(src)]) == 0

    u = store.user_by_email("real@example.com")                   # email normalised
    assert u["password_hash"] == "$2b$x" and u["is_guest"] is False
    assert store.watchlist("u1") == ["TSLA"]
    hist = store.alerts_for_user("u1")
    assert [(a["alert_id"], a["delivery_status"]) for a in hist] == [("a1", "no_webhook")]
    assert hist[0]["alert_score"] == 1.806 and hist[0]["source"] == "simulated"
    assert {a["alert_id"] for a in store.recent_alerts()} == {"a1", "a2"}
    assert next(a for a in store.recent_alerts() if a["alert_id"] == "a2")["alert_score"] == 0.97
    out = capsys.readouterr().out
    assert "watchlists       2      1        1" in out            # the orphan was skipped


def test_migration_is_idempotent(database, tmp_path, capsys):
    src = tmp_path / "legacy.db"
    legacy_file(src)
    mig.main(["--from-sqlite", str(src)])
    capsys.readouterr()
    mig.main(["--from-sqlite", str(src)])
    second = capsys.readouterr().out
    assert all(line.rstrip().endswith(" 0") for line in second.splitlines()
               if line.strip().split(" ")[0] in mig.TABLE_ORDER)
    assert store.user_count() == 2


def test_dry_run_writes_nothing(database, tmp_path):
    src = tmp_path / "legacy.db"
    legacy_file(src)
    mig.main(["--from-sqlite", str(src), "--dry-run"])
    assert store.user_count() == 0
