# Deploying RiskRadar to a small shared VM

This is how the public demo runs: one Ubuntu 24.04 VM (2 vCPU, 4 GB) that also
hosts another app. The full compose stack (Kafka, Flink, PostgreSQL) needs
several GB, so the VM runs **`services/standalone`** instead: the same scoring,
windowing, alerting and web code in one Python process, with an in-process
queue in place of Kafka, and a private PostgreSQL cluster on a unix socket.

```
internet ──443──> Caddy ──> 127.0.0.1:3001  risk-radar.service  (web + ingestion + stream engine + FinBERT)
                                              ├─ unix socket ─> risk-radar-postgres.service  (users, alerts; no TCP port)
                                              └─ unix socket ─> risk-radar-redis.service     (baselines, features; no TCP port)
```

## Deploy (from your laptop)

```bash
VM=azureuser@<vm-ip> SITE_HOST=risk-radar.<vm-ip>.sslip.io deploy/azure/deploy.sh
```

`deploy.sh` ships `git archive HEAD` (tracked files only, so `.env` and friends
can never leak), then runs `install.sh` on the VM with sudo. Re-run it to upgrade.
`LITE=1` skips torch/FinBERT (~150 MB instead of ~0.9 GB; sentiment falls back
to VADER).

## What install.sh does, and does not, touch

| Does | Does not |
|---|---|
| create system user `riskradar`, everything in `/srv/risk-radar` (750) | stop, restart or edit any other service |
| install `python3-venv`, `redis-server` and `postgresql-16` | use the system Redis or PostgreSQL (Ubuntu's default Postgres cluster is never created; a pre-existing one is left alone; a fresh system Redis is disabled) |
| run a private PostgreSQL cluster in `/srv/risk-radar/pgdata`, peer auth over a unix socket | open any database port |
| migrate an existing SQLite install's users and alerts into PostgreSQL, keeping the SQLite file as a backup | delete any data |
| write `risk-radar.env` (600) with a generated `SECRET_KEY`, once | ever overwrite an existing env file |
| install three systemd units, `enable --now` them | bind anything to 0.0.0.0 (app: 127.0.0.1:3001, Postgres and Redis: unix sockets only) |
| **append** a site block to `/etc/caddy/Caddyfile` | replace the Caddyfile: it backs it up, validates the result with `caddy validate`, restores the backup on failure, and only then `reload`s (graceful) |

If the app fails its health check, the script stops **before** the Caddyfile is
touched.

## Limits that protect the rest of the VM

- `MemoryMax=1500M` (`MemoryHigh=1300M`) on the app, `256M` on its PostgreSQL (~32 MB measured), `128M` on its Redis. Measured: ~0.8-0.9 GB steady with FinBERT.
- `CPUQuota=100%`: at most one of the two cores; one BLAS thread.
- Sandboxing: `ProtectSystem=strict`, `NoNewPrivileges`, `PrivateTmp`, no capabilities, `ReadWritePaths` limited to `data/` and `models/`. `systemd-analyze security risk-radar` scores 3.0 ("OK").
- Code and venv are owned by root, readable by the service: the running app cannot modify itself.

## Checks after deploying

```bash
curl -s https://$SITE_HOST/healthz                 # {"status":"ok"}
sudo systemctl restart risk-radar && sleep 20 && curl -s https://$SITE_HOST/healthz
free -h
systemctl show risk-radar -p MemoryCurrent
```

First start seeds 24 h of per-ticker baselines with FinBERT (~40 s at one core);
until then alerting stays silent by design.

## Costs and abuse

There are no paid APIs: news feeds are free and keyless, sentiment runs locally.
What a visitor can trigger is bounded in `services/webapp/src/guard.py` (per-IP,
per-user and site-wide rate limits; 300 new accounts per day site-wide; guest
accounts deleted after 48 h and barred from Slack; one simulation site-wide per
4 minutes). Caddy caps request bodies at 64 KB.

## Uninstall

```bash
sudo systemctl disable --now risk-radar risk-radar-redis risk-radar-postgres
sudo rm /etc/systemd/system/risk-radar.service /etc/systemd/system/risk-radar-redis.service /etc/systemd/system/risk-radar-postgres.service
sudo systemctl daemon-reload
# remove the "# ---- RiskRadar ..." block from /etc/caddy/Caddyfile, then:
sudo systemctl reload caddy
sudo rm -rf /srv/risk-radar && sudo userdel riskradar
```
