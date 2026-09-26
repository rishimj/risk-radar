#!/usr/bin/env bash
# Install or upgrade RiskRadar on a shared Ubuntu 24.04 VM.
#
#   sudo SITE_HOST=risk-radar.<ip>.sslip.io bash install.sh /tmp/risk-radar.tar.gz
#
# Run by deploy/azure/deploy.sh from your laptop; safe to re-run (every step
# is idempotent). Conventions it follows for the shared VM:
#   * dedicated system user `riskradar`, everything under /srv/risk-radar
#   * app on 127.0.0.1:3001 only; Caddy is the only public entry point
#   * systemd units with MemoryMax and sandboxing
#   * the existing Caddyfile is APPENDED to (backed up, validated, rolled back
#     on failure), never replaced
#   * nothing belonging to any other service is stopped, edited or restarted
#     (Caddy is only reloaded, which is graceful)
#
# Options (env):
#   SITE_HOST   public hostname for the Caddy block (required unless SKIP_CADDY=1)
#   LITE=1      skip torch/FinBERT; sentiment falls back to VADER/FinVADER (~150 MB RSS)
#   SKIP_CADDY=1, SKIP_SYSTEMD=1   for testing the install in a container
set -euo pipefail

TARBALL="${1:?usage: install.sh /path/to/risk-radar.tar.gz}"
ROOT=/srv/risk-radar
SVC_USER=riskradar
PORT=3001
LITE="${LITE:-0}"
SKIP_CADDY="${SKIP_CADDY:-0}"
SKIP_SYSTEMD="${SKIP_SYSTEMD:-0}"
TORCH_VERSION=2.5.1

log()  { printf '\n==> %s\n' "$*"; }
die()  { printf '\n!! %s\n' "$*" >&2; exit 1; }

[ "$(id -u)" -eq 0 ] || die "run as root (sudo)"
[ -f "$TARBALL" ] || die "no such file: $TARBALL"
if [ "$SKIP_CADDY" != 1 ]; then
  [ -n "${SITE_HOST:-}" ] || die "set SITE_HOST (e.g. risk-radar.20.25.227.252.sslip.io)"
  [[ "$SITE_HOST" =~ ^[a-z0-9.-]+$ ]] || die "SITE_HOST looks wrong: $SITE_HOST"
fi

# ---------------------------------------------------------------------------
log "preflight"
# Port 3001 must be free, or already ours. Never take over someone else's port.
if ss -ltnH "sport = :$PORT" 2>/dev/null | grep -q .; then
  systemctl is-active --quiet risk-radar 2>/dev/null \
    || die "port $PORT is already in use by another process: $(ss -ltnpH "sport = :$PORT")"
fi
avail_kb=$(df --output=avail -k /srv 2>/dev/null | tail -1 || echo 0)
need_kb=$([ "$LITE" = 1 ] && echo 700000 || echo 3500000)
[ "${avail_kb:-0}" -ge "$need_kb" ] || die "not enough disk under /srv ($((avail_kb/1024)) MB free, need ~$((need_kb/1024)) MB)"
free -h || true

# ---------------------------------------------------------------------------
log "system packages (python venv, redis-server, postgresql-16 binaries)"
redis_was_installed=0
dpkg -s redis-server >/dev/null 2>&1 && redis_was_installed=1
redis_was_active=0
systemctl is-active --quiet redis-server 2>/dev/null && redis_was_active=1
export DEBIAN_FRONTEND=noninteractive
# needrestart (on by default in Ubuntu 24.04) may otherwise restart OTHER
# services whose libraries an install touched. List only; restart nothing.
export NEEDRESTART_MODE=l NEEDRESTART_SUSPEND=1
# Ubuntu's postgresql package creates and starts a system-wide "main" cluster on
# :5432. RiskRadar runs its own private cluster instead, so when postgresql is
# not installed yet, tell postgresql-common not to create that default cluster.
# An existing PostgreSQL installation and its clusters are left untouched.
if ! dpkg -s postgresql-common >/dev/null 2>&1; then
  install -d /etc/postgresql-common
  grep -qs '^create_main_cluster' /etc/postgresql-common/createcluster.conf \
    || echo "create_main_cluster = false" >> /etc/postgresql-common/createcluster.conf
fi
apt-get update -qq
# --no-upgrade: packages already present (python3, curl, ...) stay at their
# current version, so nothing another app relies on moves underneath it.
apt-get install -y -qq --no-upgrade python3 python3-venv python3-pip redis-server postgresql-16 curl ca-certificates >/dev/null
# Installing redis-server auto-starts a system-wide instance on :6379. RiskRadar
# does not use it (it runs its own on a unix socket), so if THIS script is what
# installed the package, turn that default instance back off. If a system Redis
# already existed or was running, it is left exactly as it was.
if [ "$redis_was_installed" = 0 ] && [ "$redis_was_active" = 0 ] && [ "$SKIP_SYSTEMD" != 1 ]; then
  systemctl disable --now redis-server >/dev/null 2>&1 || true
fi

# ---------------------------------------------------------------------------
log "user and directories"
if ! id "$SVC_USER" >/dev/null 2>&1; then
  useradd --system --create-home --home-dir "$ROOT" --shell /usr/sbin/nologin "$SVC_USER"
fi
install -d -o "$SVC_USER" -g "$SVC_USER" -m 750 "$ROOT"
for d in data data/cache models redis run pgdata; do
  install -d -o "$SVC_USER" -g "$SVC_USER" -m 750 "$ROOT/$d"
done

# ---------------------------------------------------------------------------
log "code (from $TARBALL)"
stage="$ROOT/app.new"
rm -rf "$stage"
install -d -o root -g "$SVC_USER" -m 750 "$stage"
tar -xzf "$TARBALL" -C "$stage"
# Code is owned by root and only readable by the service user: the running
# app can never modify itself.
chown -R root:"$SVC_USER" "$stage"
find "$stage" -type d -exec chmod 750 {} +
find "$stage" -type f -exec chmod 640 {} +
rm -rf "$ROOT/app.old"
[ -d "$ROOT/app" ] && mv "$ROOT/app" "$ROOT/app.old"
mv "$stage" "$ROOT/app"

# ---------------------------------------------------------------------------
log "python environment"
if [ ! -x "$ROOT/venv/bin/python" ]; then
  python3 -m venv "$ROOT/venv"
fi
"$ROOT/venv/bin/pip" install -q --upgrade pip
"$ROOT/venv/bin/pip" install -q -r "$ROOT/app/services/standalone/requirements.txt"
# The shared library: a normal install first (pulls its dependencies on a fresh
# venv), then a forced reinstall of just its own code, because a regular
# (non-editable) install keeps stale code on upgrade when the version is unchanged.
"$ROOT/venv/bin/pip" install -q "$ROOT/app/libs/core"
"$ROOT/venv/bin/pip" install -q --force-reinstall --no-deps "$ROOT/app/libs/core"
if [ "$LITE" != 1 ]; then
  "$ROOT/venv/bin/pip" install -q --index-url https://download.pytorch.org/whl/cpu "torch==$TORCH_VERSION"
  "$ROOT/venv/bin/pip" install -q -r "$ROOT/app/services/standalone/requirements-finbert.txt"
else
  "$ROOT/venv/bin/pip" uninstall -q -y torch transformers >/dev/null 2>&1 || true
fi
chown -R root:"$SVC_USER" "$ROOT/venv"
chmod -R g+rX,o-rwx "$ROOT/venv"

# ---------------------------------------------------------------------------
log "secrets and config"
envfile="$ROOT/risk-radar.env"
if [ ! -f "$envfile" ]; then
  key=$(python3 -c 'import secrets; print(secrets.token_hex(32))')
  sed "s/^SECRET_KEY=.*/SECRET_KEY=$key/" "$ROOT/app/deploy/azure/risk-radar.env.example" > "$envfile"
  echo "   generated a new SECRET_KEY"
else
  echo "   keeping existing $envfile"
fi
chown "$SVC_USER:$SVC_USER" "$envfile"
chmod 600 "$envfile"
install -o "$SVC_USER" -g "$SVC_USER" -m 600 "$ROOT/app/deploy/azure/redis.conf" "$ROOT/redis.conf"
# Upgrading from the SQLite release: point the app at PostgreSQL. The rest of
# the existing env file (SECRET_KEY, tuning) is kept as it is.
if ! grep -q '^DATABASE_URL=' "$envfile"; then
  sed -i '/^DB_BACKEND=/d; /^SQLITE_PATH=/d' "$envfile"
  printf '\n# Private PostgreSQL over its unix socket (peer auth as the riskradar user).\n' >> "$envfile"
  echo "DATABASE_URL=postgresql+psycopg://riskradar@/riskradar?host=$ROOT/run" >> "$envfile"
  echo "   switched the app to PostgreSQL"
fi

if [ "$LITE" != 1 ]; then
  log "pre-downloading FinBERT (~440 MB, once)"
  runuser -u "$SVC_USER" -- env HOME="$ROOT" HF_HOME="$ROOT/models" \
    "$ROOT/venv/bin/python" -c "
from transformers import AutoModelForSequenceClassification, AutoTokenizer
AutoTokenizer.from_pretrained('ProsusAI/finbert'); AutoModelForSequenceClassification.from_pretrained('ProsusAI/finbert')
print('   FinBERT cached')"
fi

# ---------------------------------------------------------------------------
if [ "$SKIP_SYSTEMD" != 1 ]; then
  log "systemd units"
  install -m 644 "$ROOT/app/deploy/azure/risk-radar-redis.service" /etc/systemd/system/risk-radar-redis.service
  install -m 644 "$ROOT/app/deploy/azure/risk-radar-postgres.service" /etc/systemd/system/risk-radar-postgres.service
  install -m 644 "$ROOT/app/deploy/azure/risk-radar.service" /etc/systemd/system/risk-radar.service
  systemctl daemon-reload
  systemctl enable --now risk-radar-redis

  log "PostgreSQL (private cluster, unix socket only)"
  PGBIN=/usr/lib/postgresql/16/bin
  if [ ! -f "$ROOT/pgdata/PG_VERSION" ]; then
    # Peer auth on the socket, nothing over TCP: only the riskradar OS user can
    # connect, as the riskradar role, with no password to leak.
    runuser -u "$SVC_USER" -- "$PGBIN/initdb" -D "$ROOT/pgdata" -U "$SVC_USER" \
      --auth-local=peer --auth-host=reject --encoding=UTF8 --locale=C.UTF-8 >/dev/null
    echo "   initialised $ROOT/pgdata"
  fi
  systemctl enable --now risk-radar-postgres
  for i in $(seq 1 30); do
    runuser -u "$SVC_USER" -- "$PGBIN/pg_isready" -q -h "$ROOT/run" && break
    sleep 1
  done
  runuser -u "$SVC_USER" -- "$PGBIN/pg_isready" -q -h "$ROOT/run" || die "PostgreSQL did not start"
  if ! runuser -u "$SVC_USER" -- "$PGBIN/psql" -h "$ROOT/run" -d postgres -tAc \
      "SELECT 1 FROM pg_database WHERE datname='riskradar'" | grep -q 1; then
    runuser -u "$SVC_USER" -- "$PGBIN/createdb" -h "$ROOT/run" riskradar
    echo "   created database riskradar"
  fi

  # One-time data migration from the SQLite release. The app is stopped first
  # so nothing writes to SQLite mid-copy; the SQLite file is kept as a backup.
  legacy="$ROOT/data/riskradar.db"
  if [ -f "$legacy" ]; then
    log "migrating users, watchlists and alert history from SQLite"
    systemctl stop risk-radar 2>/dev/null || true
    runuser -u "$SVC_USER" -- env $(grep '^DATABASE_URL=' "$envfile") \
      "$ROOT/venv/bin/python" "$ROOT/app/tools/migrate_to_postgres.py" --from-sqlite "$legacy" \
      || die "migration failed; SQLite data is untouched at $legacy"
    stamp=$(date +%Y%m%d%H%M%S)
    for f in "$legacy" "$legacy-wal" "$legacy-shm"; do
      [ -f "$f" ] && mv "$f" "$f.migrated-$stamp"
    done
    echo "   SQLite file kept as $legacy.migrated-$stamp"
  fi

  systemctl enable risk-radar
  systemctl restart risk-radar

  log "waiting for the app on 127.0.0.1:$PORT"
  for i in $(seq 1 60); do
    curl -sf "http://127.0.0.1:$PORT/healthz" >/dev/null && break
    sleep 2
  done
  curl -sf "http://127.0.0.1:$PORT/healthz" >/dev/null || {
    journalctl -u risk-radar -n 60 --no-pager || true
    die "app did not come up; the Caddyfile has NOT been touched"
  }
  echo "   healthy"
fi
rm -rf "$ROOT/app.old"

# ---------------------------------------------------------------------------
if [ "$SKIP_CADDY" != 1 ]; then
  log "Caddy site block for $SITE_HOST"
  cf=/etc/caddy/Caddyfile
  [ -f "$cf" ] || die "$cf not found"
  if grep -q "^# ---- RiskRadar (appended by deploy/azure/install.sh) ----" "$cf"; then
    echo "   block already present; leaving the Caddyfile unchanged"
  else
    backup="$cf.bak-riskradar-$(date +%Y%m%d%H%M%S)"
    cp -p "$cf" "$backup"
    sed "s/__SITE_HOST__/$SITE_HOST/" "$ROOT/app/deploy/azure/Caddyfile.snippet" >> "$cf"
    if ! caddy validate --config "$cf" --adapter caddyfile >/dev/null 2>&1; then
      cp -p "$backup" "$cf"
      caddy validate --config "$cf" --adapter caddyfile || true
      die "new Caddyfile failed validation; restored $backup (Caddy was not reloaded)"
    fi
    echo "   appended (backup: $backup)"
  fi
  systemctl reload caddy
  sleep 3
  journalctl -u caddy -n 20 --no-pager || true
fi

log "done"
echo "   service : systemctl status risk-radar"
echo "   logs    : journalctl -u risk-radar -f"
[ "$SKIP_CADDY" != 1 ] && echo "   site    : https://$SITE_HOST  (first request may take ~30 s while Caddy gets a certificate)"
exit 0
