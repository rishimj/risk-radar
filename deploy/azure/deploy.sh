#!/usr/bin/env bash
# Deploy the committed HEAD to the Azure VM. Run from your laptop at the repo root:
#
#   VM=azureuser@<vm-ip> SITE_HOST=risk-radar.<vm-ip>.sslip.io deploy/azure/deploy.sh
#
# Ships only tracked files (git archive), so .env, .venv and anything else
# untracked can never leak onto the server. Secrets are generated ON the VM.
set -euo pipefail
: "${VM:?set VM=user@host}"
: "${SITE_HOST:?set SITE_HOST=risk-radar.<ip>.sslip.io}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_ed25519}"
LITE="${LITE:-0}"

cd "$(git rev-parse --show-toplevel)"
if ! git diff --quiet HEAD --; then
  echo "!! uncommitted changes; deploy ships HEAD only. Commit or stash first." >&2
  exit 1
fi

tarball=$(mktemp -t risk-radar.XXXXXX).tar.gz
git archive --format=tar.gz -o "$tarball" HEAD
echo "==> shipping $(git rev-parse --short HEAD) ($(du -h "$tarball" | cut -f1))"

scp -i "$SSH_KEY" -q "$tarball" "$VM:/tmp/risk-radar.tar.gz"
scp -i "$SSH_KEY" -q deploy/azure/install.sh "$VM:/tmp/risk-radar-install.sh"
ssh -i "$SSH_KEY" "$VM" "sudo SITE_HOST='$SITE_HOST' LITE='$LITE' bash /tmp/risk-radar-install.sh /tmp/risk-radar.tar.gz \
  && rm -f /tmp/risk-radar.tar.gz /tmp/risk-radar-install.sh"
rm -f "$tarball"

echo "==> checking https://$SITE_HOST"
for i in $(seq 1 30); do
  curl -sf "https://$SITE_HOST/healthz" >/dev/null && { echo "   live: https://$SITE_HOST"; exit 0; }
  sleep 4
done
echo "!! https://$SITE_HOST not answering yet; check: ssh $VM sudo journalctl -u caddy -n 50" >&2
exit 1
