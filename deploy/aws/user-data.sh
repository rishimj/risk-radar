#!/bin/bash
# EC2 user-data for a single-instance RiskRadar deployment.
#
# Target: x86_64 t3.xlarge (4 vCPU / 16 GB, ~$121/mo on-demand).
# NOT Graviton — PyFlink publishes no linux/aarch64 wheel at any version, so the
# Flink images would run emulated. On x86_64 every image in the stack is native.
#
# Usage: paste as user-data, or
#   aws ec2 run-instances ... --user-data file://user-data.sh
set -euxo pipefail

REPO_URL="${REPO_URL:-https://github.com/CHANGEME/riskradar.git}"
APP_DIR=/opt/riskradar
SSM_PREFIX="${SSM_PREFIX:-/riskradar}"
REGION="$(curl -s http://169.254.169.254/latest/meta-data/placement/region)"

dnf update -y
dnf install -y docker git

# Compose v2 as a CLI plugin (the standalone binary is deprecated).
mkdir -p /usr/local/lib/docker/cli-plugins
curl -fsSL -o /usr/local/lib/docker/cli-plugins/docker-compose \
  "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64"
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

systemctl enable --now docker
usermod -aG docker ec2-user

git clone "$REPO_URL" "$APP_DIR" || (cd "$APP_DIR" && git pull)
cd "$APP_DIR/riskradar"

# Secrets come from SSM Parameter Store, never from the AMI or the repo.
# Note what is deliberately ABSENT: DYNAMO_ENDPOINT_URL. Leaving it unset is what
# switches boto3 from dynamodb-local to real DynamoDB via the instance role, and
# it is the only difference between this deployment and a laptop.
{
  echo "SECRET_KEY=$(aws ssm get-parameter --name "$SSM_PREFIX/secret_key" \
        --with-decryption --region "$REGION" --query Parameter.Value --output text)"
  echo "AWS_REGION=$REGION"
  echo "COOKIE_SECURE=true"
  echo "LOG_LEVEL=INFO"
} > .env

# The stream engine is a compose profile; with no profile, nothing consumes
# news.raw. x86_64 runs Flink natively, so production uses the reference
# engine. Swap to --profile lite on a smaller instance.
docker compose --profile flink up -d --build

# Caddy terminates TLS and proxies to the webapp. Kept outside the compose file
# so the local stack stays plain HTTP with no certificate machinery.
if [ -f "$APP_DIR/riskradar/deploy/aws/Caddyfile" ]; then
  docker run -d --name caddy --restart unless-stopped \
    --network host \
    -v "$APP_DIR/riskradar/deploy/aws/Caddyfile:/etc/caddy/Caddyfile:ro" \
    -v caddy_data:/data -v caddy_config:/config \
    caddy:2
fi

# Seed the per-ticker baselines so the first hours aren't a cold-start alert
# storm. Safe to re-run; it no-ops for tickers already above the sample floor.
sleep 60
docker compose run --rm seed-baseline || true
