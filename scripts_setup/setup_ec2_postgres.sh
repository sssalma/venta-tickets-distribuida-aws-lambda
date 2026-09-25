#!/usr/bin/env bash
set -euo pipefail

POSTGRES_USER="${POSTGRES_USER:-tickets}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-tickets}"
POSTGRES_DB="${POSTGRES_DB:-tickets}"
CONTAINER_NAME="sd-postgres"

install_docker() {
  if command -v docker >/dev/null 2>&1; then
    return
  fi

  sudo apt-get update
  sudo apt-get install -y ca-certificates curl gnupg
  curl -fsSL https://get.docker.com | sudo sh
  sudo usermod -aG docker "$USER" || true
}

install_docker

sudo docker volume create pgdata >/dev/null
sudo docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
sudo docker run -d \
  --name "$CONTAINER_NAME" \
  --restart unless-stopped \
  -e POSTGRES_USER="$POSTGRES_USER" \
  -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  -e POSTGRES_DB="$POSTGRES_DB" \
  -v pgdata:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:16-alpine

HOST_IP="$(hostname -I | awk '{print $1}')"

echo "PostgreSQL listo"
echo "Host: ${HOST_IP}"
echo "Puerto: 5432"
echo "Usuario: ${POSTGRES_USER}"
echo "DB: ${POSTGRES_DB}"
