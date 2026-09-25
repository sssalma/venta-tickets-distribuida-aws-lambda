#!/usr/bin/env bash
set -euo pipefail

RABBITMQ_USER="${RABBITMQ_USER:-tickets}"
RABBITMQ_PASS="${RABBITMQ_PASS:-tickets123}"
CONTAINER_NAME="sd-rabbitmq"

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

sudo docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
sudo docker run -d \
  --name "$CONTAINER_NAME" \
  --restart unless-stopped \
  -e RABBITMQ_DEFAULT_USER="$RABBITMQ_USER" \
  -e RABBITMQ_DEFAULT_PASS="$RABBITMQ_PASS" \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3.13-management-alpine

PUBLIC_IP="$(curl -fsS http://checkip.amazonaws.com 2>/dev/null || hostname -I | awk '{print $1}')"

echo "RabbitMQ listo"
echo "Management: http://${PUBLIC_IP}:15672"
echo "Usuario: ${RABBITMQ_USER}"
echo "Puertos: 5672 AMQP, 15672 Management"
