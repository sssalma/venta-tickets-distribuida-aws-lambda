#!/usr/bin/env bash
set -euo pipefail

sudo apt-get update
sudo apt-get install -y \
  python3 \
  python3-pip \
  python3-venv \
  git \
  unzip \
  postgresql-client \
  awscli

python3 -m venv .venv
. .venv/bin/activate

python -m pip install --upgrade pip
python -m pip install -r requirements.txt

if [ -f lambda_requirements.txt ]; then
  python -m pip install -r lambda_requirements.txt
fi

cat <<'EOF'
Cliente EC2 listo.

Comandos utiles:

  source .venv/bin/activate

  psql "host=$POSTGRES_HOST port=$POSTGRES_PORT user=$POSTGRES_USER dbname=$POSTGRES_DB"

  python - <<'PY'
import os, pika
credentials = pika.PlainCredentials(os.getenv("RABBITMQ_USER", "tickets"), os.getenv("RABBITMQ_PASS", "tickets123"))
conn = pika.BlockingConnection(pika.ConnectionParameters(host=os.getenv("RABBITMQ_HOST"), port=int(os.getenv("RABBITMQ_PORT", 5672)), credentials=credentials))
print("RabbitMQ OK")
conn.close()
PY

  python -m workloads.bm_numbered_uniforme --total 100 --delay 0.001
  python -m scaling.lambda_scaler --batch-size 10 --max-invocations 20 --interval 2
  python -m metrics.metricas
EOF
