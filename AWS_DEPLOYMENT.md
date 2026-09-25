# AWS Deployment

Despliegue simple sin Terraform.

## Arquitectura

- EC2 cliente: workloads, `scaling/lambda_scaler.py` y métricas.
- EC2 RabbitMQ: cola principal y management.
- EC2 PostgreSQL: base de datos.
- AWS Lambda workers: ejecutan `lambda_worker.py` por batches.

## Security Groups

RabbitMQ EC2:

- `5672` desde EC2 cliente y Lambda.
- `15672` solo desde tu IP.

PostgreSQL EC2:

- `5432` desde EC2 cliente y Lambda.

Lambda:

- Salida hacia RabbitMQ `5672`.
- Salida hacia PostgreSQL `5432`.

Si RabbitMQ/PostgreSQL están en subred privada, pon Lambda en la misma VPC.

## Pasos

1. Crear EC2 PostgreSQL.
2. Ejecutar:

   ```bash
   bash scripts_setup/setup_ec2_postgres.sh
   ```

3. Crear EC2 RabbitMQ.
4. Ejecutar:

   ```bash
   RABBITMQ_USER=tickets RABBITMQ_PASS=tickets123 bash scripts_setup/setup_ec2_rabbitmq.sh
   ```

5. Crear EC2 cliente.
6. Subir o clonar el repo.
7. Ejecutar:

   ```bash
   bash scripts_setup/setup_ec2_client.sh
   ```

8. Crear `.env.aws` desde `.env.aws.example` y poner IPs reales.
9. Cargar variables:

   ```bash
   set -a
   source .env.aws
   set +a
   ```

10. Inicializar PostgreSQL:

    ```bash
    psql "host=$POSTGRES_HOST port=$POSTGRES_PORT user=$POSTGRES_USER dbname=$POSTGRES_DB" -f postgresql/schema.sql
    ```

11. Resetear RabbitMQ:

    ```bash
    python -m rabbitmq.reset_rabbit
    ```

12. Empaquetar Lambda:

    ```bash
    bash scripts/package_lambda_linux.sh
    ```

13. Subir `lambda_worker.zip` a AWS Lambda.
14. Configurar variables de entorno Lambda con `.env.aws`.
15. Ejecutar workload.
16. Ejecutar lambda scaler.
17. Sacar métricas.

## Comandos útiles

Probar PostgreSQL:

```bash
psql "host=$POSTGRES_HOST port=$POSTGRES_PORT user=$POSTGRES_USER dbname=$POSTGRES_DB" -c "SELECT 1;"
```

Probar RabbitMQ:

```bash
python - <<'PY'
import os, pika
credentials = pika.PlainCredentials(os.getenv("RABBITMQ_USER"), os.getenv("RABBITMQ_PASS"))
conn = pika.BlockingConnection(pika.ConnectionParameters(host=os.getenv("RABBITMQ_HOST"), port=int(os.getenv("RABBITMQ_PORT", 5672)), credentials=credentials))
print("RabbitMQ OK")
conn.close()
PY
```

Workload elástico:

```bash
python -m workloads.elastic_workload
```

Lambda scaler:

```bash
python -m scaling.lambda_scaler --batch-size "$BATCH_SIZE" --max-invocations "$MAX_INVOCATIONS" --interval 2
```

Métricas:

```bash
python -m metrics.metricas
```
