# AWS Lambda Notes

`lambda_worker.py` es el worker para AWS Lambda. No usa bucle infinito ni
`start_consuming()`.

Cada invocacion:

1. Conecta a RabbitMQ y PostgreSQL.
2. Lee hasta `BATCH_SIZE` mensajes con `basic_get`.
3. Procesa compras con `PostgresRepository` y `tickets`.
4. Hace ACK, retry con `x-retry-count` o DLQ.
5. Cierra conexiones y termina.

## Variables

```text
POSTGRES_HOST
POSTGRES_PORT
POSTGRES_USER
POSTGRES_PASSWORD
POSTGRES_DB
RABBITMQ_HOST
RABBITMQ_PORT
RABBITMQ_USER
RABBITMQ_PASS
RABBITMQ_QUEUE
RABBITMQ_DLQ
MAX_RETRIES
BATCH_SIZE
```

Para el scaler Lambda:

```text
AWS_REGION
AWS_LAMBDA_FUNCTION_NAME
MAX_INVOCATIONS
```

## Red

Lambda debe poder llegar a:

- RabbitMQ por `5672`.
- PostgreSQL por `5432`.

Si RabbitMQ o PostgreSQL estan en EC2 privada, pon Lambda en la misma VPC/subred
y permite el trafico con security groups.

## Empaquetado

`psycopg2-binary` puede fallar si el ZIP se crea desde Windows. Para AWS real,
empaqueta en Amazon Linux o usa una Lambda Layer compatible.
