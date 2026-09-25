import json
import os
import uuid
from datetime import datetime, timezone

import pika

from base.postgres_repository import PostgresRepository
from base.tickets import tickets


WORKER_ID = f"lambda-worker-{str(uuid.uuid4())[:8]}"


def env_int(name, default):
    return int(os.getenv(name, default))


def get_config():
    return {
        "rabbit_host": os.getenv("RABBITMQ_HOST", "localhost"),
        "rabbit_port": env_int("RABBITMQ_PORT", 5672),
        "rabbit_user": os.getenv("RABBITMQ_USER", "guest"),
        "rabbit_pass": os.getenv("RABBITMQ_PASS", "guest"),
        "queue_name": os.getenv("RABBITMQ_QUEUE", "cola_tickets"),
        "dlq_name": os.getenv("RABBITMQ_DLQ", "cola_tickets_dlq"),
        "postgres_host": os.getenv("POSTGRES_HOST", "localhost"),
        "postgres_port": env_int("POSTGRES_PORT", 5432),
        "postgres_user": os.getenv("POSTGRES_USER", "tickets"),
        "postgres_password": os.getenv("POSTGRES_PASSWORD", "tickets"),
        "postgres_db": os.getenv("POSTGRES_DB", "tickets"),
        "batch_size": env_int("BATCH_SIZE", 10),
        "max_retries": env_int("MAX_RETRIES", 3),
    }


def connect_rabbit(config):
    credentials = pika.PlainCredentials(
        config["rabbit_user"],
        config["rabbit_pass"],
    )
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=config["rabbit_host"],
            port=config["rabbit_port"],
            credentials=credentials,
        )
    )
    channel = connection.channel()
    channel.queue_declare(queue=config["queue_name"], durable=True)
    channel.queue_declare(queue=config["dlq_name"], durable=True)
    channel.basic_qos(prefetch_count=config["batch_size"])
    return connection, channel


def connect_postgres(config):
    repo = PostgresRepository(
        host=config["postgres_host"],
        port=config["postgres_port"],
        user=config["postgres_user"],
        password=config["postgres_password"],
        database=config["postgres_db"],
    )
    repo.conectar()
    return repo


def retry_count(properties):
    if not properties or not properties.headers:
        return 0
    return int(properties.headers.get("x-retry-count", 0))


def normalize_created_at(created_at):
    if isinstance(created_at, (int, float)):
        return datetime.fromtimestamp(created_at, tz=timezone.utc)
    return created_at


def publish_dlq(channel, body, properties, config, reason):
    headers = {}
    if properties and properties.headers:
        headers.update(properties.headers)

    headers["dlq_reason"] = reason
    headers["worker_id"] = WORKER_ID

    channel.basic_publish(
        exchange="",
        routing_key=config["dlq_name"],
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
            headers=headers,
        ),
    )


def publish_retry(channel, body, properties, config, current_retry_count):
    headers = {}
    if properties and properties.headers:
        headers.update(properties.headers)

    headers["x-retry-count"] = current_retry_count + 1
    headers["last_worker_id"] = WORKER_ID

    channel.basic_publish(
        exchange="",
        routing_key=config["queue_name"],
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
            headers=headers,
        ),
    )


def process_purchase(service, data):
    cliente_id = data.get("cliente_id")
    request_id = data.get("request_id")
    seat_id = data.get("seat_id")
    message_created_at = normalize_created_at(data.get("created_at"))

    if seat_id is not None:
        return service.comprar_numerada(
            cliente_id,
            seat_id,
            request_id,
            worker_id=WORKER_ID,
            created_at=message_created_at,
        )

    return service.comprar_no_numerada(
        cliente_id,
        request_id,
        worker_id=WORKER_ID,
        created_at=message_created_at,
    )


def process_message(channel, method, properties, body, service, config, summary):
    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        publish_dlq(channel, body, properties, config, reason="json_decode_error")
        channel.basic_ack(delivery_tag=method.delivery_tag)
        summary["processed"] += 1
        summary["fail"] += 1
        summary["dlq"] += 1
        print(f"[{WORKER_ID}] JSON invalido enviado a DLQ: {exc}")
        return

    try:
        if data.get("type") == "QUIT" or data.get("tipo") == "QUIT":
            channel.basic_ack(delivery_tag=method.delivery_tag)
            summary["processed"] += 1
            summary["fail"] += 1
            print(f"[{WORKER_ID}] Mensaje QUIT ignorado en Lambda")
            return

        resultado = process_purchase(service, data)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        summary["processed"] += 1

        if resultado.status == "DUPLICATE":
            summary["duplicates"] += 1
        elif resultado.ok:
            summary["success"] += 1
        else:
            summary["fail"] += 1

        print(
            f"[{WORKER_ID}] Request {data.get('request_id')}: "
            f"{resultado.status} - {resultado.motivo}"
        )

    except Exception as exc:
        current_retry_count = retry_count(properties)
        summary["processed"] += 1

        if current_retry_count < config["max_retries"]:
            publish_retry(channel, body, properties, config, current_retry_count)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            summary["retried"] += 1
            print(
                f"[{WORKER_ID}] Error tecnico. Reintento "
                f"{current_retry_count + 1}/{config['max_retries']}: {exc}"
            )
        else:
            publish_dlq(channel, body, properties, config, reason=str(exc))
            channel.basic_ack(delivery_tag=method.delivery_tag)
            summary["fail"] += 1
            summary["dlq"] += 1
            print(f"[{WORKER_ID}] Error tecnico enviado a DLQ: {exc}")


def lambda_handler(event, context):
    config = get_config()
    summary = {
        "processed": 0,
        "success": 0,
        "fail": 0,
        "duplicates": 0,
        "dlq": 0,
        "retried": 0,
    }

    rabbit_connection = None
    repo = None

    try:
        rabbit_connection, channel = connect_rabbit(config)
        repo = connect_postgres(config)
        service = tickets(repo)

        for _ in range(config["batch_size"]):
            method, properties, body = channel.basic_get(
                queue=config["queue_name"],
                auto_ack=False,
            )

            if method is None:
                break

            process_message(channel, method, properties, body, service, config, summary)

        if summary["processed"] == 0:
            summary["message"] = "no messages"
            return summary

        return summary

    finally:
        if rabbit_connection and rabbit_connection.is_open:
            rabbit_connection.close()

        if repo:
            repo.desconectar()


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=2))
