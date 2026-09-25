import json
import os
import sys
import uuid

import pika

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from base.postgres_repository import PostgresRepository
from base.tickets import tickets


WORKER_ID = f"worker-{str(uuid.uuid4())[:8]}"

QUEUE_NAME = os.getenv("RABBITMQ_QUEUE", "cola_tickets")
DLQ_NAME = os.getenv("RABBITMQ_DLQ", "cola_tickets_dlq")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))

repo = PostgresRepository(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", 5432)),
    user=os.getenv("POSTGRES_USER", "tickets"),
    password=os.getenv("POSTGRES_PASSWORD", "tickets"),
    database=os.getenv("POSTGRES_DB", "tickets"),
)
service = tickets(repo)


def publicar_en_dlq(ch, body, properties, reason):
    headers = {}
    if properties and properties.headers:
        headers.update(properties.headers)

    headers["dlq_reason"] = reason
    headers["worker_id"] = WORKER_ID

    ch.basic_publish(
        exchange="",
        routing_key=DLQ_NAME,
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
            headers=headers,
        ),
    )


def republicar_con_retry(ch, body, properties, retry_count):
    headers = {}
    if properties and properties.headers:
        headers.update(properties.headers)

    headers["x-retry-count"] = retry_count + 1
    headers["last_worker_id"] = WORKER_ID

    ch.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
            headers=headers,
        ),
    )


def obtener_retry_count(properties):
    if not properties or not properties.headers:
        return 0
    return int(properties.headers.get("x-retry-count", 0))


def procesar_compra(ch, method, properties, body):
    try:
        data = json.loads(body)

        if data.get("type") == "QUIT" or data.get("tipo") == "QUIT":
            print(f"\n[{WORKER_ID}] Recibido QUIT. Apagando worker...")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            ch.stop_consuming()
            return

        cliente_id = data.get("cliente_id")
        request_id = data.get("request_id")
        seat_id = data.get("seat_id")
        message_created_at = data.get("created_at")

        print(f"\n[{WORKER_ID}] Procesando request {request_id}...")

        if seat_id is not None:
            resultado = service.comprar_numerada(
                cliente_id,
                seat_id,
                request_id,
                worker_id=WORKER_ID,
                created_at=message_created_at,
            )
        else:
            resultado = service.comprar_no_numerada(
                cliente_id,
                request_id,
                worker_id=WORKER_ID,
                created_at=message_created_at,
            )

        print(
            f"[{WORKER_ID}] Request {request_id}: "
            f"{resultado.status} - {resultado.motivo}"
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except json.JSONDecodeError as e:
        print(f"[{WORKER_ID}] Error JSON. Enviando a DLQ: {e}")
        publicar_en_dlq(ch, body, properties, reason="json_decode_error")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        retry_count = obtener_retry_count(properties)

        print(
            f"[{WORKER_ID}] Error procesando mensaje: {e}. "
            f"Retry {retry_count}/{MAX_RETRIES}"
        )

        if retry_count < MAX_RETRIES:
            republicar_con_retry(ch, body, properties, retry_count)
            print(f"[{WORKER_ID}] Mensaje reintentado ({retry_count + 1}/{MAX_RETRIES})")
        else:
            publicar_en_dlq(ch, body, properties, reason=str(e))
            print(f"[{WORKER_ID}] Mensaje enviado a DLQ tras {MAX_RETRIES} retries")

        ch.basic_ack(delivery_tag=method.delivery_tag)


def iniciar_worker():
    worker_id_short = WORKER_ID[-8:]

    print(f"\n{'=' * 60}")
    print(f"Worker iniciado: {worker_id_short}")
    print(f"{'=' * 60}\n")

    connection = None

    try:
        print(f"[{worker_id_short}] Conectando a PostgreSQL...")
        repo.conectar()
        print(f"[{worker_id_short}] Conectado a PostgreSQL [OK]")

        print(f"[{worker_id_short}] Conectando a RabbitMQ...")
        credentials = pika.PlainCredentials(
            os.getenv("RABBITMQ_USER", "guest"),
            os.getenv("RABBITMQ_PASS", "guest"),
        )

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=os.getenv("RABBITMQ_HOST", "localhost"),
                port=int(os.getenv("RABBITMQ_PORT", 5672)),
                credentials=credentials,
            )
        )

        channel = connection.channel()
        print(f"[{worker_id_short}] Conectado a RabbitMQ [OK]")

        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        channel.queue_declare(queue=DLQ_NAME, durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=QUEUE_NAME, on_message_callback=procesar_compra)

        print(f"[{worker_id_short}] Esperando mensajes en {QUEUE_NAME}...")
        print(f"[{worker_id_short}] DLQ configurada: {DLQ_NAME}")
        print(f"[{worker_id_short}] MAX_RETRIES={MAX_RETRIES}")
        print(f"[{worker_id_short}] Presiona CTRL+C para salir\n")

        channel.start_consuming()

    except KeyboardInterrupt:
        print(f"\n[{worker_id_short}] Interrumpido por usuario")

    except Exception as e:
        print(f"[{worker_id_short}] Error fatal en el worker: {e}")

    finally:
        if connection and connection.is_open:
            connection.close()

        repo.desconectar()
        print(f"[{worker_id_short}] Worker finalizado")


if __name__ == "__main__":
    iniciar_worker()
