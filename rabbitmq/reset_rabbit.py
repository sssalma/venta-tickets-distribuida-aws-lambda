import os
import pika


def reset_rabbit():
    queue_name = os.getenv("RABBITMQ_QUEUE", "cola_tickets")
    dlq_name = os.getenv("RABBITMQ_DLQ", "cola_tickets_dlq")

    credentials = pika.PlainCredentials(
        os.getenv("RABBITMQ_USER", "guest"),
        os.getenv("RABBITMQ_PASS", "guest")
    )

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=os.getenv("RABBITMQ_HOST", "localhost"),
            port=int(os.getenv("RABBITMQ_PORT", 5672)),
            credentials=credentials,
        )
    )

    channel = connection.channel()

    for q in [queue_name, dlq_name]:
        try:
            channel.queue_delete(queue=q)
            print(f"Cola '{q}' eliminada correctamente.")
        except Exception as e:
            print(f"No se pudo eliminar la cola '{q}': {e}")

    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_declare(queue=dlq_name, durable=True)

    print(f"Cola principal '{queue_name}' creada correctamente.")
    print(f"DLQ '{dlq_name}' creada correctamente.")

    connection.close()


if __name__ == "__main__":
    reset_rabbit()