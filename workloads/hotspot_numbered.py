import random
import os
import time
import json
import uuid
import argparse
import pika
from datetime import datetime, timezone


def publicar_hotspot(total_req: int = 100, delay: float = 0.0,
                      rabbit_host: str = 'localhost', rabbit_port: int = 5672,
                      rabbit_user: str = None, rabbit_pass: str = None):
    """
    Publica `total_req` mensajes siguiendo una distribución hotspot 80/20
    sobre 100.000 asientos (5% calientes = 5.000 asientos).
    """
    total_seats = 100000
    hot_count = int(total_seats * 0.05)  # 5% = 5.000

    # Conexión a RabbitMQ
    if rabbit_user is None:
        rabbit_user = os.getenv('RABBITMQ_USER', 'guest')
    if rabbit_pass is None:
        rabbit_pass = os.getenv('RABBITMQ_PASS', 'guest')
    credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)

    params = pika.ConnectionParameters(host=rabbit_host, port=rabbit_port, credentials=credentials)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    queue_name = os.getenv("RABBITMQ_QUEUE", "cola_tickets")
    channel.queue_declare(queue=queue_name, durable=True)
    for i in range(total_req):
        cliente_id = f"user{random.randint(1, 10000)}"
        request_id = str(uuid.uuid4())

        if random.random() < 0.80:
            seat_id = random.randint(1, hot_count)
        else:
            seat_id = random.randint(hot_count + 1, total_seats)

        payload = {
            "request_id": request_id,
            "cliente_id": cliente_id,
            "tipo": "numerada",
            "seat_id": seat_id,
            "created_at": datetime.now(timezone.utc).isoformat()
        }

        channel.basic_publish(
            exchange=''
            , routing_key=queue_name
            , body=json.dumps(payload)
            , properties=pika.BasicProperties(delivery_mode=2)
        )

        if delay and delay > 0:
            time.sleep(delay)

    connection.close()
    print(f"Publicado {total_req} mensajes (hotspot) en {queue_name}")


def cli():
    parser = argparse.ArgumentParser(description='Workload hotspot numerada (80/20) (publica a RabbitMQ)')
    parser.add_argument('--total', type=int, default=100, help='Número total de requests')
    parser.add_argument('--delay', type=float, default=0.0, help='Delay (s) entre envíos')
    parser.add_argument('--rabbit-host', type=str, default=os.getenv('RABBITMQ_HOST', 'localhost'))
    parser.add_argument('--rabbit-port', type=int, default=int(os.getenv('RABBITMQ_PORT', 5672)))
    parser.add_argument('--rabbit-user', type=str, default=os.getenv('RABBITMQ_USER', 'guest'))
    parser.add_argument('--rabbit-pass', type=str, default=os.getenv('RABBITMQ_PASS', 'guest'))
    args = parser.parse_args()

    publicar_hotspot(total_req=args.total, delay=args.delay,
                     rabbit_host=args.rabbit_host, rabbit_port=args.rabbit_port,
                     rabbit_user=args.rabbit_user, rabbit_pass=args.rabbit_pass)


if __name__ == '__main__':
    cli()
