import argparse
import csv
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pika


TOTAL_SEATS = 100000
HOTSPOT_SEATS = int(TOTAL_SEATS * 0.05)  # 5% de asientos = 5000


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def connect_rabbit():
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

    return connection


def choose_seat(mode):
    """
    uniform:
        asientos aleatorios entre 1 y 100000

    hotspot:
        80% de las peticiones van al 5% de asientos calientes
        20% van al resto
    """
    if mode == "hotspot":
        if random.random() < 0.8:
            return random.randint(1, HOTSPOT_SEATS)
        return random.randint(HOTSPOT_SEATS + 1, TOTAL_SEATS)

    return random.randint(1, TOTAL_SEATS)


def build_message(mode):
    seat_id = choose_seat(mode)

    return {
        "cliente_id": f"cliente-{uuid.uuid4()}",
        "request_id": str(uuid.uuid4()),
        "seat_id": seat_id,
        "created_at": utc_now(),
    }


def publish_message(channel, queue_name, message):
    channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
        ),
    )


def log_phase(csv_path, row):
    if not csv_path:
        return

    path = Path(csv_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    file_exists = path.exists()

    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        if not file_exists:
            writer.writerow(
                [
                    "timestamp",
                    "phase",
                    "second",
                    "target_rate",
                    "published_this_second",
                    "total_published",
                ]
            )

        writer.writerow(row)


def run_phase(
    channel,
    queue_name,
    phase_name,
    duration,
    start_rate,
    end_rate,
    mode,
    csv_path,
    total_published,
):
    """
    Ejecuta una fase del workload.

    Si start_rate == end_rate:
        fase constante

    Si start_rate != end_rate:
        fase con rampa lineal
    """
    print(
        f"\nFASE: {phase_name} | duration={duration}s | "
        f"rate={start_rate}->{end_rate} req/s"
    )

    for second in range(duration):
        if duration <= 1:
            current_rate = end_rate
        else:
            progress = second / (duration - 1)
            current_rate = start_rate + (end_rate - start_rate) * progress

        messages_this_second = max(0, int(round(current_rate)))

        second_start = time.time()

        for _ in range(messages_this_second):
            message = build_message(mode)
            publish_message(channel, queue_name, message)
            total_published += 1

            # Espaciado dentro del segundo para no publicar todos exactamente a la vez.
            if messages_this_second > 0:
                time.sleep(1.0 / messages_this_second)

        elapsed = time.time() - second_start

        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)

        print(
            f"[{utc_now()}] phase={phase_name} "
            f"second={second + 1}/{duration} "
            f"rate={current_rate:.2f} "
            f"published={messages_this_second} "
            f"total={total_published}",
            flush=True,
        )

        log_phase(
            csv_path,
            [
                utc_now(),
                phase_name,
                second + 1,
                round(current_rate, 2),
                messages_this_second,
                total_published,
            ],
        )

    return total_published


def run_elastic_workload(args):
    queue_name = args.queue

    connection = connect_rabbit()
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)

    total_published = 0

    print("=" * 70)
    print("Elastic Workload Z(t)")
    print("=" * 70)
    print(f"Queue: {queue_name}")
    print(f"Mode: {args.mode}")
    print(f"CSV: {args.csv}")
    print("=" * 70)

    try:
        phases = [
            # phase_name, duration, start_rate, end_rate
            ("low_load", args.low_duration, args.low_rate, args.low_rate),
            ("ramp_up", args.ramp_duration, args.low_rate, args.high_rate),
            ("spike", args.spike_duration, args.spike_rate, args.spike_rate),
            ("sustained_high", args.high_duration, args.high_rate, args.high_rate),
            ("cool_down", args.cooldown_duration, args.high_rate, args.low_rate),
        ]

        for phase_name, duration, start_rate, end_rate in phases:
            total_published = run_phase(
                channel=channel,
                queue_name=queue_name,
                phase_name=phase_name,
                duration=duration,
                start_rate=start_rate,
                end_rate=end_rate,
                mode=args.mode,
                csv_path=args.csv,
                total_published=total_published,
            )

    finally:
        connection.close()

    print("\n" + "=" * 70)
    print(f"Elastic workload terminado. Total publicado: {total_published}")
    print("=" * 70)


def cli():
    parser = argparse.ArgumentParser(
        description="Elastic workload Z(t) para demostrar escalado dinámico"
    )

    parser.add_argument(
        "--mode",
        choices=["uniform", "hotspot"],
        default="uniform",
        help="Distribución de asientos",
    )

    parser.add_argument(
        "--queue",
        default=os.getenv("RABBITMQ_QUEUE", "cola_tickets"),
        help="Nombre de la cola RabbitMQ",
    )

    parser.add_argument("--low-duration", type=int, default=10)
    parser.add_argument("--ramp-duration", type=int, default=10)
    parser.add_argument("--spike-duration", type=int, default=5)
    parser.add_argument("--high-duration", type=int, default=15)
    parser.add_argument("--cooldown-duration", type=int, default=10)

    parser.add_argument("--low-rate", type=float, default=5)
    parser.add_argument("--high-rate", type=float, default=40)
    parser.add_argument("--spike-rate", type=float, default=120)

    parser.add_argument(
        "--csv",
        default="results/elastic_workload_log.csv",
        help="CSV para registrar la carga Z(t)",
    )

    args = parser.parse_args()
    run_elastic_workload(args)


if __name__ == "__main__":
    cli()