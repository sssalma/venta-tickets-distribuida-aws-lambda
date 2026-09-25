"""
Scaler dinámico local para Task 2.

Mide el backlog real en RabbitMQ y aplica la fórmula:

    N = ceil(B / (T * C))

Donde:
- B = backlog actual de la cola
- T = tiempo objetivo para vaciar la cola
- C = capacidad experimental de un worker

El scaler lanza workers como procesos Python y los reduce terminando procesos locales.
"""

import argparse
import csv
import json
import math
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pika


class DynamicScaler:
    def __init__(
        self,
        queue_name="cola_tickets",
        capacity=8.7,
        target_time=10.0,
        min_workers=1,
        max_workers=10,
        interval=2.0,
        rabbit_host="localhost",
        rabbit_port=5672,
        rabbit_user="guest",
        rabbit_pass="guest",
        csv_path=None,
    ):
        self.queue_name = queue_name
        self.capacity = capacity
        self.target_time = target_time
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.interval = interval
        self.rabbit_host = rabbit_host
        self.rabbit_port = rabbit_port
        self.rabbit_user = rabbit_user
        self.rabbit_pass = rabbit_pass
        self.csv_path = csv_path

        self.workers = []
        self.connection = None
        self.channel = None

    def connect_rabbit(self):
        credentials = pika.PlainCredentials(self.rabbit_user, self.rabbit_pass)
        params = pika.ConnectionParameters(
            host=self.rabbit_host,
            port=self.rabbit_port,
            credentials=credentials,
        )
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=True)

    def get_backlog(self):
        q = self.channel.queue_declare(
            queue=self.queue_name,
            durable=True,
            passive=False,
        )
        return q.method.message_count

    def desired_workers(self, backlog):
        if self.capacity <= 0 or self.target_time <= 0:
            desired = self.max_workers
        elif backlog <= 0:
            desired = self.min_workers
        else:
            desired = math.ceil(backlog / (self.target_time * self.capacity))

        return max(self.min_workers, min(self.max_workers, desired))

    def cleanup_finished_workers(self):
        alive = []
        finished = 0

        for proc in self.workers:
            if proc.poll() is None:
                alive.append(proc)
            else:
                finished += 1

                log_path = getattr(proc, "_log_path", None)
                print(
                    f"  Worker terminado PID={proc.pid}, exit={proc.returncode}, log={log_path}",
                    flush=True,
                )

                self.close_worker_log(proc)

        self.workers = alive

        return finished

    def close_worker_log(self, proc):
        log_file = getattr(proc, "_log_file", None)
        if log_file:
            log_file.close()
            proc._log_file = None

    def launch_worker(self):
        project_root = Path(__file__).resolve().parents[1]

        logs_dir = project_root / "results" / "worker_logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        log_path = logs_dir / f"worker_{len(self.workers) + 1}_{int(time.time())}.log"
        log_file = open(log_path, "w", encoding="utf-8")

        env = os.environ.copy()

        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUTF8"] = "1"

        proc = subprocess.Popen(
            [sys.executable, "-m", "rabbitmq.worker"],
            cwd=str(project_root),
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
        )

        proc._log_file = log_file
        proc._log_path = log_path

        self.workers.append(proc)

        print(f"  Worker lanzado PID={proc.pid}, log={log_path}", flush=True)

        return proc

    def send_quit(self, count):
        for _ in range(count):
            payload = {
                "type": "QUIT",
                "created_at": datetime.now(timezone.utc).isoformat(),
            }

            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=json.dumps(payload),
                properties=pika.BasicProperties(delivery_mode=2),
            )

    def log_csv(self, timestamp, backlog, active_workers, desired_workers, action):
        if not self.csv_path:
            return

        path = Path(self.csv_path)

        if path.parent and str(path.parent) != ".":
            path.parent.mkdir(parents=True, exist_ok=True)

        file_exists = path.exists()

        with path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            if not file_exists:
                writer.writerow(
                    [
                        "timestamp",
                        "backlog",
                        "active_workers",
                        "desired_workers",
                        "action",
                    ]
                )

            writer.writerow(
                [
                    timestamp,
                    backlog,
                    active_workers,
                    desired_workers,
                    action,
                ]
            )

    def scale_once(self):
        self.cleanup_finished_workers()

        backlog = self.get_backlog()
        active = len(self.workers)

        desired = self.desired_workers(backlog)

        action = "none"

        if desired > active:
            to_launch = desired - active

            for _ in range(to_launch):
                self.launch_worker()

            action = f"launch {to_launch}"

        elif desired < active:
            to_stop = active - desired
            stopped = 0

            for _ in range(to_stop):
                if not self.workers:
                    break

                proc = self.workers.pop()

                if proc.poll() is None:
                    proc.terminate()
                    stopped += 1

                self.close_worker_log(proc)

            action = f"terminate {stopped}"

        timestamp = datetime.now(timezone.utc).isoformat()

        print(
            f"[{timestamp}] backlog={backlog} "
            f"active={len(self.workers)} "
            f"desired={desired} "
            f"action={action}",
            flush=True,
        )

        self.log_csv(
            timestamp,
            backlog,
            len(self.workers),
            desired,
            action,
        )

    def shutdown(self):
        print("\nScaler apagandose. Terminando workers locales...")

        try:
            self.cleanup_finished_workers()

            for proc in self.workers:
                if proc.poll() is None:
                    proc.terminate()

            for proc in self.workers:
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
                finally:
                    self.close_worker_log(proc)

        finally:
            if self.connection and self.connection.is_open:
                self.connection.close()

            print("Scaler finalizado.")

    def run(self):
        self.connect_rabbit()

        print("Scaler dinámico iniciado")
        print(f"Fórmula: N = ceil(B / (T*C)); C={self.capacity}, T={self.target_time}")
        print(f"Workers: min={self.min_workers}, max={self.max_workers}; cola={self.queue_name}")

        try:
            while True:
                self.scale_once()
                time.sleep(self.interval)

        except KeyboardInterrupt:
            pass

        finally:
            self.shutdown()


def cli():
    parser = argparse.ArgumentParser(
        description="Scaler dinámico local basado en backlog de RabbitMQ"
    )

    parser.add_argument(
        "--capacity",
        type=float,
        default=8.7,
        help="Capacidad experimental de 1 worker en ops/s",
    )

    parser.add_argument(
        "--target-time",
        type=float,
        default=10.0,
        help="Tiempo objetivo para vaciar backlog en segundos",
    )

    parser.add_argument("--min-workers", type=int, default=1)
    parser.add_argument("--max-workers", type=int, default=10)

    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="Intervalo de control en segundos",
    )

    parser.add_argument("--queue", default="cola_tickets")
    parser.add_argument("--rabbit-host", default=os.getenv("RABBITMQ_HOST", "localhost"))
    parser.add_argument("--rabbit-port", type=int, default=int(os.getenv("RABBITMQ_PORT", 5672)))
    parser.add_argument("--rabbit-user", default=os.getenv("RABBITMQ_USER", "guest"))
    parser.add_argument("--rabbit-pass", default=os.getenv("RABBITMQ_PASS", "guest"))
    parser.add_argument("--csv", help="Ruta para guardar log de decisiones CSV")

    args = parser.parse_args()

    scaler = DynamicScaler(
        queue_name=args.queue,
        capacity=args.capacity,
        target_time=args.target_time,
        min_workers=args.min_workers,
        max_workers=args.max_workers,
        interval=args.interval,
        rabbit_host=args.rabbit_host,
        rabbit_port=args.rabbit_port,
        rabbit_user=args.rabbit_user,
        rabbit_pass=args.rabbit_pass,
        csv_path=args.csv,
    )

    scaler.run()


if __name__ == "__main__":
    cli()
