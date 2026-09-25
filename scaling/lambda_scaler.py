import argparse
import csv
import json
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pika


class LambdaScaler:
    def __init__(
        self,
        batch_size=None,
        max_invocations=None,
        interval=2.0,
        queue_name=None,
        csv_path="results/lambda_scaling_log.csv",
        rabbit_host=None,
        rabbit_port=None,
        rabbit_user=None,
        rabbit_pass=None,
        function_name=None,
        aws_region=None,
    ):
        self.batch_size = batch_size or int(os.getenv("BATCH_SIZE", 10))
        self.max_invocations = max_invocations or int(os.getenv("MAX_INVOCATIONS", 20))
        self.interval = interval
        self.queue_name = queue_name or os.getenv("RABBITMQ_QUEUE", "cola_tickets")
        self.csv_path = csv_path
        self.rabbit_host = rabbit_host or os.getenv("RABBITMQ_HOST", "localhost")
        self.rabbit_port = rabbit_port or int(os.getenv("RABBITMQ_PORT", 5672))
        self.rabbit_user = rabbit_user or os.getenv("RABBITMQ_USER", "guest")
        self.rabbit_pass = rabbit_pass or os.getenv("RABBITMQ_PASS", "guest")
        self.function_name = function_name or os.getenv("AWS_LAMBDA_FUNCTION_NAME")
        self.aws_region = aws_region or os.getenv("AWS_REGION", "eu-west-1")

        if self.batch_size <= 0:
            raise ValueError("batch_size debe ser mayor que 0")
        if self.max_invocations <= 0:
            raise ValueError("max_invocations debe ser mayor que 0")
        if not self.function_name:
            raise ValueError("AWS_LAMBDA_FUNCTION_NAME es obligatorio")

        self.connection = None
        self.channel = None
        self.lambda_client = boto3.client("lambda", region_name=self.aws_region)

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
        queue = self.channel.queue_declare(
            queue=self.queue_name,
            durable=True,
            passive=False,
        )
        return queue.method.message_count

    def desired_invocations(self, backlog):
        if backlog <= 0:
            return 0
        desired = math.ceil(backlog / self.batch_size)
        return min(self.max_invocations, desired)

    def invoke_lambda(self, count, backlog):
        invoked = 0

        for index in range(count):
            payload = {
                "source": "lambda_scaler",
                "queue": self.queue_name,
                "backlog": backlog,
                "batch_size": self.batch_size,
                "invocation_index": index + 1,
                "requested_invocations": count,
            }

            self.lambda_client.invoke(
                FunctionName=self.function_name,
                InvocationType="Event",
                Payload=json.dumps(payload).encode("utf-8"),
            )
            invoked += 1

        return invoked

    def log_csv(self, timestamp, backlog, desired_invocations, invoked, action):
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
                        "desired_invocations",
                        "invoked",
                        "action",
                    ]
                )
            writer.writerow(
                [
                    timestamp,
                    backlog,
                    desired_invocations,
                    invoked,
                    action,
                ]
            )

    def scale_once(self):
        backlog = self.get_backlog()
        desired = self.desired_invocations(backlog)
        invoked = 0
        action = "none"

        if desired > 0:
            invoked = self.invoke_lambda(desired, backlog)
            action = f"invoke {invoked}"

        timestamp = datetime.now(timezone.utc).isoformat()
        print(
            f"[{timestamp}] backlog={backlog} "
            f"batch_size={self.batch_size} "
            f"desired_invocations={desired} "
            f"invoked={invoked} "
            f"action={action}",
            flush=True,
        )

        self.log_csv(timestamp, backlog, desired, invoked, action)

    def shutdown(self):
        if self.connection and self.connection.is_open:
            self.connection.close()

    def run(self):
        self.connect_rabbit()
        print("Lambda scaler iniciado")
        print(f"Function={self.function_name}; region={self.aws_region}")
        print(
            f"Formula: N = ceil(B / batch_size); "
            f"batch_size={self.batch_size}; max_invocations={self.max_invocations}"
        )
        print(f"Cola={self.queue_name}")

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
        description="Scaler AWS Lambda basado en backlog de RabbitMQ"
    )
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("BATCH_SIZE", 10)))
    parser.add_argument(
        "--max-invocations",
        type=int,
        default=int(os.getenv("MAX_INVOCATIONS", 20)),
    )
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument(
        "--queue",
        default=os.getenv("RABBITMQ_QUEUE", "cola_tickets"),
    )
    parser.add_argument("--csv", default="results/lambda_scaling_log.csv")
    parser.add_argument("--rabbit-host", default=os.getenv("RABBITMQ_HOST", "localhost"))
    parser.add_argument(
        "--rabbit-port",
        type=int,
        default=int(os.getenv("RABBITMQ_PORT", 5672)),
    )
    parser.add_argument("--rabbit-user", default=os.getenv("RABBITMQ_USER", "guest"))
    parser.add_argument("--rabbit-pass", default=os.getenv("RABBITMQ_PASS", "guest"))
    parser.add_argument(
        "--function-name",
        default=os.getenv("AWS_LAMBDA_FUNCTION_NAME"),
    )
    parser.add_argument("--aws-region", default=os.getenv("AWS_REGION", "eu-west-1"))

    args = parser.parse_args()

    scaler = LambdaScaler(
        batch_size=args.batch_size,
        max_invocations=args.max_invocations,
        interval=args.interval,
        queue_name=args.queue,
        csv_path=args.csv,
        rabbit_host=args.rabbit_host,
        rabbit_port=args.rabbit_port,
        rabbit_user=args.rabbit_user,
        rabbit_pass=args.rabbit_pass,
        function_name=args.function_name,
        aws_region=args.aws_region,
    )
    scaler.run()


if __name__ == "__main__":
    cli()
