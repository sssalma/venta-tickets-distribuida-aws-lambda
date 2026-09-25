# Venta de tickets con autoescalado elástico sobre AWS Lambda

Sistema distribuido de venta de entradas que **ajusta dinámicamente su número de workers
al backlog de la cola**, con dos backends de cómputo intercambiables: procesos locales o
funciones **AWS Lambda**. Persistencia transaccional en PostgreSQL.

> Task 2 de *Sistemes Distribuïts* — Grau en Enginyeria Informàtica, URV.
> Trabajo en pareja: Alejandro Fernández y Salma Jadiani.
> Continuación de [la Task 1](../middleware-venta-tickets-pyro-rabbitmq), que comparaba
> modelos de comunicación; aquí el foco es la **elasticidad**.

## Arquitectura

```text
workloads ──► RabbitMQ ──► workers ──► PostgreSQL ──► métricas
                  ▲            ▲
                  └── scaler ──┘   (observa el backlog y ajusta los workers)
```

En AWS, los workers dejan de ser procesos y pasan a ser invocaciones Lambda:

```text
workloads ──► RabbitMQ (EC2) ──► lambda_scaler ──► Lambda workers ──► PostgreSQL (EC2)
```

## El autoescalado

Es el núcleo de la práctica. El scaler (`scaling/scaler.py`) mide el backlog real de
RabbitMQ cada pocos segundos y calcula cuántos workers hacen falta:

```text
N = ceil(B / (T · C))

B = mensajes pendientes en la cola
T = tiempo objetivo para vaciarla
C = capacidad medida de un worker (msg/s)
```

`C` no es un número inventado: se obtuvo **midiendo experimentalmente** el throughput de
un worker aislado (≈8,7 msg/s). El resultado se acota entre `min_workers` y
`max_workers` para no oscilar ni desbordar la infraestructura, y cada decisión de escalado
se registra en CSV para poder reconstruir después la curva de escalado frente a la carga.

`scaling/lambda_scaler.py` aplica la misma fórmula pero traduce `N` a invocaciones
concurrentes de Lambda vía **boto3**, con `BATCH_SIZE` mensajes por invocación.

## El worker Lambda

`lambda_worker.py` está escrito para el modelo de ejecución de Lambda, no como un
demonio: **no tiene bucle infinito**. Cada invocación consume hasta `BATCH_SIZE`
mensajes, los procesa, hace **ACK / reintento / DLQ** según el resultado y termina.
Así cada invocación es corta, acotada y facturable por lo que realmente usa.

## Cargas de prueba

| Workload | Qué estresa |
|---|---|
| `workloads/bm_numbered_uniforme.py` | Carga uniforme: rendimiento en el caso favorable |
| `workloads/hotspot_numbered.py` | Contención: muchos clientes sobre los mismos asientos |
| `workloads/elastic_workload.py` | Carga variable en el tiempo: dispara el autoescalado |

## Puesta en marcha local

```bash
docker compose up -d
```

RabbitMQ Management queda en `http://localhost:15672` (`guest`/`guest`).

Resetear el estado entre pruebas:

```bash
python -m rabbitmq.reset_rabbit
python -m base.reset_postgres
```

Enviar carga:

```bash
python -m workloads.bm_numbered_uniforme --total 300 --delay 0.001
python -m workloads.hotspot_numbered --total 300
python -m workloads.elastic_workload
```

Lanzar el scaler dinámico:

```bash
python -m scaling.scaler --target-time 10 --min-workers 1 --max-workers 10 --interval 2
```

## Despliegue en AWS

Los scripts de aprovisionamiento de las instancias están en `scripts_setup/`
(`setup_ec2_rabbitmq.sh`, `setup_ec2_postgres.sh`, `setup_ec2_client.sh`), y el
empaquetado de la función en `scripts/package_lambda.ps1` / `package_lambda_linux.sh`.

```powershell
$env:AWS_LAMBDA_FUNCTION_NAME="sd-task2-worker"
$env:AWS_REGION="eu-west-1"
python -m scaling.lambda_scaler --batch-size 10 --max-invocations 20 --interval 2
```

> RabbitMQ y PostgreSQL deben ser accesibles desde Lambda. Si están en una EC2 privada,
> hay que colocar la Lambda en la misma VPC y abrir los security groups.

El detalle completo está en [`AWS_DEPLOYMENT.md`](AWS_DEPLOYMENT.md) y
[`AWS_LAMBDA_NOTES.md`](AWS_LAMBDA_NOTES.md).

## Métricas

```bash
python -m metrics.metricas
python -m metrics.metricas --csv results/metrics_summary.csv --raw-csv results/metrics_raw.csv
```

## Configuración

Copia `.env.example` (local) o `.env.aws.example` (AWS) a `.env` y rellena los valores.
Los `.env` reales están excluidos del repositorio.

## Tests

```bash
python -m pytest tests/
```

## Stack

Python · RabbitMQ (pika) · PostgreSQL · Docker Compose · AWS Lambda · AWS EC2 · boto3 · pytest
