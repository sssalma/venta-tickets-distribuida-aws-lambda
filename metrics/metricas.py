import argparse
import os
import csv
from datetime import datetime
import psycopg2
from statistics import mean


def percentile(sorted_list, p):
    """Calcula el percentil p (0-100) de una lista ordenada no vacía."""
    if not sorted_list:
        return None
    k = (len(sorted_list) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_list) - 1)
    if f == c:
        return sorted_list[int(k)]
    d0 = sorted_list[f] * (c - k)
    d1 = sorted_list[c] * (k - f)
    return d0 + d1


def connect_db(host, port, user, password, dbname):
    return psycopg2.connect(host=host, port=port, user=user, password=password, database=dbname)


def summarize_metrics(conn, since=None, until=None):
    cur = conn.cursor()
    q = "SELECT status, latency_ms, created_at, completed_at FROM metrics"
    params = []
    if since and until:
        q += " WHERE completed_at >= %s AND completed_at <= %s"
        params = [since, until]
    elif since:
        q += " WHERE completed_at >= %s"
        params = [since]
    elif until:
        q += " WHERE completed_at <= %s"
        params = [until]

    cur.execute(q, params)
    rows = cur.fetchall()
    statuses = [r[0] for r in rows]
    latencies = [r[1] for r in rows if r[1] is not None]
    created_times = [r[2] for r in rows if r[2] is not None]
    completed_times = [r[3] for r in rows if r[3] is not None]

    total_ops = len(rows)
    success = sum(1 for s in statuses if s == 'SUCCESS')
    fail = sum(1 for s in statuses if s in ('FAIL', 'ERROR'))
    duplicates = sum(1 for s in statuses if s == 'DUPLICATE')

    first_created = min(created_times) if created_times else None
    first_completed = min(completed_times) if completed_times else None
    last_completed = max(completed_times) if completed_times else None
    duration_sec = (last_completed - first_created).total_seconds() if first_created and last_completed else None

    throughput = (total_ops / duration_sec) if duration_sec and duration_sec > 0 else None

    avg_latency = mean(latencies) if latencies else None
    lat_sorted = sorted(latencies)
    p50 = percentile(lat_sorted, 50)
    p95 = percentile(lat_sorted, 95)
    p99 = percentile(lat_sorted, 99)

    return {
        'total_ops': total_ops,
        'success': success,
        'fail': fail,
        'duplicates': duplicates,
        'throughput_ops_per_sec': throughput,
        'avg_latency_ms': avg_latency,
        'p50_ms': p50,
        'p95_ms': p95,
        'p99_ms': p99,
        'first_created_at': first_created,
        'first_completed_at': first_completed,
        'last_completed_at': last_completed,
        'duration_sec': duration_sec
    }


def cli():
    parser = argparse.ArgumentParser(description='Calcula métricas desde la tabla metrics en PostgreSQL')
    parser.add_argument('--host', default=os.getenv('POSTGRES_HOST', 'localhost'))
    parser.add_argument('--port', type=int, default=int(os.getenv('POSTGRES_PORT', 5432)))
    parser.add_argument('--user', default=os.getenv('POSTGRES_USER', 'tickets'))
    parser.add_argument('--password', default=os.getenv('POSTGRES_PASSWORD', 'tickets'))
    parser.add_argument('--db', default=os.getenv('POSTGRES_DB', 'tickets'))
    parser.add_argument('--since', help='Filtrar desde timestamp ISO (ej: 2026-06-11T12:00:00)')
    parser.add_argument('--until', help='Filtrar hasta timestamp ISO')
    parser.add_argument('--csv', help='Ruta para exportar resumen CSV')
    parser.add_argument('--raw-csv', help='Ruta para exportar filas crudas de metrics (opcional)')
    args = parser.parse_args()

    since = datetime.fromisoformat(args.since) if args.since else None
    until = datetime.fromisoformat(args.until) if args.until else None

    conn = connect_db(args.host, args.port, args.user, args.password, args.db)
    summary = summarize_metrics(conn, since=since, until=until)

    # Imprimir resumen
    print("\nMétricas resumen:")
    print(f"  Total ops: {summary['total_ops']}")
    print(f"  Success: {summary['success']}")
    print(f"  Fail: {summary['fail']}")
    print(f"  Duplicates: {summary['duplicates']}")
    if summary['throughput_ops_per_sec'] is not None:
        print(f"  Throughput (ops/sec): {summary['throughput_ops_per_sec']:.3f}")
    else:
        print("  Throughput (ops/sec): N/A")
    if summary['avg_latency_ms'] is not None:
        print(f"  Avg latency (ms): {summary['avg_latency_ms']:.2f}")
        print(f"  p50 (ms): {summary['p50_ms']}")
        print(f"  p95 (ms): {summary['p95_ms']}")
        print(f"  p99 (ms): {summary['p99_ms']}")
    else:
        print("  Latency: N/A")
    print(f"  First created:   {summary['first_created_at']}")
    print(f"  First completed: {summary['first_completed_at']}")
    print(f"  Last completed:  {summary['last_completed_at']}")
    print(f"  Duration (s):    {summary['duration_sec']}")

    # Export CSV resumen
    if args.csv:
        csv_dir = os.path.dirname(args.csv)
        if csv_dir:
            os.makedirs(csv_dir, exist_ok=True)
        with open(args.csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['total_ops','success','fail','duplicates','throughput_ops_per_sec','avg_latency_ms','p50_ms','p95_ms','p99_ms','first_created_at','first_completed_at','last_completed_at','duration_sec'])
            writer.writerow([
                summary['total_ops'], summary['success'], summary['fail'], summary['duplicates'],
                summary['throughput_ops_per_sec'], summary['avg_latency_ms'], summary['p50_ms'], summary['p95_ms'], summary['p99_ms'],
                summary['first_created_at'], summary['first_completed_at'], summary['last_completed_at'], summary['duration_sec']
            ])
        print(f"Resumen exportado a CSV: {args.csv}")

    # Export raw rows
    if args.raw_csv:
        with conn.cursor() as cur:
            q = "SELECT id, operation_type, cliente_id, request_id, seat_id, worker_id, mode, status, motivo, created_at, started_at, completed_at, latency_ms FROM metrics"
            params = []
            if since and until:
                q += " WHERE completed_at >= %s AND completed_at <= %s"
                params = [since, until]
            elif since:
                q += " WHERE completed_at >= %s"
                params = [since]
            elif until:
                q += " WHERE completed_at <= %s"
                params = [until]
            cur.execute(q, params)
            rows = cur.fetchall()
        raw_dir = os.path.dirname(args.raw_csv)
        if raw_dir:
            os.makedirs(raw_dir, exist_ok=True)
        with open(args.raw_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['id','operation_type','cliente_id','request_id','seat_id','worker_id','mode','status','motivo','created_at','started_at','completed_at','latency_ms'])
            for r in rows:
                writer.writerow(r)
        print(f"Filas raw exportadas a CSV: {args.raw_csv}")

    conn.close()


if __name__ == '__main__':
    cli()