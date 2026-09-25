import psycopg2
from psycopg2 import sql, extras
from datetime import datetime, timezone
import json
import time
import os

from base.modelo_compra import modelo_compra


class PostgresRepository:
    """Repositorio PostgreSQL de compras."""

    def __init__(self, host='localhost', port=5432, user='tickets', password='tickets', database='tickets'):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
    
    def conectar(self):
        """Establece conexión a PostgreSQL"""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                connect_timeout=5
            )
            # Usar el aislamiento por defecto de PostgreSQL (READ COMMITTED)
            # La consistencia se garantiza con transacciones + UPDATE atómicos.
        except Exception as e:
            print(f"Error conectando a PostgreSQL: {e}")
            raise
    
    def desconectar(self):
        """Cierra conexión a PostgreSQL"""
        if self.connection:
            self.connection.close()
    
    def _guardar_metrica(self, operation_type: str, cliente_id: str, request_id: str,
                         seat_id, worker_id: str, mode: str, status: str, motivo: str,
                         created_at: datetime, started_at: datetime, completed_at: datetime, latency_ms: float):
        """Guarda una métrica de compra."""
        try:
            with self.connection.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO metrics 
                    (operation_type, cliente_id, request_id, seat_id, worker_id, mode, status, motivo,
                     created_at, started_at, completed_at, latency_ms)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (operation_type, cliente_id, request_id, seat_id, worker_id, mode, status, motivo,
                     created_at, started_at, completed_at, latency_ms)
                )
                self.connection.commit()
        except Exception as e:
            print(f"[MÉTRICA] Error guardando métrica: {e}")
            self.connection.rollback()
            # No relanzar: las métricas son secundarias
    
    def comprar_numerada(self, cliente_id: str, seat_id: int, request_id: str, 
                        worker_id: str = "worker-unknown", created_at=None) -> modelo_compra:
        """Compra numerada con idempotencia."""
        # Normalizar created_at.
        if created_at:
            if isinstance(created_at, str):
                # Remove 'Z' and replace with '+00:00' if needed
                s = created_at
                if s.endswith('Z'):
                    s = s[:-1] + '+00:00'
                try:
                    dt_with_tz = datetime.fromisoformat(s)
                    # Convert to UTC and make naive
                    created_at_dt = dt_with_tz.astimezone(timezone.utc).replace(tzinfo=None)
                except Exception:
                    # fallback to now if parsing fails
                    created_at_dt = datetime.now(timezone.utc).replace(tzinfo=None)
            elif isinstance(created_at, datetime):
                # If datetime has timezone, convert to UTC naive
                if created_at.tzinfo:
                    created_at_dt = created_at.astimezone(timezone.utc).replace(tzinfo=None)
                else:
                    created_at_dt = created_at
            else:
                created_at_dt = datetime.now(timezone.utc).replace(tzinfo=None)
        else:
            created_at_dt = datetime.now(timezone.utc).replace(tzinfo=None)

        # started_at is when worker/repository starts processing (UTC, naive)
        started_at = datetime.now(timezone.utc).replace(tzinfo=None)
        latency_ms = 0
        resultado_final = None
        status_metrica = "SUCCESS"
        motivo_metrica = ""
        
        try:
            # INICIO TRANSACCIÓN + LOCK ADVISORY
            with self.connection.cursor(cursor_factory=extras.RealDictCursor) as cur:
                # Lock advisory: serializa requests con el mismo request_id
                cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s));", (request_id,))
                
                # Verificar idempotencia DENTRO de la transacción
                cur.execute(
                    "SELECT resultado FROM idempotency WHERE request_id = %s",
                    (request_id,)
                )
                row = cur.fetchone()
                
                if row:
                    # Duplicado: retornar resultado previo
                    self.connection.commit()
                    resultado_guardado = row['resultado']

                    completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
                    latency_ms = (completed_at - created_at_dt).total_seconds() * 1000
                    
                    self._guardar_metrica(
                        "COMPRA_NUMERADA", cliente_id, request_id, seat_id, worker_id, "NUMERADA",
                        "DUPLICATE", "Request ya procesado (idempotencia)",
                        created_at_dt, started_at, completed_at, latency_ms
                    )
                    
                    return modelo_compra(
                        ok=resultado_guardado['ok'],
                        status=resultado_guardado['status'],
                        motivo=resultado_guardado['motivo'],
                        cliente_id=cliente_id,
                        request_id=request_id,
                        seat_id=resultado_guardado.get('seat_id')
                    )
                
                # NUEVO REQUEST: intentar comprar
                cur.execute(
                    """
                    UPDATE seats 
                    SET cliente_id = %s, request_id = %s, vendido = TRUE, updated_at = NOW()
                    WHERE seat_id = %s AND vendido = FALSE
                    """,
                    (cliente_id, request_id, seat_id)
                )
                filas_actualizadas = cur.rowcount
                
                # Construir resultado de la compra
                if filas_actualizadas > 0:
                    # ÉXITO: asiento comprado
                    resultado = {
                        'ok': True,
                        'status': 'COMPRADO',
                        'motivo': 'Asiento comprado exitosamente',
                        'seat_id': seat_id
                    }
                    status_metrica = "SUCCESS"
                    motivo_metrica = "Asiento comprado"
                else:
                    # FALLO: asiento no disponible
                    resultado = {
                        'ok': False,
                        'status': 'FAIL',
                        'motivo': 'Asiento no disponible o ya vendido',
                        'seat_id': None
                    }
                    status_metrica = "FAIL"
                    motivo_metrica = "Asiento no disponible"
                
                # GUARDAR RESULTADO EN IDEMPOTENCY (misma transacción)
                cur.execute(
                    "INSERT INTO idempotency (request_id, resultado) VALUES (%s, %s)",
                    (request_id, json.dumps(resultado))
                )
                
                # COMMIT: TODO ATÓMICO
                self.connection.commit()
                resultado_final = resultado
        
        except psycopg2.errors.UniqueViolation:
            # Otro worker insertó en idempotency primero: re-leer resultado
            self.connection.rollback()
            try:
                with self.connection.cursor(cursor_factory=extras.RealDictCursor) as cur:
                    cur.execute(
                        "SELECT resultado FROM idempotency WHERE request_id = %s",
                        (request_id,)
                    )
                    row = cur.fetchone()
                    if row:
                        resultado_final = row['resultado']
                        status_metrica = "DUPLICATE"
                        motivo_metrica = "Otro worker ya procesó este request"
            except Exception as e:
                print(f"[ERROR] No se pudo leer resultado idempotente: {e}")
                raise
        
        except Exception as e:
            self.connection.rollback()
            print(f"[ERROR] comprar_numerada: {e}")
            completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
            latency_ms = (completed_at - created_at_dt).total_seconds() * 1000
            self._guardar_metrica(
                "COMPRA_NUMERADA", cliente_id, request_id, seat_id, worker_id, "NUMERADA",
                "ERROR", f"Error de BD: {str(e)}",
                created_at_dt, started_at, completed_at, latency_ms
            )
            raise
        
        # DELAY DE 100MS (después de commit, medido)
        time.sleep(0.1)
        completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        latency_ms = (completed_at - created_at_dt).total_seconds() * 1000

        # GUARDAR MÉTRICA (aparte)
        self._guardar_metrica(
            "COMPRA_NUMERADA", cliente_id, request_id, seat_id, worker_id, "NUMERADA",
            status_metrica, motivo_metrica,
            created_at_dt, started_at, completed_at, latency_ms
        )
        
        # Retornar resultado
        return modelo_compra(
            ok=resultado_final['ok'],
            status=resultado_final['status'],
            motivo=resultado_final['motivo'],
            cliente_id=cliente_id,
            request_id=request_id,
            seat_id=resultado_final.get('seat_id')
        )
    
    def comprar_no_numerada(self, cliente_id: str, request_id: str,
                           worker_id: str = "worker-unknown", created_at=None) -> modelo_compra:
        """Compra no numerada con idempotencia."""
        # Normalizar created_at.
        if created_at:
            if isinstance(created_at, str):
                # Remove 'Z' and replace with '+00:00' if needed
                s = created_at
                if s.endswith('Z'):
                    s = s[:-1] + '+00:00'
                try:
                    dt_with_tz = datetime.fromisoformat(s)
                    # Convert to UTC and make naive
                    created_at_dt = dt_with_tz.astimezone(timezone.utc).replace(tzinfo=None)
                except Exception:
                    # fallback to now if parsing fails
                    created_at_dt = datetime.now(timezone.utc).replace(tzinfo=None)
            elif isinstance(created_at, datetime):
                # If datetime has timezone, convert to UTC naive
                if created_at.tzinfo:
                    created_at_dt = created_at.astimezone(timezone.utc).replace(tzinfo=None)
                else:
                    created_at_dt = created_at
            else:
                created_at_dt = datetime.now(timezone.utc).replace(tzinfo=None)
        else:
            created_at_dt = datetime.now(timezone.utc).replace(tzinfo=None)

        # started_at is when worker/repository starts processing (UTC, naive)
        started_at = datetime.now(timezone.utc).replace(tzinfo=None)
        latency_ms = 0
        resultado_final = None
        ticket_number = None
        status_metrica = "SUCCESS"
        motivo_metrica = ""
        
        try:
            # INICIO TRANSACCIÓN + LOCK ADVISORY
            with self.connection.cursor(cursor_factory=extras.RealDictCursor) as cur:
                # Lock advisory: serializa requests con el mismo request_id
                cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s));", (request_id,))
                
                # Verificar idempotencia DENTRO de la transacción
                cur.execute(
                    "SELECT resultado FROM idempotency WHERE request_id = %s",
                    (request_id,)
                )
                row = cur.fetchone()
                
                if row:
                    # Duplicado: retornar resultado previo
                    self.connection.commit()
                    resultado_guardado = row['resultado']

                    completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
                    latency_ms = (completed_at - created_at_dt).total_seconds() * 1000

                    self._guardar_metrica(
                        "COMPRA_NO_NUMERADA", cliente_id, request_id, None, worker_id, "NO_NUMERADA",
                        "DUPLICATE", "Request ya procesado (idempotencia)",
                        created_at_dt, started_at, completed_at, latency_ms
                    )

                    return modelo_compra(
                        ok=resultado_guardado['ok'],
                        status=resultado_guardado['status'],
                        motivo=resultado_guardado['motivo'],
                        cliente_id=cliente_id,
                        request_id=request_id,
                        seat_id=resultado_guardado.get('ticket_number')
                    )
                
                # NUEVO REQUEST: intentar comprar
                cur.execute(
                    """
                    UPDATE unnumbered_counter 
                    SET sold_count = sold_count + 1
                    WHERE id = 1 AND sold_count < max_tickets
                    RETURNING sold_count
                    """,
                    ()
                )
                row = cur.fetchone()
                
                # Construir resultado de la compra
                if row:
                    # ÉXITO: ticket asignado
                    ticket_number = row['sold_count']
                    resultado = {
                        'ok': True,
                        'status': 'COMPRADO',
                        'motivo': f'Ticket no numerado {ticket_number} comprado',
                        'ticket_number': ticket_number
                    }
                    status_metrica = "SUCCESS"
                    motivo_metrica = f"Ticket {ticket_number} comprado"
                else:
                    # FALLO: cuota agotada
                    resultado = {
                        'ok': False,
                        'status': 'FAIL',
                        'motivo': 'Tickets no numerados agotados (100.000 máximo)',
                        'ticket_number': None
                    }
                    status_metrica = "FAIL"
                    motivo_metrica = "Cuota agotada"
                
                # GUARDAR RESULTADO EN IDEMPOTENCY (misma transacción)
                cur.execute(
                    "INSERT INTO idempotency (request_id, resultado) VALUES (%s, %s)",
                    (request_id, json.dumps(resultado))
                )
                
                # COMMIT: TODO ATÓMICO
                self.connection.commit()
                resultado_final = resultado
        
        except psycopg2.errors.UniqueViolation:
            # Otro worker insertó en idempotency primero: re-leer resultado
            self.connection.rollback()
            try:
                with self.connection.cursor(cursor_factory=extras.RealDictCursor) as cur:
                    cur.execute(
                        "SELECT resultado FROM idempotency WHERE request_id = %s",
                        (request_id,)
                    )
                    row = cur.fetchone()
                    if row:
                        resultado_final = row['resultado']
                        status_metrica = "DUPLICATE"
                        motivo_metrica = "Otro worker ya procesó este request"
            except Exception as e:
                print(f"[ERROR] No se pudo leer resultado idempotente: {e}")
                raise
        
        except Exception as e:
            self.connection.rollback()
            print(f"[ERROR] comprar_no_numerada: {e}")
            completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
            latency_ms = (completed_at - created_at_dt).total_seconds() * 1000
            self._guardar_metrica(
                "COMPRA_NO_NUMERADA", cliente_id, request_id, None, worker_id, "NO_NUMERADA",
                "ERROR", f"Error de BD: {str(e)}",
                created_at_dt, started_at, completed_at, latency_ms
            )
            raise
        
        # DELAY DE 100MS (después de commit, medido)
        time.sleep(0.1)
        completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        latency_ms = (completed_at - created_at_dt).total_seconds() * 1000

        # GUARDAR MÉTRICA (aparte)
        self._guardar_metrica(
            "COMPRA_NO_NUMERADA", cliente_id, request_id, None, worker_id, "NO_NUMERADA",
            status_metrica, motivo_metrica,
            created_at_dt, started_at, completed_at, latency_ms
        )
        
        # Retornar resultado
        return modelo_compra(
            ok=resultado_final['ok'],
            status=resultado_final['status'],
            motivo=resultado_final['motivo'],
            cliente_id=cliente_id,
            request_id=request_id,
            seat_id=resultado_final.get('ticket_number')
        )
