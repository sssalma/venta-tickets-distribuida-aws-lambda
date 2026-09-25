from base.postgres_repository import PostgresRepository


def reset_postgres():
    repo = PostgresRepository()
    repo.conectar()
    try:
        with repo.connection.cursor() as cur:
            cur.execute("DELETE FROM idempotency")
            cur.execute("DELETE FROM metrics")
            cur.execute(
                "UPDATE seats SET cliente_id = NULL, request_id = NULL, vendido = FALSE, updated_at = NOW()"
            )
            cur.execute("UPDATE unnumbered_counter SET sold_count = 0 WHERE id = 1")
        repo.connection.commit()
        print("PostgreSQL limpiado correctamente.")
    except Exception as e:
        repo.connection.rollback()
        print(f"Error al resetear PostgreSQL: {e}")
        raise
    finally:
        repo.desconectar()


if __name__ == "__main__":
    reset_postgres()
