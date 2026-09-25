from base.modelo_compra import modelo_compra


class tickets:
    def __init__(self, repository):
        self.repository = repository

    def comprar_numerada(self, cliente_id: str, seat_id: int, request_id: str, 
                        worker_id: str = "worker-unknown", created_at=None) -> modelo_compra:
        """
        Valida y delega la compra numerada al repositorio.
        
        Args:
            cliente_id: ID del cliente
            seat_id: ID del asiento (1-100.000)
            request_id: UUID único para idempotencia
            worker_id: ID del worker procesando (opcional)
        """
        # si no hay cliente_id o request_id es una peticion a la que le falta info
        if not cliente_id or not request_id:
            return modelo_compra(
                ok=False,
                status="FAIL",
                motivo="faltan_datos",
                cliente_id=cliente_id,
                request_id=request_id,
                seat_id=seat_id
            )

        # si no es un entero el asiento da error
        if not isinstance(seat_id, int):
            return modelo_compra(
                ok=False,
                status="FAIL",
                motivo="seat_id_debe_ser_int",
                cliente_id=cliente_id,
                request_id=request_id,
                seat_id=seat_id
            )

        return self.repository.comprar_numerada(cliente_id, seat_id, request_id, worker_id, created_at)
    

    def comprar_no_numerada(self, cliente_id: str, request_id: str,
                           worker_id: str = "worker-unknown", created_at=None) -> modelo_compra:
        """
        Valida y delega la compra no numerada al repositorio.
        
        Args:
            cliente_id: ID del cliente
            request_id: UUID único para idempotencia
            worker_id: ID del worker procesando (opcional)
        """
        # si no hay cliente_id o request_id:  falta info
        if not cliente_id or not request_id:
            return modelo_compra(
                ok=False,
                status="FAIL",
                motivo="faltan_datos",
                cliente_id=cliente_id,
                request_id=request_id
            )

        return self.repository.comprar_no_numerada(cliente_id, request_id, worker_id, created_at)
