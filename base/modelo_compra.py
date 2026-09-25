from dataclasses import dataclass
from typing import Optional


@dataclass
class modelo_compra:
    ok: bool
    status: str
    motivo: str
    cliente_id: str
    request_id: str
    seat_id: Optional[int] = None

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "status": self.status,
            "motivo": self.motivo,
            "cliente_id": self.cliente_id,
            "request_id": self.request_id,
            "seat_id": self.seat_id,
        }