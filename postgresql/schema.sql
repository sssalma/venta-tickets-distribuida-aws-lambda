-- Esquema base del servicio de tickets

DROP TABLE IF EXISTS metrics CASCADE;
DROP TABLE IF EXISTS idempotency CASCADE;
DROP TABLE IF EXISTS unnumbered_counter CASCADE;
DROP TABLE IF EXISTS seats CASCADE;

-- Asientos numerados
CREATE TABLE seats (
    seat_id INTEGER PRIMARY KEY,
    cliente_id TEXT,
    request_id TEXT UNIQUE,
    vendido BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_seats_vendido ON seats(vendido);
CREATE INDEX idx_seats_cliente_id ON seats(cliente_id);
CREATE INDEX idx_seats_request_id ON seats(request_id);

-- Contador de tickets no numerados
CREATE TABLE unnumbered_counter (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    sold_count INTEGER DEFAULT 0 CHECK (sold_count >= 0 AND sold_count <= 100000),
    max_tickets INTEGER DEFAULT 100000 CHECK (max_tickets = 100000)
);

-- Idempotencia por request_id
CREATE TABLE idempotency (
    request_id TEXT PRIMARY KEY,
    resultado JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_idempotency_request_id ON idempotency(request_id);
CREATE INDEX idx_idempotency_created_at ON idempotency(created_at);

-- Metricas de procesamiento
CREATE TABLE metrics (
    id BIGSERIAL PRIMARY KEY,
    operation_type TEXT NOT NULL,
    cliente_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    seat_id INTEGER,
    worker_id TEXT,
    mode TEXT,
    status TEXT NOT NULL,
    motivo TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    latency_ms FLOAT,
    details JSONB
);

CREATE INDEX idx_metrics_operation_type ON metrics(operation_type);
CREATE INDEX idx_metrics_completed_at ON metrics(completed_at);
CREATE INDEX idx_metrics_cliente_id ON metrics(cliente_id);
CREATE INDEX idx_metrics_status ON metrics(status);
CREATE INDEX idx_metrics_worker_id ON metrics(worker_id);
CREATE INDEX idx_metrics_mode ON metrics(mode);

-- Datos iniciales
INSERT INTO seats (seat_id)
SELECT generate_series(1, 100000);

INSERT INTO unnumbered_counter (id, sold_count, max_tickets)
VALUES (1, 0, 100000);
