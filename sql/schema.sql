-- Events that passed validation (PK makes writes idempotent: INSERT ... ON CONFLICT DO NOTHING)
CREATE TABLE IF NOT EXISTS clean_events (
    event_id     UUID PRIMARY KEY,
    shipment_id  TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    event_time   TIMESTAMPTZ NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    device_id    TEXT,
    origin       TEXT,
    destination  TEXT,
    lat          DOUBLE PRECISION,
    lon          DOUBLE PRECISION,
    weight_kg    DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_clean_shipment ON clean_events (shipment_id, event_time);

-- Events that failed validation, kept with a reason (never silently dropped)
CREATE TABLE IF NOT EXISTS quarantine_events (
    id           BIGSERIAL PRIMARY KEY,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    reason_code  TEXT NOT NULL,
    raw_payload  JSONB NOT NULL
);

-- Hidden truth from the simulator, used ONLY to evaluate your detectors
CREATE TABLE IF NOT EXISTS ground_truth (
    event_id    UUID,
    fault       TEXT NOT NULL,
    emitted_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_truth_event ON ground_truth (event_id);

-- Anomaly alerts raised by your detectors
CREATE TABLE IF NOT EXISTS alerts (
    id          BIGSERIAL PRIMARY KEY,
    raised_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    event_id    UUID,
    detector    TEXT NOT NULL,      -- rule / zscore / iforest
    severity    TEXT NOT NULL,
    detail      TEXT
);

-- Rolling data quality score per time window
CREATE TABLE IF NOT EXISTS quality_metrics (
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    total         INT NOT NULL,
    clean         INT NOT NULL,
    quarantined   INT NOT NULL,
    quality_score DOUBLE PRECISION NOT NULL
);
