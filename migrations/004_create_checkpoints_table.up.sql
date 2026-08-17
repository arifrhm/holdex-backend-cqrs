CREATE TABLE IF NOT EXISTS projector_checkpoints (
    projector_name VARCHAR(100) PRIMARY KEY,
    last_event_id  BIGINT NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
