CREATE TABLE IF NOT EXISTS market_summaries (
    coin_id        VARCHAR(100) PRIMARY KEY,
    symbol         VARCHAR(20) NOT NULL,
    name           VARCHAR(255) NOT NULL,
    current_price  NUMERIC(20, 8) NOT NULL,
    market_cap     NUMERIC(30, 2),
    volume_24h     NUMERIC(30, 2),
    price_change_24h NUMERIC(10, 4),
    last_updated   TIMESTAMPTZ NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS price_history (
    id          BIGSERIAL PRIMARY KEY,
    coin_id     VARCHAR(100) NOT NULL,
    price       NUMERIC(20, 8) NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_price_history_coin ON price_history(coin_id, recorded_at DESC);
