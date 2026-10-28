CREATE TABLE IF NOT EXISTS raw_trades(
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    trade_id BIGINT NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    is_buyer_maker BOOLEAN,
    UNIQUE(symbol, trade_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_trades_time
    ON raw_trades (symbol, event_time DESC);

CREATE TABLE IF NOT EXISTS trade_metrics (
    symbol        TEXT             NOT NULL,
    window_start  TIMESTAMPTZ      NOT NULL,
    window_end    TIMESTAMPTZ      NOT NULL,
    avg_price     DOUBLE PRECISION,
    vwap          DOUBLE PRECISION,
    min_price     DOUBLE PRECISION,
    max_price     DOUBLE PRECISION,
    volatility    DOUBLE PRECISION,
    trade_count   BIGINT,
    total_volume  DOUBLE PRECISION,
    PRIMARY KEY (symbol, window_start, window_end)
);