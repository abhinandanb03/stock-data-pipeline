-- WINDOWED AGGREGATIONS TABLE
-- Stores Spark's 5-minute window results per symbol
CREATE TABLE IF NOT EXISTS stock_aggregations (
    id              SERIAL PRIMARY KEY,
    window_start    TIMESTAMP NOT NULL,
    window_end      TIMESTAMP NOT NULL,
    symbol          VARCHAR(10) NOT NULL,
    avg_price       DECIMAL(12, 4),
    min_price       DECIMAL(12, 4),
    max_price       DECIMAL(12, 4),
    tick_count      INTEGER,
    created_at      TIMESTAMP DEFAULT NOW(),

    -- Prevent duplicate windows for the same symbol
    UNIQUE (window_start, symbol)
);

-- gives latest window for each symbol or all windows for a symbol in the last hour
CREATE INDEX idx_agg_symbol_windowgadmin
    ON stock_aggregations(symbol, window_start DESC);

-- ALERTS TABLE Stores every price/volume alert Spark detects
CREATE TABLE IF NOT EXISTS stock_alerts (
    id          SERIAL PRIMARY KEY,
    symbol      VARCHAR(10) NOT NULL,
    price       DECIMAL(12, 4),
    volume      BIGINT,
    alert_type  VARCHAR(50),    -- 'VOLUME_SPIKE', 'PRICE_SURGE', etc.
    timestamp   TIMESTAMP NOT NULL,
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_alerts_symbol_time
    ON stock_alerts(symbol, timestamp DESC);

-- RAW TICKS TABLE (optional, for historical replay) Stores every single tick — useful for backtesting later
CREATE TABLE IF NOT EXISTS stock_ticks (
    id          SERIAL PRIMARY KEY,
    symbol      VARCHAR(10) NOT NULL,
    price       DECIMAL(12, 4),
    volume      BIGINT,
    source      VARCHAR(30),
    event_time  TIMESTAMP NOT NULL,
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_ticks_symbol_time
    ON stock_ticks(symbol, event_time DESC);