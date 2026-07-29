CREATE TABLE IF NOT EXISTS processed_orders (
    event_id UUID PRIMARY KEY,
    order_id UUID NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    amount NUMERIC(18, 2) NOT NULL CHECK (amount > 0),
    currency CHAR(3) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    schema_version VARCHAR(32) NOT NULL
);

CREATE INDEX IF NOT EXISTS processed_orders_order_id_idx
    ON processed_orders (order_id);

CREATE INDEX IF NOT EXISTS processed_orders_created_at_idx
    ON processed_orders (created_at);
