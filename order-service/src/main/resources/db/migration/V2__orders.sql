CREATE TABLE IF NOT EXISTS orders
(
    id               VARCHAR(64)              NOT NULL,
    customer_id      VARCHAR(64)              NOT NULL,
    total_amount     NUMERIC(12, 2)           NOT NULL,
    status           VARCHAR(20)              NOT NULL,
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL,
    cancelled_at     TIMESTAMP WITH TIME ZONE,
    -- Event id emitted for this order, kept so the duplicate-delivery demo can replay
    -- an identical event through the real outbox path.
    created_event_id VARCHAR(64)              NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders (created_at);
