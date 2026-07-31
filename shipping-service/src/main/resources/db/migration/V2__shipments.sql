CREATE TABLE IF NOT EXISTS shipments
(
    id          VARCHAR(64)              NOT NULL,
    order_id    VARCHAR(64)              NOT NULL,
    customer_id VARCHAR(64)              NOT NULL,
    order_total NUMERIC(12, 2)           NOT NULL,
    status      VARCHAR(20)              NOT NULL,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_shipments_order_id ON shipments (order_id);
