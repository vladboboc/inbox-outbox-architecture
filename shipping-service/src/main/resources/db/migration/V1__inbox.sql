-- Deduplication ledger for the idempotent consumer.
--
-- The primary key is (event_id, consumer), not event_id alone: several consumers may fan out from
-- the same topic, and each needs its own record of what it has handled. A shared event_id key would
-- let whichever consumer processed an event first silently suppress it for all the others.
--
-- The composite primary key is also the concurrency control. INSERT ... ON CONFLICT DO NOTHING
-- resolves races inside the database, so two simultaneous deliveries of one event cannot both be
-- accepted -- which a SELECT-then-INSERT could not guarantee.
CREATE TABLE IF NOT EXISTS inbox_message
(
    event_id      VARCHAR(255)             NOT NULL,
    consumer      VARCHAR(255)             NOT NULL,
    topic         VARCHAR(255)             NOT NULL,
    partition_no  INTEGER                  NOT NULL,
    record_offset BIGINT                   NOT NULL,
    received_at   TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (event_id, consumer)
);

-- Supports the retention purge, which scans by age.
CREATE INDEX IF NOT EXISTS idx_inbox_message_received_at ON inbox_message (received_at);
