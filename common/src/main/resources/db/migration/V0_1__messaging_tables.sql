-- Shared by every service (each service runs it against its OWN database).

-- Transactional outbox: messages are written here in the same DB transaction
-- as the business change, then published to Kafka by OutboxRelay.
CREATE TABLE outbox (
    seq          BIGSERIAL PRIMARY KEY,          -- publish order
    id           UUID         NOT NULL UNIQUE,   -- becomes the Kafka "message-id" header
    topic        VARCHAR(100) NOT NULL,
    message_key  VARCHAR(100) NOT NULL,          -- orderId -> same partition -> ordered per order
    message_type VARCHAR(100) NOT NULL,
    payload      JSONB        NOT NULL,
    traceparent  VARCHAR(55),                    -- W3C trace context captured at write time
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX idx_outbox_unpublished ON outbox (seq) WHERE published_at IS NULL;

-- Idempotent consumer: one row per (consumer, message id) that was processed.
CREATE TABLE processed_messages (
    consumer     VARCHAR(100) NOT NULL,
    message_id   VARCHAR(100) NOT NULL,
    processed_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, message_id)
);
