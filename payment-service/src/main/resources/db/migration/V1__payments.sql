CREATE TABLE payments (
    id                  UUID          PRIMARY KEY,
    order_id            UUID          NOT NULL UNIQUE,   -- one payment per order, enforced by the DB
    customer_id         VARCHAR(64)   NOT NULL,
    amount              NUMERIC(12,2) NOT NULL,
    status              VARCHAR(20)   NOT NULL,          -- CHARGED, FAILED
    bank_transaction_id VARCHAR(100),
    failure_reason      VARCHAR(255),
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT now()
);
