CREATE TABLE orders (
    id            UUID          PRIMARY KEY,
    customer_id   VARCHAR(64)   NOT NULL,
    product_id    VARCHAR(64)   NOT NULL,
    quantity      INT           NOT NULL CHECK (quantity > 0),
    amount        NUMERIC(12,2) NOT NULL CHECK (amount > 0),
    status        VARCHAR(20)   NOT NULL,          -- PENDING, APPROVED, REJECTED
    reject_reason VARCHAR(255),
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- Saga state lives in the DB, never only in memory: it must survive restarts.
CREATE TABLE saga_instance (
    id             UUID         PRIMARY KEY,
    order_id       UUID         NOT NULL UNIQUE REFERENCES orders (id),
    current_step   VARCHAR(30)  NOT NULL,          -- RESERVE_STOCK, CHARGE_PAYMENT, RELEASE_STOCK, DONE
    status         VARCHAR(20)  NOT NULL,          -- STARTED, COMPENSATING, COMPLETED, ROLLED_BACK, FAILED
    attempts       INT          NOT NULL DEFAULT 0,
    failure_reason VARCHAR(255),
    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX idx_saga_active ON saga_instance (status, updated_at);
