CREATE TABLE products (
    id        VARCHAR(64)  PRIMARY KEY,
    name      VARCHAR(100) NOT NULL,
    available INT          NOT NULL CHECK (available >= 0)
);

-- Reservations are rows, not just a decremented counter:
-- that makes release idempotent (RESERVED -> RELEASED happens at most once).
CREATE TABLE reservations (
    id         UUID        PRIMARY KEY,
    order_id   UUID        NOT NULL UNIQUE,
    product_id VARCHAR(64) NOT NULL REFERENCES products (id),
    quantity   INT         NOT NULL,
    status     VARCHAR(20) NOT NULL,   -- RESERVED, RELEASED
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO products (id, name, available) VALUES
    ('p-1', 'Mechanical keyboard', 100),
    ('p-2', 'Wireless mouse', 0),       -- always out of stock: tests the StockReserveFailed path
    ('p-3', 'USB-C hub', 5);
