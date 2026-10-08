-- One Postgres container, one database per service.
-- The rule "database per service" is about ownership: no service reads another service's tables.
CREATE DATABASE order_db;
CREATE DATABASE inventory_db;
CREATE DATABASE payment_db;
