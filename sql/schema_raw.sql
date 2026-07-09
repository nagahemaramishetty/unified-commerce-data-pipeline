-- schema_raw.sql
--
-- Raw landing zone schema. Mirrors the two source systems as-is, with NO cleaning applied.
-- Column types are intentionally permissive (e.g. order_date as TEXT, not DATE) because the
-- two source systems write dates in different formats (legacy_pos: MM/DD/YYYY,
-- web_platform: YYYY-MM-DD) and forcing a DATE type here would fail on load. Standardizing
-- this is a downstream (dbt staging layer) concern, not a raw-load concern.
--
-- No primary key / uniqueness constraints on orders_raw.order_id on purpose: the retry-bug
-- duplicates need to actually land in this table so they can be caught and documented by
-- QA checks downstream, not silently rejected at load time.

CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.customers;
CREATE TABLE raw.customers (
    customer_id     TEXT,
    first_name      TEXT,
    last_name       TEXT,
    email           TEXT,
    region          TEXT,
    signup_date     TEXT,
    _loaded_at      TIMESTAMP DEFAULT NOW()
);

DROP TABLE IF EXISTS raw.products;
CREATE TABLE raw.products (
    product_id      TEXT,
    product_name    TEXT,
    category        TEXT,
    unit_cost       NUMERIC,
    unit_price      NUMERIC,
    _loaded_at      TIMESTAMP DEFAULT NOW()
);

DROP TABLE IF EXISTS raw.orders_raw;
CREATE TABLE raw.orders_raw (
    order_id        TEXT,
    customer_id     TEXT,
    product_id      TEXT,
    quantity        INTEGER,
    order_date      TEXT,       -- deliberately TEXT, mixed formats across sources
    load_timestamp  TIMESTAMP,
    source_system   TEXT,
    _loaded_at      TIMESTAMP DEFAULT NOW()
);

-- Quick sanity indexes for query performance during QA exploration.
-- Not uniqueness constraints, just lookup speed.
CREATE INDEX idx_orders_raw_order_id ON raw.orders_raw (order_id);
CREATE INDEX idx_orders_raw_source_system ON raw.orders_raw (source_system);
