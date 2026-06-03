-- Run these in the AWS Athena console or via scripts/setup_aws.py
-- Replace YOUR_BUCKET with your actual S3 bucket name.

CREATE DATABASE IF NOT EXISTS ecommerce;

-- ── orders_placed ─────────────────────────────────────────────────────────

CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce.orders_placed (
    event_type  STRING,
    order_id    STRING,
    customer_id STRING,
    product_id  STRING,
    category    STRING,
    quantity    INT,
    price       DOUBLE,
    timestamp   STRING
)
PARTITIONED BY (year STRING, month STRING, day STRING, hour STRING)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://YOUR_BUCKET/orders.placed/'
TBLPROPERTIES ('has_encrypted_data'='false');

MSCK REPAIR TABLE ecommerce.orders_placed;

-- ── orders_shipped ────────────────────────────────────────────────────────

CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce.orders_shipped (
    event_type         STRING,
    order_id           STRING,
    carrier            STRING,
    tracking_number    STRING,
    estimated_delivery STRING,
    timestamp          STRING
)
PARTITIONED BY (year STRING, month STRING, day STRING, hour STRING)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://YOUR_BUCKET/orders.shipped/'
TBLPROPERTIES ('has_encrypted_data'='false');

MSCK REPAIR TABLE ecommerce.orders_shipped;

-- ── page_views ────────────────────────────────────────────────────────────

CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce.page_views (
    event_type  STRING,
    session_id  STRING,
    customer_id STRING,
    product_id  STRING,
    page_url    STRING,
    device_type STRING,
    timestamp   STRING
)
PARTITIONED BY (year STRING, month STRING, day STRING, hour STRING)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://YOUR_BUCKET/page.views/'
TBLPROPERTIES ('has_encrypted_data'='false');

MSCK REPAIR TABLE ecommerce.page_views;

-- ── dead-letter queue ─────────────────────────────────────────────────────

CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce.dlq (
    event_type STRING,
    error      STRING
)
PARTITIONED BY (year STRING, month STRING, day STRING, hour STRING)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://YOUR_BUCKET/orders.dlq/'
TBLPROPERTIES ('has_encrypted_data'='false');

MSCK REPAIR TABLE ecommerce.dlq;

-- ── Quick sanity checks ───────────────────────────────────────────────────

-- SELECT COUNT(*) FROM ecommerce.orders_placed;
-- SELECT category, COUNT(*) as cnt, AVG(price) as avg_price
--   FROM ecommerce.orders_placed GROUP BY category ORDER BY cnt DESC;
-- SELECT device_type, COUNT(*) FROM ecommerce.page_views GROUP BY device_type;
