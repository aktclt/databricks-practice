-- Silver layer (reference; notebook MERGE/upsert for idempotency on business / surrogate keys)

USE CATALOG practice;
USE SCHEMA silver;

-- Customer dimension: SCD2 structure; merge logic is in 02_silver notebook
CREATE TABLE IF NOT EXISTS dim_customer (
  customer_sk       BIGINT NOT NULL,
  customer_id       INT    NOT NULL,
  first_name        STRING,
  last_name         STRING,
  customer_name     STRING,
  email             STRING,
  email_raw         STRING,
  city              STRING,
  city_raw          STRING,
  state             STRING,
  signup_date       DATE,
  loyalty_tier      STRING,
  as_of_ingest_ts   TIMESTAMP,
  row_hash          STRING,
  valid_from        TIMESTAMP,
  valid_to          TIMESTAMP,
  is_current        BOOLEAN
) USING DELTA
TBLPROPERTIES ( 'delta.enableChangeDataFeed' = 'true' );

-- Product dimension
CREATE TABLE IF NOT EXISTS dim_product (
  product_sk      BIGINT  NOT NULL,
  product_id      INT     NOT NULL,
  product_name    STRING,
  category        STRING,
  brand           STRING,
  price           DOUBLE,
  created_date    DATE,
  as_of_ingest_ts TIMESTAMP,
  row_hash        STRING,
  valid_from      TIMESTAMP,
  valid_to        TIMESTAMP,
  is_current      BOOLEAN
) USING DELTA
TBLPROPERTIES ( 'delta.enableChangeDataFeed' = 'true' );

-- Quarantine (append-only from validation passes)
CREATE TABLE IF NOT EXISTS quarantine_rows (
  quarantined_at  TIMESTAMP,
  source_layer    STRING,
  entity          STRING,
  business_key    STRING,
  reason          STRING
) USING DELTA;

-- Fact: one business row per transaction_id after silver deduplication
CREATE TABLE IF NOT EXISTS fact_transaction (
  transaction_id            INT     NOT NULL,
  customer_sk               BIGINT,
  product_sk                BIGINT  NOT NULL,
  order_date                DATE    NOT NULL,
  transaction_timestamp     TIMESTAMP NOT NULL,
  transaction_amount        DOUBLE  NOT NULL,
  quantity                  INT     NOT NULL,
  channel                   STRING  NOT NULL,
  is_anonymous_or_unlinked  BOOLEAN NOT NULL
) USING DELTA
TBLPROPERTIES ( 'delta.enableChangeDataFeed' = 'true' );
