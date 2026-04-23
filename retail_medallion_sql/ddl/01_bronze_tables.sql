-- Bronze layer Delta table DDL (reference; sync column names with notebooks)
-- Row idempotency uses _bronze_row_key = sha2(concatenation of source columns)

USE CATALOG practice;
USE SCHEMA bronze;

-- Customers (synthetic: customer_id, name, contact, address, dates, loyalty)
CREATE TABLE IF NOT EXISTS customers (
  customer_id      INT     NOT NULL,
  first_name       STRING,
  last_name        STRING,
  email            STRING,
  city             STRING,
  state            STRING,
  signup_date      DATE,
  loyalty_tier     STRING,
  _ingest_ts       TIMESTAMP NOT NULL,
  _source_path     STRING  NOT NULL,
  _source_name     STRING,
  _source_size_bytes BIGINT,
  _bronze_row_key  STRING  NOT NULL
) USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'comment' = 'Idempotent MERGE on _bronze_row_key; COPY-style append alternative documented in notebook.'
);

-- Products
CREATE TABLE IF NOT EXISTS products (
  product_id       INT     NOT NULL,
  product_name     STRING,
  category         STRING,
  brand            STRING,
  price            DOUBLE,
  created_date     DATE,
  _ingest_ts       TIMESTAMP NOT NULL,
  _source_path     STRING  NOT NULL,
  _source_name     STRING,
  _source_size_bytes BIGINT,
  _bronze_row_key  STRING  NOT NULL
) USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
);

-- Transactions
CREATE TABLE IF NOT EXISTS transactions (
  transaction_id          INT     NOT NULL,
  customer_id             INT,
  product_id              INT     NOT NULL,
  quantity                INT     NOT NULL,
  transaction_amount      DOUBLE  NOT NULL,
  transaction_timestamp   TIMESTAMP NOT NULL,
  channel                 STRING  NOT NULL,
  _ingest_ts              TIMESTAMP NOT NULL,
  _source_path            STRING  NOT NULL,
  _source_name            STRING,
  _source_size_bytes      BIGINT,
  _bronze_row_key         STRING  NOT NULL
) USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
);
