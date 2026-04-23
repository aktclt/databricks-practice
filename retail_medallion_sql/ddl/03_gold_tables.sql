-- Gold reporting tables (idempotent key-based MERGE from aggregates over silver)

USE CATALOG practice;
USE SCHEMA gold;

CREATE TABLE IF NOT EXISTS daily_revenue (
  transaction_date  DATE  NOT NULL,
  total_revenue     DOUBLE  NOT NULL,
  average_order_value  DOUBLE  NOT NULL,
  total_orders      BIGINT  NOT NULL
) USING DELTA
TBLPROPERTIES ( 'comment' = 'One row per calendar day; key = transaction_date' );

CREATE TABLE IF NOT EXISTS customer_spend_by_month (
  month             DATE    NOT NULL,
  customer_id       INT     NOT NULL,
  customer_name     STRING  NOT NULL,
  total_spend       DOUBLE  NOT NULL,
  transaction_count BIGINT  NOT NULL,
  rank_by_spend     INT     NOT NULL
) USING DELTA
TBLPROPERTIES ( 'comment' = 'Per customer per month; rank within month' );

CREATE TABLE IF NOT EXISTS category_sales_daily (
  transaction_date  DATE  NOT NULL,
  category         STRING  NOT NULL,
  total_revenue     DOUBLE  NOT NULL
) USING DELTA
TBLPROPERTIES ( 'comment' = 'Per category per day' );
