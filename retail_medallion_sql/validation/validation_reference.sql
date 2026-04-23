-- =============================================================================
-- RETAIL MEDALLION – validation & sample queries (reference only, no ETL)
-- Run manually or as optional SQL task after: 01_bronze / 02_silver / 03_gold notebooks
-- =============================================================================

-- ---------------------------------------------------------------------------
-- A) After bronze
-- ---------------------------------------------------------------------------
USE CATALOG practice;

SELECT
  'customers'     AS table_name, COUNT(*) AS row_count, COUNT(DISTINCT customer_id) AS dist_keys
  FROM practice.bronze.customers
UNION ALL
SELECT 'products', COUNT(*), COUNT(DISTINCT product_id) FROM practice.bronze.products
UNION ALL
SELECT 'transactions', COUNT(*), COUNT(DISTINCT transaction_id) FROM practice.bronze.transactions;

SELECT
  'dup_customer_id'  AS check_name, COUNT(*) - COUNT(DISTINCT customer_id) AS dup_count
  FROM practice.bronze.customers
UNION ALL
SELECT 'dup_product_id', COUNT(*) - COUNT(DISTINCT product_id) FROM practice.bronze.products
UNION ALL
SELECT 'dup_transaction_id', COUNT(*) - COUNT(DISTINCT transaction_id) FROM practice.bronze.transactions;

SELECT * FROM practice.bronze.customers   LIMIT 3;
SELECT * FROM practice.bronze.products    LIMIT 3;
SELECT * FROM practice.bronze.transactions LIMIT 3;

-- ---------------------------------------------------------------------------
-- B) After silver
-- ---------------------------------------------------------------------------
SELECT
  'dim_customer'   AS obj, COUNT(*)               AS n,
  COUNT(DISTINCT customer_id)                     AS dist_bk,
  SUM(CASE WHEN is_current THEN 1 ELSE 0 END)    AS current_rows
  FROM practice.silver.dim_customer
UNION ALL
SELECT 'dim_product',     COUNT(*), COUNT(DISTINCT product_id), SUM(CASE WHEN is_current THEN 1 ELSE 0 END)
  FROM practice.silver.dim_product;

SELECT
  s.bucket  AS quarantine_category,
  COUNT(*)  AS n
  FROM (
    SELECT CASE
        WHEN reason LIKE '%product_id_not_in%' THEN 'missing_product'
        WHEN reason LIKE '%email_null%'       THEN 'email_null_audit'
        ELSE 'other' END AS bucket
    FROM practice.silver.quarantine_rows
) s
GROUP BY 1
ORDER BY 1;

SELECT
  f.transaction_id, f.order_date, c.customer_name, p.category, f.transaction_amount, f.is_anonymous_or_unlinked
  FROM practice.silver.fact_transaction f
  LEFT  JOIN practice.silver.dim_customer c ON f.customer_sk = c.customer_sk AND c.is_current
  JOIN practice.silver.dim_product  p  ON f.product_sk = p.product_sk AND p.is_current
  LIMIT 20;

-- ---------------------------------------------------------------------------
-- C) After gold
-- ---------------------------------------------------------------------------
SELECT
  (SELECT ROUND(SUM(transaction_amount), 2) FROM practice.silver.fact_transaction) AS fact_total,
  (SELECT ROUND(SUM(total_revenue), 2)     FROM practice.gold.daily_revenue)  AS sum_daily;

SELECT * FROM practice.gold.daily_revenue              LIMIT 10;
SELECT * FROM practice.gold.customer_spend_by_month   LIMIT 15;
SELECT * FROM practice.gold.category_sales_daily     LIMIT 15;
