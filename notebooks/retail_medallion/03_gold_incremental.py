# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — idempotent MERGE into reporting tables
# MAGIC
# MAGIC - **Idempotency:** aggregates are recomputed from **silver**, then **MERGE**d on:
# MAGIC   - `daily_revenue`: `transaction_date`
# MAGIC   - `customer_spend_by_month`: (`month`, `customer_id`)
# MAGIC   - `category_sales_daily`: (`transaction_date`, `category`)
# MAGIC - Re-running with the same fact/dim data overwrites the same keys (no duplicate keys).
# MAGIC - **Prerequisite:** silver layer loaded and `ddl/03_gold_tables.sql` applied.

# COMMAND ----------

CATALOG = "practice"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Daily revenue

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW _tmp_daily_revenue AS
    SELECT
      f.order_date  AS transaction_date,
      SUM(f.transaction_amount)  AS total_revenue,
      SUM(f.transaction_amount) / NULLIF(COUNT(DISTINCT f.transaction_id), 0)  AS average_order_value,
      COUNT(DISTINCT f.transaction_id)  AS total_orders
    FROM {CATALOG}.silver.fact_transaction f
    GROUP BY f.order_date
    """
)

spark.sql(
    f"""
    MERGE INTO {CATALOG}.gold.daily_revenue t
    USING _tmp_daily_revenue s
    ON t.transaction_date = s.transaction_date
    WHEN MATCHED THEN UPDATE SET
      t.total_revenue = s.total_revenue,
      t.average_order_value = s.average_order_value,
      t.total_orders = s.total_orders
    WHEN NOT MATCHED THEN INSERT (transaction_date, total_revenue, average_order_value, total_orders)
    VALUES (s.transaction_date, s.total_revenue, s.average_order_value, s.total_orders)
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Customer spend by month (non-anonymous transactions only)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW _tmp_cust_spend AS
    WITH spent AS (
      SELECT
        CAST(DATE_TRUNC('month', f.order_date) AS DATE)  AS month,
        c.customer_id,
        c.customer_name,
        SUM(f.transaction_amount)  AS total_spend,
        COUNT(*)  AS transaction_count
      FROM {CATALOG}.silver.fact_transaction f
      INNER JOIN {CATALOG}.silver.dim_customer c
        ON f.customer_sk = c.customer_sk AND c.is_current
      WHERE f.customer_sk IS NOT NULL
      GROUP BY
        CAST(DATE_TRUNC('month', f.order_date) AS DATE),
        c.customer_id,
        c.customer_name
    )
    SELECT
      s.month,
      s.customer_id,
      s.customer_name,
      s.total_spend,
      s.transaction_count,
      RANK() OVER (PARTITION BY s.month ORDER BY s.total_spend DESC)  AS rank_by_spend
    FROM spent s
    """
)

spark.sql(
    f"""
    MERGE INTO {CATALOG}.gold.customer_spend_by_month t
    USING _tmp_cust_spend s
    ON t.month = s.month AND t.customer_id = s.customer_id
    WHEN MATCHED THEN UPDATE SET
      t.customer_name = s.customer_name,
      t.total_spend = s.total_spend,
      t.transaction_count = s.transaction_count,
      t.rank_by_spend = s.rank_by_spend
    WHEN NOT MATCHED THEN INSERT
      (month, customer_id, customer_name, total_spend, transaction_count, rank_by_spend)
    VALUES
      (s.month, s.customer_id, s.customer_name, s.total_spend, s.transaction_count, s.rank_by_spend)
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Category sales by day

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW _tmp_cat_sales AS
    SELECT
      f.order_date  AS transaction_date,
      p.category,
      SUM(f.transaction_amount)  AS total_revenue
    FROM {CATALOG}.silver.fact_transaction f
    INNER JOIN {CATALOG}.silver.dim_product p
      ON f.product_sk = p.product_sk AND p.is_current
    GROUP BY f.order_date, p.category
    """
)

spark.sql(
    f"""
    MERGE INTO {CATALOG}.gold.category_sales_daily t
    USING _tmp_cat_sales s
    ON t.transaction_date = s.transaction_date AND t.category = s.category
    WHEN MATCHED THEN UPDATE SET t.total_revenue = s.total_revenue
    WHEN NOT MATCHED THEN INSERT (transaction_date, category, total_revenue)
    VALUES (s.transaction_date, s.category, s.total_revenue)
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

spark.sql(
    f"""
    SELECT
      (SELECT COUNT(*) FROM {CATALOG}.gold.daily_revenue)  AS n_daily,
      (SELECT COUNT(*) FROM {CATALOG}.gold.customer_spend_by_month)  AS n_cust_mo,
      (SELECT COUNT(*) FROM {CATALOG}.gold.category_sales_daily)  AS n_cat_day
    """
).show()

# COMMAND ----------

try:
    spark.sql(
        f"""
        INSERT INTO {CATALOG}.meta.layer_run_state (layer, last_event_ts, run_id, run_notes)
        VALUES ('gold', current_timestamp(), '03_gold_incremental', 'ok')
        """
    )
except Exception:
    pass
