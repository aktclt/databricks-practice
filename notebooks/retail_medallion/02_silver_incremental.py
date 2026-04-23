# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — idempotent refresh (dedupe, quarantine, dims, fact)
# MAGIC
# MAGIC - **Behavior:** `CREATE OR REPLACE` each silver table from **current bronze** in dependency order.
# MAGIC   Re-running with the same bronze data yields the same silver (deterministic) — **idempotent refresh**.
# MAGIC   For **true incremental** MERGE/SCD2, extend using `row_hash` and `is_current` (see comments in
# MAGIC   `retail_medallion_sql/ddl/02_silver_tables.sql`); the transformation logic matches the
# MAGIC   reference SQL that previously lived in `20_silver_retail.sql`.
# MAGIC - **Prerequisite:** bronze filled (`01_bronze_incremental`) and `ddl/02_silver_tables.sql` applied
# MAGIC   (or equivalent `CREATE IF NOT EXISTS`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Dim customer

# COMMAND ----------

spark.sql(
    """
    CREATE OR REPLACE TABLE practice.silver.dim_customer
    USING DELTA
    TBLPROPERTIES ( 'delta.enableChangeDataFeed' = 'true' )
    AS
    WITH dedup AS (
      SELECT
        c.*,
        ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY c._ingest_ts DESC, c._source_path) AS _rn
      FROM practice.bronze.customers c
    ),
    stg AS (
      SELECT
        d.customer_id,
        TRIM(COALESCE(d.first_name,  'Unknown')) AS first_name,
        TRIM(COALESCE(d.last_name,   'Unknown')) AS last_name,
        TRIM(concat(COALESCE(d.first_name, ''), ' ', COALESCE(d.last_name,  ''))) AS customer_name,
        d.email  AS email_raw,
        d.city   AS city_raw,
        d.state  AS state_raw,
        d.signup_date,
        d.loyalty_tier,
        d._ingest_ts,
        COALESCE(NULLIF(TRIM(d.email),  ''), CONCAT('unknown+', CAST(d.customer_id AS STRING), '@imputed.invalid'))  AS email,
        COALESCE(NULLIF(TRIM(d.city),  ''),  'Unknown')  AS city,
        COALESCE(NULLIF(TRIM(d.state),  ''), 'UN')  AS state,
        SHA2(
          CONCAT_WS(
            '||',
            CAST(d.customer_id AS STRING),
            COALESCE(d.first_name, ''), COALESCE(d.last_name,  ''),
            COALESCE(d.email,  ''),     COALESCE(d.city,  ''),  COALESCE(d.state,  ''),
            CAST(d.signup_date AS STRING), COALESCE(d.loyalty_tier,  '')
          ),
          256
        ) AS row_hash
      FROM dedup d
      WHERE d._rn = 1
    )
    SELECT
      ROW_NUMBER() OVER (ORDER BY s.customer_id)  AS customer_sk,
      s.customer_id,
      s.first_name,
      s.last_name,
      s.customer_name,
      s.email,
      s.email_raw,
      s.city,
      s.city_raw,
      s.state,
      s.signup_date,
      s.loyalty_tier,
      s._ingest_ts     AS as_of_ingest_ts,
      s.row_hash,
      CURRENT_TIMESTAMP()  AS valid_from,
      to_timestamp('9999-12-31 23:59:59')  AS valid_to,
      TRUE  AS is_current
    FROM stg s
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Dim product (depends only on bronze)

# COMMAND ----------

spark.sql(
    """
    CREATE OR REPLACE TABLE practice.silver.dim_product
    USING DELTA
    TBLPROPERTIES ( 'delta.enableChangeDataFeed' = 'true' )
    AS
    WITH dedup AS (
      SELECT
        p.*,
        ROW_NUMBER() OVER (PARTITION BY p.product_id ORDER BY p._ingest_ts DESC, p._source_path) AS _rn
      FROM practice.bronze.products p
    ),
    stg AS (
      SELECT
        d.product_id,
        d.product_name,
        d.category,
        d.brand,
        d.price,
        d.created_date,
        d._ingest_ts,
        SHA2(
          CONCAT_WS('||', CAST(d.product_id AS STRING), d.product_name, d.category, d.brand, CAST(d.price AS STRING), CAST(d.created_date AS STRING)),
          256
        ) AS row_hash
      FROM dedup d
      WHERE d._rn = 1
    )
    SELECT
      ROW_NUMBER() OVER (ORDER BY s.product_id)  AS product_sk,
      s.product_id,
      s.product_name,
      s.category,
      s.brand,
      s.price,
      s.created_date,
      s._ingest_ts AS as_of_ingest_ts,
      s.row_hash,
      CURRENT_TIMESTAMP()  AS valid_from,
      to_timestamp('9999-12-31 23:59:59')  AS valid_to,
      TRUE  AS is_current
    FROM stg s
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Quarantine rows (audit: missing product, null email in latest bronze row per customer)

# COMMAND ----------

spark.sql(
    """
    CREATE OR REPLACE TABLE practice.silver.quarantine_rows
    USING DELTA
    AS
    SELECT
      current_timestamp() AS quarantined_at,
      'bronze'  AS source_layer,
      'transaction' AS entity,
      CAST(t.transaction_id AS STRING)  AS business_key,
      'product_id_not_in_bronze_product_feed'  AS reason
    FROM practice.bronze.transactions t
    LEFT ANTI JOIN ( SELECT DISTINCT product_id FROM practice.bronze.products ) p
      ON t.product_id = p.product_id
    UNION ALL
    SELECT
      current_timestamp(), 'bronze', 'customer', CAST(c.customer_id AS STRING), 'email_null_will_impute_in_dim'
    FROM (
      SELECT customer_id, email, _ingest_ts, _source_path,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _ingest_ts DESC) AS _rn
      FROM practice.bronze.customers
    ) c
    WHERE c._rn = 1
      AND (c.email IS NULL OR TRIM(c.email) = '')
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Fact (joins to dims we just built)

# COMMAND ----------

spark.sql(
    """
    CREATE OR REPLACE TABLE practice.silver.fact_transaction
    USING DELTA
    TBLPROPERTIES ( 'delta.enableChangeDataFeed' = 'true' )
    AS
    WITH tx_dedup AS (
      SELECT
        t.*,
        ROW_NUMBER() OVER (PARTITION BY t.transaction_id ORDER BY t._ingest_ts DESC) AS _rn
      FROM practice.bronze.transactions t
    ),
    f AS ( SELECT * FROM tx_dedup WHERE _rn = 1 )
    SELECT
      f.transaction_id,
      dc.customer_sk,
      dp.product_sk,
      to_date(f.transaction_timestamp)  AS order_date,
      f.transaction_timestamp,
      f.transaction_amount,
      f.quantity,
      f.channel,
      f.customer_id IS NULL OR dc.customer_sk IS NULL AS is_anonymous_or_unlinked
    FROM f
    INNER JOIN practice.silver.dim_product dp
      ON f.product_id = dp.product_id AND dp.is_current
    LEFT JOIN practice.silver.dim_customer dc
      ON f.customer_id = dc.customer_id
     AND dc.is_current
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

spark.sql(
    """
    SELECT
      (SELECT COUNT(*) FROM practice.silver.dim_customer)    AS dim_customer,
      (SELECT COUNT(*) FROM practice.silver.dim_product)   AS dim_product,
      (SELECT COUNT(*) FROM practice.silver.fact_transaction)  AS fact_transaction,
      (SELECT COUNT(*) FROM practice.silver.quarantine_rows)  AS quarantine_rows,
      'SILVER_LOAD_COMPLETE' AS run_message
    """
).show()

# COMMAND ----------

try:
    spark.sql(
        """
        INSERT INTO practice.meta.layer_run_state (layer, last_event_ts, run_id, run_notes)
        VALUES ('silver', current_timestamp(), '02_silver_incremental', 'ok')
        """
    )
except Exception:
    pass
