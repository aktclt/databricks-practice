# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — incremental, idempotent load
# MAGIC
# MAGIC - **Idempotency:** `MERGE` on a deterministic `_bronze_row_key` (sha256 over data columns) so
# MAGIC   re-runs of the same CSV rows do not duplicate. Same key with changed content **updates** the row.
# MAGIC - **Prerequisite:** run `retail_medallion_sql/ddl/00_schemas.sql` and `01_bronze_tables.sql` once, or
# MAGIC   `CREATE IF NOT EXISTS` the tables from the repo (DDL is reference / drift control).
# MAGIC - **Source:** ` /Volumes/{catalog}/demo_dw_raw/raw_data/{customers,products,transactions}/`

# COMMAND ----------

# --- configuration (edit or override with widgets in Jobs) --------------------
CATALOG = "practice"
VOLUME_BASE = f"/Volumes/{CATALOG}/demo_dw_raw/raw_data"

# COMMAND ----------

import pyspark.sql.functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers: read CSV, add lineage + row key, merge into target Delta

# COMMAND ----------


def _precondition(table: str) -> None:
    full = f"{CATALOG}.bronze.{table}"
    if not spark.catalog.tableExists(full):
        raise RuntimeError(
            f"Missing {full}. Apply retail_medallion_sql/ddl/00_schemas.sql and 01_bronze_tables.sql first."
        )


def _sha2_key(df, *cols: str):
    """Concatenate listed columns (string) for a stable per-row id before ingest metadata."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]
    return F.sha2(F.concat_ws("||", *parts), 256)


def load_merge_customers():
    _precondition("customers")
    path = f"{VOLUME_BASE}/customers/"
    r = (
        spark.read.option("header", "true")
        .option("recursiveFileLookup", "true")
        .csv(path)
    )
    s = (
        r.withColumn("customer_id", F.col("customer_id").cast("int"))
        .withColumn("first_name", F.col("first_name"))
        .withColumn("last_name", F.col("last_name"))
        .withColumn("email", F.col("email"))
        .withColumn("city", F.col("city"))
        .withColumn("state", F.col("state"))
        .withColumn("signup_date", F.to_date(F.col("signup_date")))
        .withColumn("loyalty_tier", F.col("loyalty_tier"))
    )
    key = _sha2_key(
        s, "customer_id", "first_name", "last_name", "email", "city", "state", "signup_date", "loyalty_tier"
    )
    s = (
        s.withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_path", F.input_file_name())
        .withColumn("_source_name", F.element_at(F.split(F.input_file_name(), "/"), -1))
        .withColumn("_source_size_bytes", F.lit(None).cast("long"))
        .withColumn("_bronze_row_key", key)
    )
    (
        DeltaTable.forName(spark, f"{CATALOG}.bronze.customers")
        .alias("t")
        .merge(
            s.alias("s"),
            "t._bronze_row_key = s._bronze_row_key",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def load_merge_products():
    _precondition("products")
    path = f"{VOLUME_BASE}/products/"
    r = (
        spark.read.option("header", "true")
        .option("recursiveFileLookup", "true")
        .csv(path)
    )
    s = (
        r.withColumn("product_id", F.col("product_id").cast("int"))
        .withColumn("product_name", F.col("product_name"))
        .withColumn("category", F.col("category"))
        .withColumn("brand", F.col("brand"))
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("created_date", F.to_date(F.col("created_date")))
    )
    key = _sha2_key(s, "product_id", "product_name", "category", "brand", "price", "created_date")
    s = (
        s.withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_path", F.input_file_name())
        .withColumn("_source_name", F.element_at(F.split(F.input_file_name(), "/"), -1))
        .withColumn("_source_size_bytes", F.lit(None).cast("long"))
        .withColumn("_bronze_row_key", key)
    )
    (
        DeltaTable.forName(spark, f"{CATALOG}.bronze.products")
        .alias("t")
        .merge(s.alias("s"), "t._bronze_row_key = s._bronze_row_key")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def load_merge_transactions():
    _precondition("transactions")
    path = f"{VOLUME_BASE}/transactions/"
    r = (
        spark.read.option("header", "true")
        .option("recursiveFileLookup", "true")
        .csv(path)
    )
    s = (
        r.withColumn("transaction_id", F.col("transaction_id").cast("int"))
        .withColumn(
            "customer_id",
            F.when((F.col("customer_id") == "") | F.col("customer_id").isNull(), F.lit(None))
            .otherwise(F.col("customer_id").cast("int")),
        )
        .withColumn("product_id", F.col("product_id").cast("int"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("transaction_amount", F.col("transaction_amount").cast("double"))
        .withColumn("transaction_timestamp", F.to_timestamp(F.col("transaction_timestamp")))
        .withColumn("channel", F.col("channel"))
    )
    key = _sha2_key(
        s,
        "transaction_id",
        "customer_id",
        "product_id",
        "quantity",
        "transaction_amount",
        "transaction_timestamp",
        "channel",
    )
    s = (
        s.withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_path", F.input_file_name())
        .withColumn("_source_name", F.element_at(F.split(F.input_file_name(), "/"), -1))
        .withColumn("_source_size_bytes", F.lit(None).cast("long"))
        .withColumn("_bronze_row_key", key)
    )
    (
        DeltaTable.forName(spark, f"{CATALOG}.bronze.transactions")
        .alias("t")
        .merge(s.alias("s"), "t._bronze_row_key = s._bronze_row_key")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Run (order: independent feeds; you may parallelize in a Job with preconditions)
# MAGIC - **File-level alternative:** for append-only, file-granular idempotency use
# MAGIC   `COPY INTO` on these paths; then add `_ingest_ts` with defaults in a follow-up. This notebook
# MAGIC   prioritizes **row MERGE** so corrected rows replace by content hash.

# COMMAND ----------

load_merge_customers()
load_merge_products()
load_merge_transactions()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row counts (quick validation)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM practice.bronze.customers)     AS n_customers,
# MAGIC   (SELECT COUNT(*) FROM practice.bronze.products)  AS n_products,
# MAGIC   (SELECT COUNT(*) FROM practice.bronze.transactions)  AS n_transactions;

# COMMAND ----------

# Optional: record success (if practice.meta.layer_run_state exists; see ddl/04_pipeline_control.sql)
try:
    spark.sql(
        """
        INSERT INTO practice.meta.layer_run_state (layer, last_event_ts, run_id, run_notes)
        VALUES ('bronze', current_timestamp(), '01_bronze_incremental', 'ok')
        """
    )
except Exception:
    pass
