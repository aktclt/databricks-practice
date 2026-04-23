-- Unity Catalog: practice catalog, separate schemas for bronze / silver / gold
-- (Reference only — run once per workspace or as part of IaC/Job SQL task)
--
-- Load & merge logic: notebooks/retail_medallion/01_bronze_incremental.py
--   → 02_silver_incremental.py → 03_gold_incremental.py

USE CATALOG practice;

CREATE SCHEMA IF NOT EXISTS bronze
  COMMENT 'Medallion bronze: idempotent file/row-merged loads from volume CSVs.';

CREATE SCHEMA IF NOT EXISTS silver
  COMMENT 'Medallion silver: deduplicated, cleansed, SCD2 dimension/fact.';

CREATE SCHEMA IF NOT EXISTS gold
  COMMENT 'Medallion gold: reporting aggregates, merge-idempotent.';
