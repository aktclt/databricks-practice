-- Optional: pipeline checkpoint for incremental handoffs (not required for idempotent full-merge runs)

USE CATALOG practice;
CREATE SCHEMA IF NOT EXISTS meta COMMENT 'Internal pipeline state (optional)';

USE SCHEMA meta;

CREATE TABLE IF NOT EXISTS layer_run_state (
  layer         STRING  NOT NULL,
  last_event_ts TIMESTAMP  NOT NULL,
  run_id        STRING,
  run_notes     STRING
) USING DELTA
TBLPROPERTIES ( 'comment' = 'Last successful ETL per layer; use for watermarks in incremental extensions.');

-- Insert seed row (optional; notebook may update)
-- INSERT INTO layer_run_state VALUES ('bronze', to_timestamp('1970-01-01'), 'init', 'bootstrap');
