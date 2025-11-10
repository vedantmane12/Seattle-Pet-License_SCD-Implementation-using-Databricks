-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Assignment 08: Seattle Pet License SCD Implementation using Databricks

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze Layer Implementation

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE workspace.spl_schema.spl_bronze
TBLPROPERTIES (
  "checkpointLocation" = "/Volumes/workspace/spl_schema/datastore/SPL/checkpoint/spl_bronze",
  'delta.columnMapping.mode' = 'name'
)
AS
SELECT
  *,
  current_timestamp() AS load_dt,
  _metadata.file_path AS source_file_path,
  _metadata.file_name AS source_file_name,
  -- Extract year and month from the filename
  to_date(
    regexp_extract(
      _metadata.file_name, '(\\d{4}_\\d{2})', 1
    ), 'yyyy_MM'
  ) AS source_file_date,
  -- Compute end date as the last day of the month
  last_day(
    to_date(
      regexp_extract(
        _metadata.file_name, '(\\d{4}_\\d{2})', 1
      ), 'yyyy_MM'
    )
  ) AS source_file_end_date
FROM STREAM cloud_files(
  "/Volumes/workspace/spl_schema/datastore/SPL",
  "csv"
);
