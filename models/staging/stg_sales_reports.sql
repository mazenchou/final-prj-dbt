{{ config(materialized='view') }}

SELECT
  TRIM(CAST(group_by AS STRING)) as group_by_column,
  TRIM(CAST(display AS STRING)) as display_name,
  TRIM(CAST(title AS STRING)) as report_title,
  TRIM(CAST(filter_row_source AS STRING)) as filter_source,
  TRIM(CAST(`default` AS STRING)) as default_setting,  -- ← FIXED: Backticks around default
  CURRENT_TIMESTAMP() as loaded_at
FROM 
{{ source('raw_data', 'sales_reports') }}