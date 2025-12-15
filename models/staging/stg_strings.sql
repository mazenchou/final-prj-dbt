-- models/staging/stg_strings.sql
{{ config(materialized='view') }}

SELECT
  CAST(string_id AS INT64) as string_id,
  TRIM(CAST(string_data AS STRING)) as string_text,
  CURRENT_TIMESTAMP() as loaded_at
FROM {{ ref('strings') }}