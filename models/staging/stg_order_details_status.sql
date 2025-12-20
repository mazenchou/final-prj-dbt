-- models/staging/stg_order_details_status.sql
{{ config(materialized='view') }}

SELECT
    CAST(id AS INT64) as status_id,
    TRIM(status) as status_name,
    CURRENT_TIMESTAMP() as loaded_at
FROM 
{{ source('raw_data', 'order_details_status') }}

WHERE id IS NOT NULL
  AND status IS NOT NULL