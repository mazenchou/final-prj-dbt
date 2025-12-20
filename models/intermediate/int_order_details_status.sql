-- models/intermediate/int_order_details_status.sql
{{ config(materialized='view') }}

SELECT
    status_id,
    status_name,
    loaded_at
FROM {{ ref('stg_order_details_status') }}
-- That's it! No extra columns