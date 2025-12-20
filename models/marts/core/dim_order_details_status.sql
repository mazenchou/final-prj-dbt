-- models/marts/core/dim_order_details_status.sql
{{ 
    config(
        materialized='table',
        tags=['dimension', 'status']
    ) 
}}

SELECT
    status_id,
    status_name,
    CURRENT_TIMESTAMP() as loaded_at
FROM {{ ref('int_order_details_status') }}
-- Simple dimension, just IDs and names