-- models/marts/core/dim_inventory_transaction_type.sql
{{ config(
    materialized='table',
    tags=['dimension', 'reference']
) }}

SELECT
  transaction_type_id,  -- CHANGED from id
  transaction_type_name,
  loaded_at
FROM {{ ref('stg_inventory_transaction_types') }}
WHERE transaction_type_id IS NOT NULL