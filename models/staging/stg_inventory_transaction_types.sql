{{ config(materialized='view') }}

SELECT
  CAST(id AS INT64) as transaction_type_id,
  INITCAP(TRIM(CAST(type_name AS STRING))) as transaction_type_name,
  CURRENT_TIMESTAMP() as loaded_at
FROM {{ ref('inventory_transaction_types') }}