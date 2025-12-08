-- models/core/fct_inventory_transactions.sql
{{ config(
    materialized='table',
    tags=['fact', 'inventory']
) }}

SELECT
  inventory_transaction_id,
  transaction_type_id,
  product_id,
  quantity,
  purchase_order_id,
  customer_order_id,
  
  -- Date keys
  CAST(FORMAT_DATE('%Y%m%d', DATE(transaction_created_at)) AS INT64) as transaction_date_id,
  
  -- Original dates (keep during transition)
  transaction_created_at,
  transaction_modified_at,
  
  comments,
  loaded_at
FROM {{ ref('int_inventory_transactions') }}
WHERE inventory_transaction_id IS NOT NULL