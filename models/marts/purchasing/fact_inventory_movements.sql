-- models/marts/purchasing/fact_inventory_movements.sql
{{ config(
    materialized='table',
    tags=['fact', 'inventory', 'purchasing']
) }}

SELECT
  inventory_transaction_id as fact_inventory_id,
  
  -- Foreign keys
  transaction_type_id,
  product_id,
  CAST(FORMAT_DATE('%Y%m%d', DATE(transaction_created_at)) AS INT64) as transaction_date_id,
  customer_order_id as order_id,
  purchase_order_id,

  -- Measures
  quantity,
  
  -- Comments
  comments,
  
  -- Original dates (for reference)
  transaction_created_at,
  transaction_modified_at

FROM {{ ref('int_inventory_transactions') }}
WHERE inventory_transaction_id IS NOT NULL