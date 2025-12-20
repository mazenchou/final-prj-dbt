-- models/staging/stg_inventory_transactions.sql (CONSISTENT FORMAT)
{{ config(materialized='view') }}

SELECT
  CAST(id AS INT64) as inventory_transaction_id,
  CAST(transaction_type AS INT64) as transaction_type_id,
  
  -- SAME as purchase_orders: NO CONCAT, just parse directly
  CASE 
    WHEN transaction_created_date IS NOT NULL AND TRIM(transaction_created_date) != ''
    THEN SAFE.PARSE_DATETIME('%m/%d/%Y %H:%M:%S', TRIM(transaction_created_date))
    ELSE NULL
  END as transaction_created_at,
  
  CASE 
    WHEN transaction_modified_date IS NOT NULL AND TRIM(transaction_modified_date) != ''
    THEN SAFE.PARSE_DATETIME('%m/%d/%Y %H:%M:%S', TRIM(transaction_modified_date))
    ELSE NULL
  END as transaction_modified_at,
  
  CAST(product_id AS INT64) as product_id,
  CAST(quantity AS INT64) as quantity,
  CAST(purchase_order_id AS INT64) as purchase_order_id,
  CAST(customer_order_id AS INT64) as customer_order_id,
  TRIM(comments) as comments,
  CURRENT_TIMESTAMP() as loaded_at
FROM {{ source('raw_data', 'inventory_transactions') }}