{{ config(materialized='view') }}

SELECT
  CAST(id AS INT64) as inventory_transaction_id,
  CAST(transaction_type AS INT64) as transaction_type_id,
  SAFE.PARSE_DATETIME('%Y/%m/%d %H:%M:%S', transaction_created_date) as transaction_created_at,
  SAFE.PARSE_DATETIME('%Y/%m/%d %H:%M:%S', transaction_modified_date) as transaction_modified_at,
  CAST(product_id AS INT64) as product_id,
  CAST(quantity AS INT64) as quantity,
  CAST(purchase_order_id AS INT64) as purchase_order_id,
  CAST(customer_order_id AS INT64) as customer_order_id,
  TRIM(comments) as comments,
  CURRENT_TIMESTAMP() as loaded_at
FROM {{ ref('inventory_transactions') }}
