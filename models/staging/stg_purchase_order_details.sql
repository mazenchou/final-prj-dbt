{{ config(materialized='view') }}

SELECT
  CAST(id AS INT64) as purchase_order_detail_id,
  CAST(purchase_order_id AS INT64) as purchase_order_id,
  CAST(product_id AS INT64) as product_id,
  CAST(quantity AS INT64) as quantity,
  CAST(unit_cost AS FLOAT64) as unit_cost,
  PARSE_DATE('%Y-%m-%d', date_received) as date_received,
  CAST(posted_to_inventory AS INT64) as posted_to_inventory_flag,
  CAST(inventory_id AS INT64) as inventory_id,
  CAST(quantity * unit_cost AS FLOAT64) as total_cost,
  CURRENT_TIMESTAMP() as loaded_at
FROM {{ ref('purchase_order_details') }}