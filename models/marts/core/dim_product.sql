-- models/core/dim_products.sql
{{ config(
    materialized='table',
    tags=['dimension', 'product']
) }}

SELECT
  product_id,
  product_code,
  product_name,
  description,
  standard_cost,
  list_price,
  reorder_level,
  target_level,
  quantity_per_unit,
  discontinued_flag,
  minimum_reorder_quantity,
  category,
  supplier_ids,  -- CHANGED from supplier_id to supplier_ids
  product_status,
  margin_percentage,
  price_tier,
  loaded_at
FROM {{ ref('int_products') }}
WHERE product_id IS NOT NULL