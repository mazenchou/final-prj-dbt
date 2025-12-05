{{ config(materialized='view') }}

SELECT
  TRIM(CAST(supplier_ids AS STRING)) as supplier_ids,
  CAST(id AS INT64) as product_id,
  TRIM(CAST(product_code AS STRING)) as product_code,
  TRIM(CAST(product_name AS STRING)) as product_name,
  TRIM(CAST(description AS STRING)) as description,
  CAST(standard_cost AS FLOAT64) as standard_cost,
  CAST(list_price AS FLOAT64) as list_price,
  CAST(reorder_level AS INT64) as reorder_level,
  CAST(target_level AS INT64) as target_level,
  TRIM(CAST(quantity_per_unit AS STRING)) as quantity_per_unit,
  CAST(discontinued AS INT64) as discontinued_flag,
  CAST(minimum_reorder_quantity AS INT64) as minimum_reorder_quantity,
  TRIM(CAST(category AS STRING)) as category,
  attachments,
  CASE 
    WHEN CAST(discontinued AS INT64) = 1 THEN 'Discontinued'
    ELSE 'Active'
  END as product_status,
  CURRENT_TIMESTAMP() as loaded_at
FROM {{ ref('products') }}