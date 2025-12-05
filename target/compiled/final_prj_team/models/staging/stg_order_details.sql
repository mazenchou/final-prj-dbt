

SELECT
  CAST(id AS INT64) as order_detail_id,
  CAST(order_id AS INT64) as order_id,
  CAST(product_id AS INT64) as product_id,
  CAST(quantity AS INT64) as quantity,
  CAST(unit_price AS FLOAT64) as unit_price,
  CAST(discount AS FLOAT64) as discount_rate,
  CAST(status_id AS INT64) as status_id,
  CASE 
    WHEN CAST(date_allocated AS STRING) = '' THEN NULL
    ELSE PARSE_DATE('%Y-%m-%d', CAST(date_allocated AS STRING))
  END as date_allocated,
  CAST(purchase_order_id AS INT64) as purchase_order_id,
  CAST(inventory_id AS INT64) as inventory_id,
  CAST(unit_price * quantity * (1 - COALESCE(discount, 0)) AS FLOAT64) as net_amount,
  CURRENT_TIMESTAMP() as loaded_at
FROM `final-prj-480309`.`dbt_dev`.`order_details`