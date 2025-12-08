-- models/staging/stg_orders.sql (FIXED for YYYY/MM/DD)
{{ config(materialized='view') }}

SELECT
  CAST(id AS INT64) as order_id,
  CAST(employee_id AS INT64) as employee_id,
  CAST(customer_id AS INT64) as customer_id,
  
  -- Handle YYYY/MM/DD format with slashes
  CASE 
    WHEN order_date IS NOT NULL AND order_date != ''
    THEN SAFE.PARSE_DATE('%Y/%m/%d', CAST(order_date AS STRING))
    ELSE NULL
  END as order_date,
  
  CASE 
    WHEN shipped_date IS NOT NULL AND shipped_date != ''
    THEN SAFE.PARSE_DATE('%Y/%m/%d', CAST(shipped_date AS STRING))
    ELSE NULL
  END as shipped_date,
  
  CAST(shipper_id AS INT64) as shipper_id,
  TRIM(CAST(ship_name AS STRING)) as ship_name,
  TRIM(CAST(ship_address AS STRING)) as ship_address,
  TRIM(CAST(ship_city AS STRING)) as ship_city,
  TRIM(CAST(ship_state_province AS STRING)) as ship_state_province,
  TRIM(CAST(ship_zip_postal_code AS STRING)) as ship_zip_code,
  TRIM(CAST(ship_country_region AS STRING)) as ship_country,
  CAST(shipping_fee AS FLOAT64) as shipping_fee,
  CAST(taxes AS FLOAT64) as taxes,
  TRIM(CAST(payment_type AS STRING)) as payment_type,
  
  CASE 
    WHEN paid_date IS NOT NULL AND paid_date != ''
    THEN SAFE.PARSE_DATE('%Y/%m/%d', CAST(paid_date AS STRING))
    ELSE NULL
  END as paid_date,
  
  TRIM(CAST(notes AS STRING)) as notes,
  CAST(tax_rate AS FLOAT64) as tax_rate,
  CAST(tax_status_id AS INT64) as tax_status_id,
  CAST(status_id AS INT64) as status_id,
  CURRENT_TIMESTAMP() as loaded_at
FROM {{ ref('orders') }}  -- Changed from ref('orders') to source('raw', 'orders')