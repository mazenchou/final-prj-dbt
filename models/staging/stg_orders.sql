-- models/staging/stg_orders.sql (FIXED - no loaded_at)
{{ config(materialized='view') }}

SELECT
  CAST(id AS INT64) as order_id,
  CAST(employee_id AS INT64) as employee_id,
  CAST(customer_id AS INT64) as customer_id,
  
  -- Parse MM/DD/YYYY HH:MM:SS dates
  CASE 
    WHEN order_date IS NOT NULL AND order_date != ''
    THEN SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', CAST(order_date AS STRING))
    ELSE NULL
  END as order_date,
  
  -- Parse shipped_date (may be empty)
  CASE 
    WHEN shipped_date IS NOT NULL AND shipped_date != ''
    THEN SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', CAST(shipped_date AS STRING))
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
  
  -- Parse paid_date
  CASE 
    WHEN paid_date IS NOT NULL AND paid_date != ''
    THEN SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', CAST(paid_date AS STRING))
    ELSE NULL
  END as paid_date,
  
  TRIM(CAST(notes AS STRING)) as notes,
  CAST(tax_rate AS FLOAT64) as tax_rate,
  CAST(tax_status_id AS INT64) as tax_status_id,
  CAST(status_id AS INT64) as status_id,
  
  -- Add loaded_at timestamp (since source doesn't have it)
  CURRENT_TIMESTAMP() as loaded_at
  
FROM {{ source('raw_data', 'orders') }}
WHERE id IS NOT NULL
  AND customer_id IS NOT NULL