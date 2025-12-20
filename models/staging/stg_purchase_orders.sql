-- models/staging/stg_purchase_orders.sql (HANDLES MIXED TYPES)
{{ config(materialized='view') }}

SELECT
  CAST(id AS INT64) as purchase_order_id,
  CAST(supplier_id AS INT64) as supplier_id,
  CAST(created_by AS INT64) as created_by_employee_id,
  
  -- Handle BOTH string and integer dates
  CASE 
    WHEN submitted_date IS NOT NULL 
    THEN COALESCE(
      -- Try as STRING first (MM/DD/YYYY HH:MM:SS)
      SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', 
        TRIM(SAFE_CAST(submitted_date AS STRING))),
      -- Try as INTEGER (YYYYMMDD)
      SAFE.PARSE_TIMESTAMP('%Y%m%d', 
        CONCAT(SAFE_CAST(submitted_date AS STRING), ' 00:00:00'))
    )
    ELSE NULL
  END as submitted_date,
  
  CASE 
    WHEN creation_date IS NOT NULL 
    THEN COALESCE(
      SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', 
        TRIM(SAFE_CAST(creation_date AS STRING))),
      SAFE.PARSE_TIMESTAMP('%Y%m%d', 
        CONCAT(SAFE_CAST(creation_date AS STRING), ' 00:00:00'))
    )
    ELSE NULL
  END as creation_date,
  
  CAST(status_id AS INT64) as status_id,
  
  CASE 
    WHEN expected_date IS NOT NULL 
    THEN COALESCE(
      SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', 
        TRIM(SAFE_CAST(expected_date AS STRING))),
      SAFE.PARSE_TIMESTAMP('%Y%m%d', 
        CONCAT(SAFE_CAST(expected_date AS STRING), ' 00:00:00'))
    )
    ELSE NULL
  END as expected_date,
  
  SAFE_CAST(shipping_fee AS NUMERIC) as shipping_fee,
  SAFE_CAST(taxes AS NUMERIC) as taxes,
  
  CASE 
    WHEN payment_date IS NOT NULL 
    THEN COALESCE(
      SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', 
        TRIM(SAFE_CAST(payment_date AS STRING))),
      SAFE.PARSE_TIMESTAMP('%Y%m%d', 
        CONCAT(SAFE_CAST(payment_date AS STRING), ' 00:00:00'))
    )
    ELSE NULL 
  END as payment_date,
  
  SAFE_CAST(payment_amount AS NUMERIC) as payment_amount,
  TRIM(CAST(payment_method AS STRING)) as payment_method,
  TRIM(CAST(notes AS STRING)) as notes,
  CAST(approved_by AS INT64) as approved_by_employee_id,
  
  CASE 
    WHEN approved_date IS NOT NULL 
    THEN COALESCE(
      SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', 
        TRIM(SAFE_CAST(approved_date AS STRING))),
      SAFE.PARSE_TIMESTAMP('%Y%m%d', 
        CONCAT(SAFE_CAST(approved_date AS STRING), ' 00:00:00'))
    )
    ELSE NULL 
  END as approved_date,
  
  CAST(submitted_by AS INT64) as submitted_by_employee_id,
  CURRENT_TIMESTAMP() as loaded_at
FROM {{ source('raw_data', 'purchase_orders') }}