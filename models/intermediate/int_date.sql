-- models/intermediate/int_date.sql
{{ config(materialized='table') }}

WITH 

-- Extract ALL dates from your intermediate models
all_dates AS (
  -- From orders
  SELECT order_date as date_day FROM {{ ref('int_orders') }} WHERE order_date IS NOT NULL
  UNION DISTINCT
  SELECT shipped_date FROM {{ ref('int_orders') }} WHERE shipped_date IS NOT NULL
  UNION DISTINCT
  SELECT paid_date FROM {{ ref('int_orders') }} WHERE paid_date IS NOT NULL
  
  -- From purchase orders
  UNION DISTINCT
  SELECT submitted_date FROM {{ ref('int_purchase_orders') }} WHERE submitted_date IS NOT NULL
  UNION DISTINCT
  SELECT creation_date FROM {{ ref('int_purchase_orders') }} WHERE creation_date IS NOT NULL
  UNION DISTINCT
  SELECT expected_date FROM {{ ref('int_purchase_orders') }} WHERE expected_date IS NOT NULL
  UNION DISTINCT
  SELECT payment_date FROM {{ ref('int_purchase_orders') }} WHERE payment_date IS NOT NULL
  UNION DISTINCT
  SELECT approved_date FROM {{ ref('int_purchase_orders') }} WHERE approved_date IS NOT NULL
  
  -- From order details (if has dates)
  UNION DISTINCT
  SELECT date_allocated FROM {{ ref('int_order_details') }} WHERE date_allocated IS NOT NULL
  
  -- From inventory transactions
  UNION DISTINCT
  SELECT DATE(transaction_created_at) FROM {{ ref('int_inventory_transactions') }} WHERE transaction_created_at IS NOT NULL
  UNION DISTINCT
  SELECT DATE(transaction_modified_at) FROM {{ ref('int_inventory_transactions') }} WHERE transaction_modified_at IS NOT NULL
),

-- Get date range bounds
date_bounds AS (
  SELECT 
    MIN(date_day) as min_date,
    MAX(date_day) as max_date
  FROM all_dates
),

-- Create continuous date range
date_range AS (
  SELECT
    date_day
  FROM UNNEST(
    GENERATE_DATE_ARRAY(
      COALESCE((SELECT min_date FROM date_bounds), DATE('2020-01-01')),
      COALESCE((SELECT max_date FROM date_bounds), DATE('2030-12-31')),
      INTERVAL 1 DAY
    )
  ) AS date_day
)

SELECT DISTINCT
  date_day
FROM date_range
WHERE date_day IS NOT NULL
ORDER BY date_day