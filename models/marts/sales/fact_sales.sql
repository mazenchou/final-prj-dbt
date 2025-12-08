-- CORRECTED fact_sales.sql
{{ config(
    materialized='table',
    tags=['fact', 'sales', 'orders']
) }}

WITH order_details AS (
    SELECT * FROM {{ ref('int_order_details') }}
),
orders AS (
    SELECT * FROM {{ ref('int_orders') }}
)

SELECT
  od.order_detail_id as fact_sales_id,
  od.order_id,
  od.product_id,
  
  -- Get dates from orders table, not order_details
  CAST(FORMAT_DATE('%Y%m%d', o.order_date) AS INT64) as order_date_id,
  CAST(FORMAT_DATE('%Y%m%d', o.shipped_date) AS INT64) as shipped_date_id,
  CAST(FORMAT_DATE('%Y%m%d', o.paid_date) AS INT64) as paid_date_id,
  
  od.quantity,
  od.unit_price,
  od.discount_rate as discount,
  od.net_amount as net_revenue,
  
  -- Other columns
  od.status_id as order_detail_status_id,
  od.date_allocated,
  od.purchase_order_id,
  od.inventory_id,
  
  -- Original dates for reference
  o.order_date,
  o.shipped_date,
  o.paid_date

FROM order_details od
JOIN orders o ON od.order_id = o.order_id
WHERE od.order_id IS NOT NULL
  AND od.product_id IS NOT NULL