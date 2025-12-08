-- models/marts/sales/fact_orders.sql
{{ config(
    materialized='table',
    tags=['fact', 'sales', 'orders']
) }}

SELECT
  order_id,
  
  -- Foreign keys
  customer_id,
  employee_id,
  shipper_id,
  CAST(FORMAT_DATE('%Y%m%d', order_date) AS INT64) as order_date_id,
  CAST(FORMAT_DATE('%Y%m%d', shipped_date) AS INT64) as shipped_date_id,
  CAST(FORMAT_DATE('%Y%m%d', paid_date) AS INT64) as paid_date_id,

  -- Measures
  shipping_fee,
  taxes,
  tax_rate,
  
  -- Payment info
  payment_type,
  
  -- Status
  status_id as order_status_id,
  tax_status_id,
  
  -- Shipping info
  ship_name,
  ship_address,
  ship_city,
  ship_state_province,
  ship_zip_code,
  ship_country,
  
  -- Business metrics
  days_to_ship,
  
  -- Notes
  notes,
  
  -- Original dates (for reference)
  order_date,
  shipped_date,
  paid_date

FROM {{ ref('int_orders') }}
WHERE order_id IS NOT NULL