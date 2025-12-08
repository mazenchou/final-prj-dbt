-- models/marts/purchasing/fact_purchase_orders.sql
{{ config(
    materialized='table',
    tags=['fact', 'purchasing', 'purchase']
) }}

SELECT
  purchase_order_id as fact_purchase_id,
  
  -- Foreign keys
  supplier_id,
  CAST(FORMAT_DATE('%Y%m%d', submitted_date) AS INT64) as submitted_date_id,
  CAST(FORMAT_DATE('%Y%m%d', creation_date) AS INT64) as creation_date_id,
  CAST(FORMAT_DATE('%Y%m%d', approved_date) AS INT64) as approved_date_id,
  CAST(FORMAT_DATE('%Y%m%d', expected_date) AS INT64) as expected_date_id,
  CAST(FORMAT_DATE('%Y%m%d', payment_date) AS INT64) as payment_date_id,

  -- Measures
  payment_amount,
  shipping_fee,
  taxes,
  total_amount,
  
  -- Status
  status_id as purchase_order_status_id,
  payment_status,
  
  -- Delays
  days_to_approval,
  days_to_expected_delivery,
  
  -- Payment info
  payment_method,
  
  -- Notes
  notes,
  
  -- Original dates (for reference)
  submitted_date,
  creation_date,
  approved_date,
  expected_date,
  payment_date

FROM {{ ref('int_purchase_orders') }}
WHERE purchase_order_id IS NOT NULL