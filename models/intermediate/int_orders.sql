-- models/intermediate/int_orders.sql (ADD DATE IDs)
{{ config(materialized='view') }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
)

SELECT
    order_id,
    employee_id,
    customer_id,
    
    -- Original dates (keep as DATE)
    DATE(order_date) as order_date,
    DATE(shipped_date) as shipped_date,
    DATE(paid_date) as paid_date,
    
    -- ADD DATE IDs HERE (NEW!)
    CAST(FORMAT_DATE('%Y%m%d', DATE(order_date)) AS INT64) as order_date_id,
    CAST(FORMAT_DATE('%Y%m%d', DATE(shipped_date)) AS INT64) as shipped_date_id,
    CAST(FORMAT_DATE('%Y%m%d', DATE(paid_date)) AS INT64) as paid_date_id,
    
    -- Rest of columns
    shipper_id,
    ship_name,
    ship_address,
    ship_city,
    ship_state_province,
    ship_zip_code,
    ship_country,
    shipping_fee,
    taxes,
    payment_type,
    notes,
    tax_rate,
    tax_status_id,
    status_id,
    loaded_at,
    
    -- Business logic
    DATE_DIFF(
        COALESCE(DATE(shipped_date), CURRENT_DATE()),
        DATE(order_date),
        DAY
    ) as days_to_ship
    
FROM orders
WHERE order_id IS NOT NULL
  AND order_date IS NOT NULL