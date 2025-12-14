-- models/marts/core/fact_purchases.sql
{{ 
    config(
        materialized='table',
        tags=['fact', 'purchases', 'procurement', 'cost']
    ) 
}}

SELECT
    -- Primary Key
    purchase_order_id,
    
    -- Dimension Foreign Keys
    supplier_id,
    created_by_employee_id as purchasing_employee_id,
    approved_by_employee_id,
    
    -- Date Dimensions (from your int_purchase_orders)
    submitted_date_id,
    creation_date_id,
    expected_date_id,
    payment_date_id,
    approved_date_id,
    
    -- PURCHASE FACTS (Based on your actual data)
    shipping_fee,
    taxes as purchase_taxes,
    payment_amount,
    total_amount as total_purchase_cost,
    
    -- Payment Information
    payment_method,
    payment_status,
    
    -- Process Metrics (from your calculations)
    days_to_approval,
    days_to_expected_delivery,
    
    -- Status
    status_id as purchase_status_id,
    
    -- Notes (for analysis)
    notes,
    
    CURRENT_TIMESTAMP() as loaded_at
    
FROM {{ ref('int_purchase_orders') }}
WHERE supplier_id IS NOT NULL
  AND purchase_order_id IS NOT NULL