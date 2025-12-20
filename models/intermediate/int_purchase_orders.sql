-- models/intermediate/int_purchase_orders.sql (FIXED - NO DATE IDs)
{{ config(materialized='view') }}

WITH purchase_orders AS (
    SELECT * FROM {{ ref('stg_purchase_orders') }}
)

SELECT
    purchase_order_id,
    supplier_id,
    created_by_employee_id,
    
    -- Convert TIMESTAMP to DATE
    DATE(submitted_date) as submitted_date,
    DATE(creation_date) as creation_date,
    DATE(expected_date) as expected_date,
    DATE(payment_date) as payment_date,
    DATE(approved_date) as approved_date,
    -- Rest of columns (unchanged)
    status_id,
    shipping_fee,
    taxes,
    payment_amount,
    payment_method,
    notes,
    approved_by_employee_id,
    submitted_by_employee_id,
    loaded_at,
    
    -- Business logic
    coalesce(payment_amount, 0) + 
    coalesce(shipping_fee, 0) + 
    coalesce(taxes, 0) as total_amount,
    
    case 
        when payment_date is not null then 'Paid'
        when payment_date is null and payment_amount > 0 then 'Pending'
        else 'Not Paid'
    end as payment_status,
    
    case 
        when submitted_date is not null and approved_date is not null 
        then date_diff(approved_date, submitted_date, day)
        else null
    end as days_to_approval,
    
    case 
        when creation_date is not null and expected_date is not null 
        then date_diff(expected_date, creation_date, day)
        else null
    end as days_to_expected_delivery

from purchase_orders
where purchase_order_id is not null