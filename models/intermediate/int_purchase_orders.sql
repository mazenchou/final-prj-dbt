-- models/intermediate/int_purchase_orders.sql
{{ config(materialized='view') }}

with purchase_orders as (
    select * from {{ ref('stg_purchase_orders') }}
)

select
    purchase_order_id,
    supplier_id,
    created_by_employee_id,
    
    -- Original dates (keep them)
    submitted_date,
    creation_date,
    expected_date,
    payment_date,
    approved_date,
    
    -- ADD DATE KEYS HERE (new)
    CAST(FORMAT_DATE('%Y%m%d', submitted_date) AS INT64) as submitted_date_id,
    CAST(FORMAT_DATE('%Y%m%d', creation_date) AS INT64) as creation_date_id,
    CAST(FORMAT_DATE('%Y%m%d', expected_date) AS INT64) as expected_date_id,
    CAST(FORMAT_DATE('%Y%m%d', payment_date) AS INT64) as payment_date_id,
    CAST(FORMAT_DATE('%Y%m%d', approved_date) AS INT64) as approved_date_id,
    
    -- Rest of columns
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