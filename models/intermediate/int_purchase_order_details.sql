-- models/intermediate/int_purchase_orders.sql
{{ config(materialized='view') }}

with purchase_orders as (
    select * from {{ ref('stg_purchase_orders') }}
)

select
    purchase_order_id,
    supplier_id,
    created_by_employee_id,
    submitted_date,
    creation_date,
    status_id,
    expected_date,
    shipping_fee,
    taxes,
    payment_date,
    payment_amount,
    payment_method,
    notes,
    approved_by_employee_id,
    approved_date,
    submitted_by_employee_id,
    loaded_at,
    -- Calculate total amount
    coalesce(payment_amount, 0) + coalesce(shipping_fee, 0) + coalesce(taxes, 0) as total_amount
from purchase_orders
where purchase_order_id is not null