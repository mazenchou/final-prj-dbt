-- models/intermediate/int_orders.sql
{{ config(materialized='view') }}

with orders as (
    select * from {{ ref('stg_orders') }}
)

select
    order_id,
    employee_id,
    customer_id,
    
    -- Original dates (keep them)
    order_date,
    shipped_date,
    paid_date,
    
    -- ADD DATE KEYS HERE (new)
    CAST(FORMAT_DATE('%Y%m%d', order_date) AS INT64) as order_date_id,
    CAST(FORMAT_DATE('%Y%m%d', shipped_date) AS INT64) as shipped_date_id,
    CAST(FORMAT_DATE('%Y%m%d', paid_date) AS INT64) as paid_date_id,
    
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
    date_diff(
        coalesce(shipped_date, current_date()),
        order_date,
        day
    ) as days_to_ship
    
from orders
where order_id is not null