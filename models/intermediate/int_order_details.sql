-- models/intermediate/int_order_details.sql
{{ config(materialized='view') }}

with order_details as (
    select * from {{ ref('stg_order_details') }}
)

select
    order_detail_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    discount_rate,
    status_id,
    
    -- Original date (keep it)
    date_allocated,
    
    -- ADD DATE KEY HERE (if date_allocated exists)
    CAST(FORMAT_DATE('%Y%m%d', date_allocated) AS INT64) as date_allocated_id,
    
    purchase_order_id,
    inventory_id,
    net_amount,
    loaded_at,
    net_amount as extended_price
    
from order_details
where order_id is not null
  and product_id is not null