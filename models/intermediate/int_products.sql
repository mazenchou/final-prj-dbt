-- models/intermediate/int_products.sql
{{ config(materialized='view') }}

with products as (
    select * from {{ ref('stg_products') }}
)

select
    supplier_ids,  -- ← Added this column
    product_id,
    product_code,
    product_name,
    description,
    standard_cost,
    list_price,
    reorder_level,
    target_level,
    quantity_per_unit,
    discontinued_flag,  -- ← Changed from discontinued
    minimum_reorder_quantity,
    category,
    attachments,
    product_status,  -- ← Added this column
    loaded_at,
    case
        when standard_cost > 0 then 
            round((list_price - standard_cost) / standard_cost * 100, 2)
        else null
    end as margin_percentage,
    case
        when list_price < 10 then 'Budget'
        when list_price between 10 and 50 then 'Mid-range'
        when list_price > 50 then 'Premium'
        else 'Unknown'
    end as price_tier
from products
where product_id is not null