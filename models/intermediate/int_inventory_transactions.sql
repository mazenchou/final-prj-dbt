-- models/intermediate/int_inventory_transactions.sql
{{ config(materialized='view') }}

with inventory_transactions as (
    select * from {{ ref('stg_inventory_transactions') }}
)

select
    inventory_transaction_id,
    transaction_type_id,
    
    -- Original dates (keep them)
    transaction_created_at,
    transaction_modified_at,
    
    -- ADD DATE KEYS HERE
    CAST(FORMAT_DATE('%Y%m%d', DATE(transaction_created_at)) AS INT64) as transaction_created_date_id,
    CAST(FORMAT_DATE('%Y%m%d', DATE(transaction_modified_at)) AS INT64) as transaction_modified_date_id,
    
    product_id,
    quantity,
    purchase_order_id,
    customer_order_id,
    comments,
    loaded_at
    
from inventory_transactions
where inventory_transaction_id is not null