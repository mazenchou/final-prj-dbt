-- models/intermediate/int_inventory_transactions.sql (FIXED)
{{ config(materialized='view') }}

SELECT
    inventory_transaction_id,
    transaction_type_id,
    
    -- Keep as DATETIME/TIMESTAMP
    transaction_created_at,
    transaction_modified_at,
    
    -- NO DATE ID CALCULATION HERE!
    -- Just convert to DATE if needed for joins
    DATE(transaction_created_at) as transaction_created_date,
    DATE(transaction_modified_at) as transaction_modified_date,
    
    product_id,
    quantity,
    purchase_order_id,
    customer_order_id,
    comments,
    loaded_at
    
FROM {{ ref('stg_inventory_transactions') }}
WHERE inventory_transaction_id IS NOT NULL