-- models/marts/core/fact_inventory.sql
{{ 
    config(
        materialized='table',
        tags=['fact', 'inventory', 'stock']
    ) 
}}

WITH inventory_with_costs AS (
    SELECT 
        it.inventory_transaction_id,
        it.transaction_type_id,
        it.transaction_created_at,
        it.transaction_created_date_id,
        it.transaction_modified_at,
        it.transaction_modified_date_id,
        it.product_id,
        it.quantity,
        it.purchase_order_id,
        it.customer_order_id,
        it.comments,
        p.standard_cost,
        it.quantity * p.standard_cost as transaction_value,
        
        -- Determine if it's inbound or outbound
        CASE 
            WHEN it.quantity > 0 THEN 'Inbound'
            WHEN it.quantity < 0 THEN 'Outbound'
            ELSE 'Adjustment'
        END as movement_direction
        
    FROM {{ ref('int_inventory_transactions') }} it
    LEFT JOIN {{ ref('int_products') }} p ON it.product_id = p.product_id
    WHERE it.product_id IS NOT NULL
)

SELECT
    inventory_transaction_id,
    
    -- Dimension Foreign Keys
    product_id,
    transaction_type_id,
    
    -- Link to other facts (if available)
    purchase_order_id,
    customer_order_id,
    
    -- Date Dimensions
    transaction_created_date_id,
    transaction_modified_date_id,
    
    -- INVENTORY FACTS
    quantity,
    ABS(quantity) as absolute_quantity,
    standard_cost,
    transaction_value,
    ABS(transaction_value) as absolute_value,
    movement_direction,
    
    -- Transaction Details
    comments,
    
    CURRENT_TIMESTAMP() as loaded_at
    
FROM inventory_with_costs
WHERE quantity != 0