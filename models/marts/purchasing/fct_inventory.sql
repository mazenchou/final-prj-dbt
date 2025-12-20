-- models/marts/inventory/fact_inventory_transactions.sql (FIXED WITH DIMENSION)
{{ 
    config(
        materialized='table',
        tags=['fact', 'inventory', 'stock']
    ) 
}}

WITH inventory_data AS (
    SELECT 
        it.inventory_transaction_id,
        it.transaction_type_id,
        it.product_id,
        it.quantity,
        it.purchase_order_id,
        it.customer_order_id,
        it.comments,
        
        -- Original dates
        it.transaction_created_at,
        it.transaction_modified_at,
        
        -- Date IDs from DIM_DATE
        cd.date_id as transaction_created_date_id,
        md.date_id as transaction_modified_date_id,
        
        -- Get product cost
        p.standard_cost,
        it.quantity * p.standard_cost as transaction_value,
        
        -- Get transaction type details FROM DIMENSION
        tt.transaction_type_name  -- ← ADDED HERE
        
    FROM {{ ref('int_inventory_transactions') }} it
    
    -- JOIN with DIM_DATE for date columns
    LEFT JOIN {{ ref('dim_date') }} cd 
        ON DATE(it.transaction_created_at) = cd.date_day
    LEFT JOIN {{ ref('dim_date') }} md 
        ON DATE(it.transaction_modified_at) = md.date_day
    
    -- Get product info
    LEFT JOIN {{ ref('dim_product') }} p 
        ON it.product_id = p.product_id
    
    -- GET TRANSACTION TYPE DETAILS FROM DIMENSION ← ADDED
    LEFT JOIN {{ ref('dim_inventory_transaction_type') }} tt 
        ON it.transaction_type_id = tt.transaction_type_id
    
    WHERE it.product_id IS NOT NULL
)

SELECT
    -- Primary Key
    inventory_transaction_id,
    
    -- Dimension Foreign Keys
    product_id,
    transaction_type_id,
    
    -- ====== DATE DIMENSION KEYS ======
    transaction_created_date_id,
    transaction_modified_date_id,
    
    -- ====== DIMENSION ATTRIBUTES ======
    transaction_type_name,  -- From dim_inventory_transaction_type
    
    -- Link to other fact tables
    purchase_order_id,
    customer_order_id,
    
    -- INVENTORY FACTS
    quantity,
    ABS(quantity) as absolute_quantity,
    standard_cost,
    transaction_value,
    ABS(transaction_value) as absolute_value,
    
    -- Movement direction
    CASE 
        WHEN quantity > 0 THEN 'Inbound'
        WHEN quantity < 0 THEN 'Outbound'
        ELSE 'Adjustment'
    END as movement_direction,
    
    -- Transaction Details
    comments,
    
    -- Metadata
    CURRENT_TIMESTAMP() as loaded_at
    
FROM inventory_data