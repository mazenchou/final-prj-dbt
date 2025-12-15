-- models/marts/purchasing/fact_inventory.sql (CORRECTED)
{{ 
    config(
        materialized='table',
        tags=['fact', 'inventory', 'stock']
    ) 
}}

WITH inventory_base AS (
    SELECT 
        it.inventory_transaction_id,
        it.transaction_type_id,
        it.transaction_created_at,
        it.transaction_modified_at,
        it.product_id,
        it.quantity,
        it.purchase_order_id,
        it.customer_order_id,
        it.comments,
        -- Get standard_cost in the CTE
        p.standard_cost,
        it.quantity * p.standard_cost as transaction_value
    FROM {{ ref('int_inventory_transactions') }} it
    LEFT JOIN {{ ref('dim_product') }} p ON it.product_id = p.product_id  -- Changed to LEFT JOIN
    WHERE it.product_id IS NOT NULL
)

SELECT
    -- Primary Key
    ib.inventory_transaction_id,
    
    -- Dimension Foreign Keys 
    CAST(ib.product_id AS STRING) as product_id_fk,
    CAST(ib.transaction_type_id AS STRING) as transaction_type_id_fk,
    
    -- Date Dimension Keys (handle NULLs)
    COALESCE(
        CAST(FORMAT_DATE('%Y%m%d', DATE(ib.transaction_created_at)) AS INT64),
        19000101
    ) as transaction_created_date_id,
    
    COALESCE(
        CAST(FORMAT_DATE('%Y%m%d', DATE(ib.transaction_modified_at)) AS INT64),
        19000101
    ) as transaction_modified_date_id,
    
    -- Original IDs
    ib.product_id,
    ib.transaction_type_id,
    
    -- Link to other fact tables
    ib.purchase_order_id,
    ib.customer_order_id,
    
    -- INVENTORY FACTS
    ib.quantity,
    ABS(ib.quantity) as absolute_quantity,
    ib.standard_cost,  -- Already from CTE
    ib.transaction_value,  -- Already from CTE
    ABS(ib.transaction_value) as absolute_value,
    
    -- Movement direction
    CASE 
        WHEN ib.quantity > 0 THEN 'Inbound'
        WHEN ib.quantity < 0 THEN 'Outbound'
        ELSE 'Adjustment'
    END as movement_direction,
    
    -- Transaction type name (join only what's needed)
    tt.transaction_type_name,
    
    -- Transaction Details
    ib.comments,
    
    -- Metadata
    CURRENT_TIMESTAMP() as loaded_at
    
FROM inventory_base ib
LEFT JOIN {{ ref('dim_inventory_transaction_type') }} tt 
    ON ib.transaction_type_id = tt.transaction_type_id
WHERE 1=1  --