-- models/intermediate/int_purchase_order_details.sql
{{ config(materialized='view') }}

WITH purchase_details AS (
    SELECT * FROM {{ ref('stg_purchase_order_details') }}
)

SELECT
    purchase_order_detail_id,
    purchase_order_id,
    product_id,
    quantity,
    unit_cost,
    date_received,
    posted_to_inventory_flag,
    inventory_id,
    total_cost,
    
    -- Add date_id for dim_date relationship
    CAST(FORMAT_DATE('%Y%m%d', date_received) AS INT64) as date_received_id,
    
    -- Receipt status
    CASE 
        WHEN date_received IS NOT NULL THEN 'Received'
        ELSE 'Pending'
    END as receipt_status,
    
    -- Inventory status
    CASE 
        WHEN posted_to_inventory_flag = 1 THEN 'Posted to Inventory'
        ELSE 'Not Posted'
    END as inventory_status,
    
    -- Calculate received quantity (if partially received)
    quantity as ordered_quantity,
    quantity as received_quantity,  -- Assuming full receipt if date_received exists
    
    loaded_at
    
FROM purchase_details
WHERE purchase_order_id IS NOT NULL
  AND product_id IS NOT NULL
  AND quantity > 0