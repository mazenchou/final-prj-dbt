-- models/marts/purchasing/fact_purchase_orders.sql
{{ 
    config(
        materialized='table',
        tags=['fact', 'purchasing', 'procurement']
    ) 
}}

WITH purchase_data AS (
    SELECT
        pod.purchase_order_detail_id,
        pod.purchase_order_id,
        pod.product_id,
        pod.quantity,
        pod.unit_cost,
        pod.total_cost,
        
        po.supplier_id,
        po.created_by_employee_id,
        po.status_id,
        
        -- Original dates (from int_purchase_orders)
        po.creation_date,
        po.submitted_date,
        po.expected_date,
        po.payment_date,
        po.approved_date,
        
        -- Date IDs from DIM_DATE (via JOIN)
        cd.date_id as creation_date_id,
        sd.date_id as submitted_date_id,
        ed.date_id as expected_date_id,
        pd.date_id as payment_date_id,
        ad.date_id as approved_date_id
        
    FROM {{ ref('int_purchase_order_details') }} pod
    INNER JOIN {{ ref('int_purchase_orders') }} po 
        ON pod.purchase_order_id = po.purchase_order_id
    
    -- JOIN with DIM_DATE for each date column
    LEFT JOIN {{ ref('dim_date') }} cd 
        ON po.creation_date = cd.date_day
    LEFT JOIN {{ ref('dim_date') }} sd 
        ON po.submitted_date = sd.date_day
    LEFT JOIN {{ ref('dim_date') }} ed 
        ON po.expected_date = ed.date_day
    LEFT JOIN {{ ref('dim_date') }} pd 
        ON po.payment_date = pd.date_day
    LEFT JOIN {{ ref('dim_date') }} ad 
        ON po.approved_date = ad.date_day
)

SELECT
    -- Primary Key (Purchase Order Line level)
    purchase_order_detail_id as purchase_line_id,
    
    -- Dimension Foreign Keys
    purchase_order_id,
    product_id,
    supplier_id,
    created_by_employee_id,
    
    -- ====== DATE DIMENSION KEYS (FROM DIM_DATE) ======
    creation_date_id,
    submitted_date_id,
    expected_date_id,
    payment_date_id,
    approved_date_id,
    
    -- Core Measures
    quantity as ordered_quantity,
    unit_cost,
    total_cost as line_total_cost,
    
    -- Calculated Measures
    quantity * unit_cost as calculated_line_total,
    
    -- Status
    status_id as purchase_status_id,
    
    -- Original dates (optional - for debugging/calculations)
    creation_date,
    expected_date,
    
    -- Timelines (calculated using original dates)
    CASE 
        WHEN submitted_date IS NOT NULL 
        THEN DATE_DIFF(submitted_date, creation_date, DAY)
        ELSE NULL
    END as days_to_submission,
    
    CASE 
        WHEN expected_date IS NOT NULL AND creation_date IS NOT NULL
        THEN DATE_DIFF(expected_date, creation_date, DAY)
        ELSE NULL
    END as expected_lead_time,
    
    -- Metadata
    CURRENT_TIMESTAMP() as loaded_at

FROM purchase_data
WHERE product_id IS NOT NULL
  AND supplier_id IS NOT NULL