-- models/marts/sales/fact_orders.sql (CORRECTED)
{{ 
    config(
        materialized='table',
        tags=['fact', 'sales', 'orders']
    ) 
}}

WITH order_details AS (
    SELECT 
        od.order_detail_id,
        od.order_id,
        od.product_id,
        od.quantity,
        od.unit_price,
        od.discount_rate,
        od.extended_price,
        od.date_allocated
    FROM {{ ref('int_order_details') }} od
),
-- In your fact_orders.sql orders CTE:
orders AS (
    SELECT 
        o.order_id,
        o.customer_id,
        o.employee_id,
        o.shipper_id,
        o.order_date,
        o.shipped_date,
        o.paid_date,
        o.shipping_fee,
        o.taxes,
        o.status_id,
        o.tax_status_id,
        -- DATE IDs
        CAST(FORMAT_DATE('%Y%m%d', o.order_date) AS INT64) as order_date_id,
        CAST(FORMAT_DATE('%Y%m%d', o.shipped_date) AS INT64) as shipped_date_id,
        CAST(FORMAT_DATE('%Y%m%d', o.paid_date) AS INT64) as paid_date_id,
        -- Just use location_id directly (it's already STRING)
        c.location_id as customer_location_id  -- REMOVED TO_HEX()
    FROM {{ ref('int_orders') }} o
    LEFT JOIN {{ ref('dim_customer') }} c ON o.customer_id = c.customer_id
    WHERE o.customer_id IS NOT NULL
      AND o.order_date IS NOT NULL
),
products AS (
    SELECT 
        product_id,
        standard_cost,
        list_price,
        category
    FROM {{ ref('dim_product') }}
)

SELECT
    -- Primary Key (LINE ITEM level)
    MD5(CONCAT(CAST(od.order_detail_id AS STRING), '|', CAST(od.product_id AS STRING))) as order_line_key,
    
    -- Dimension Foreign Keys (STAR SCHEMA)
    od.order_id,
    od.product_id,
    o.customer_id,
    o.employee_id,
    o.shipper_id,
    o.customer_location_id,
    
    -- ====== DATE DIMENSION KEYS (FIXED) ======
    -- Use date_id FROM int_orders (already calculated)
    o.order_date_id,
    o.shipped_date_id,
    o.paid_date_id,
    
    -- SALES MEASURES
    od.quantity,
    od.unit_price,
    od.discount_rate,
    
    -- Amounts
    od.quantity * od.unit_price as gross_amount,
    od.quantity * od.unit_price * (1 - od.discount_rate) as net_amount,
    (od.quantity * od.unit_price * od.discount_rate) as discount_amount,
    
    -- Cost & Profit
    od.quantity * p.standard_cost as total_cost,
    (od.quantity * od.unit_price * (1 - od.discount_rate)) - (od.quantity * p.standard_cost) as gross_profit,
    
    -- Profit Margin
    CASE 
        WHEN od.quantity * od.unit_price * (1 - od.discount_rate) > 0
        THEN ROUND(((od.quantity * od.unit_price * (1 - od.discount_rate)) - (od.quantity * p.standard_cost)) / 
                   (od.quantity * od.unit_price * (1 - od.discount_rate)) * 100, 2)
        ELSE 0
    END as profit_margin_percentage,
    
    -- Shipping & Taxes (prorated)
    o.shipping_fee / COUNT(od.order_id) OVER(PARTITION BY od.order_id) as prorated_shipping,
    o.taxes / COUNT(od.order_id) OVER(PARTITION BY od.order_id) as prorated_tax,
    
    -- Timeline Metrics
    DATE_DIFF(o.shipped_date, o.order_date, DAY) as days_to_ship,
    DATE_DIFF(o.paid_date, o.order_date, DAY) as days_to_payment,
    
    -- Product Info
    p.standard_cost,
    p.list_price,
    p.category,
    
    -- Status
    o.status_id as order_status_id,
    o.tax_status_id,
    
    -- Metadata
    CURRENT_TIMESTAMP() as dw_created_at

FROM order_details od
INNER JOIN orders o ON od.order_id = o.order_id
LEFT JOIN products p ON od.product_id = p.product_id
WHERE od.order_id IS NOT NULL
  AND od.product_id IS NOT NULL
  AND o.order_date_id IS NOT NULL  -- Ensure date connection