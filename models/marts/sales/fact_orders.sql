-- models/marts/sales/fact_orders.sql (FIXED VERSION)
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
        od.extended_price
    FROM {{ ref('int_order_details') }} od
),

-- FIXED: Join with DIM_DATE to get date_id
orders_with_dates AS (
    SELECT 
        o.order_id,
        o.customer_id,
        o.employee_id,
        o.shipper_id,
        -- Keep original dates (for calculations if needed)
        o.order_date,
        o.shipped_date,
        o.paid_date,
        o.shipping_fee,
        o.taxes,
        o.status_id,
        o.tax_status_id,
        -- DATE IDs FROM DIM_DATE (NOT calculated!)
        od.date_id as order_date_id,
        sd.date_id as shipped_date_id,
        pd.date_id as paid_date_id,
        -- Location from dim_customer
        c.location_id as customer_location_id
    FROM {{ ref('int_orders') }} o
    -- JOIN with DIM_DATE for EACH date column
    LEFT JOIN {{ ref('dim_date') }} od 
      ON DATE(o.order_date) = od.date_day
    LEFT JOIN {{ ref('dim_date') }} sd 
      ON DATE(o.shipped_date) = sd.date_day
    LEFT JOIN {{ ref('dim_date') }} pd 
      ON DATE(o.paid_date) = pd.date_day
    LEFT JOIN {{ ref('dim_customer') }} c 
      ON o.customer_id = c.customer_id
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
    -- Primary Key
    MD5(CONCAT(CAST(od.order_detail_id AS STRING), '|', CAST(od.product_id AS STRING))) as order_line_key,
    
    -- Dimension Foreign Keys
    od.order_id,
    od.product_id,
    o.customer_id,
    o.employee_id,
    o.shipper_id,
    o.customer_location_id,
    
    -- ====== DATE DIMENSION KEYS ======
    o.order_date_id,    -- From DIM_DATE via JOIN
    o.shipped_date_id,  -- From DIM_DATE via JOIN
    o.paid_date_id,     -- From DIM_DATE via JOIN
    
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
    
    -- Timeline Metrics (using original dates)
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
INNER JOIN orders_with_dates o ON od.order_id = o.order_id
LEFT JOIN products p ON od.product_id = p.product_id
WHERE od.order_id IS NOT NULL
  AND od.product_id IS NOT NULL
  AND o.order_date_id IS NOT NULL