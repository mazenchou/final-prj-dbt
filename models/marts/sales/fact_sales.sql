-- models/marts/core/fact_sales.sql
{{ 
    config(
        materialized='table',
        tags=['fact', 'sales', 'revenue']
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
        od.date_allocated,
        od.date_allocated_id,
        od.extended_price as net_amount,
        o.customer_id,
        o.employee_id,
        o.order_date,
        o.order_date_id,
        o.shipped_date,
        o.shipped_date_id,
        o.paid_date,
        o.paid_date_id,
        o.shipper_id,
        o.shipping_fee,
        o.taxes,
        o.status_id as order_status_id,
        o.tax_rate,
        p.standard_cost,
        p.list_price,
        (od.quantity * od.unit_price) as gross_amount,
        (od.quantity * od.unit_price * COALESCE(od.discount_rate, 0)) as discount_amount,
        (od.quantity * od.unit_price) * COALESCE(o.tax_rate, 0) as calculated_tax
    FROM {{ ref('int_order_details') }} od
    JOIN {{ ref('int_orders') }} o ON od.order_id = o.order_id
    JOIN {{ ref('int_products') }} p ON od.product_id = p.product_id
    WHERE od.order_id IS NOT NULL
      AND o.customer_id IS NOT NULL
      AND od.product_id IS NOT NULL
)

SELECT
    -- Primary Key
    order_detail_id as sales_id,
    
    -- Dimension Foreign Keys (from your actual dimensions)
    customer_id,
    product_id,
    employee_id as sales_employee_id,
    shipper_id,
    
    -- Date Dimensions
    order_date_id,
    shipped_date_id,
    paid_date_id,
    date_allocated_id,
    
    -- SALES FACTS (Based on your actual data)
    quantity as quantity_sold,
    unit_price,
    discount_rate as discount_percentage,
    discount_amount,
    gross_amount,
    net_amount,
    COALESCE(taxes, calculated_tax) as tax_amount,
    shipping_fee,
    standard_cost,
    list_price,
    
    -- Calculated Profit Metrics
    (gross_amount - discount_amount) - (quantity * standard_cost) as gross_profit,
    ROUND(
        ((gross_amount - discount_amount) - (quantity * standard_cost)) / 
        NULLIF((gross_amount - discount_amount), 0) * 100, 
        2
    ) as profit_margin_percentage,
    
    -- Status
    order_status_id,
    
    CURRENT_TIMESTAMP() as loaded_at
    
FROM order_details