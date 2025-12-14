-- models/marts/core/fact_orders.sql
{{ 
    config(
        materialized='table',
        tags=['fact', 'orders', 'fulfillment']
    ) 
}}

WITH order_summary AS (
    SELECT 
        o.order_id,
        o.customer_id,
        o.employee_id,
        o.shipper_id,
        o.order_date,
        o.order_date_id,
        o.shipped_date,
        o.shipped_date_id,
        o.paid_date,
        o.paid_date_id,
        o.shipping_fee,
        o.taxes,
        o.status_id as order_status_id,
        o.tax_status_id,
        
        -- Order Metrics from order details
        SUM(od.quantity) as total_quantity_ordered,
        COUNT(od.order_detail_id) as number_of_items,
        SUM(od.extended_price) as total_order_amount,
        SUM(od.discount_rate * od.unit_price * od.quantity) as total_discount_amount
        
    FROM {{ ref('int_orders') }} o
    LEFT JOIN {{ ref('int_order_details') }} od ON o.order_id = od.order_id
    WHERE o.order_id IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)

SELECT
    order_id,
    
    -- Dimension Foreign Keys
    customer_id,
    employee_id as order_employee_id,
    shipper_id,
    
    -- Date Dimensions
    order_date_id,
    shipped_date_id,
    paid_date_id,
    
    -- ORDER PROCESS FACTS
    total_quantity_ordered,
    number_of_items,
    total_order_amount,
    total_discount_amount,
    shipping_fee,
    taxes,
    total_order_amount + shipping_fee + taxes as grand_total,
    
    -- Fulfillment Timeline Metrics
    DATE_DIFF(shipped_date, order_date, DAY) as days_to_ship,
    DATE_DIFF(paid_date, order_date, DAY) as days_to_payment,
    
    -- Order Status
    order_status_id,
    tax_status_id,
    
    CURRENT_TIMESTAMP() as loaded_at
    
FROM order_summary