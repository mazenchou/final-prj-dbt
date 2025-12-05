{{ config(materialized='view') }}

SELECT
  CAST(id AS INT64) as invoice_id,
  CAST(order_id AS INT64) as order_id,
  PARSE_DATE('%Y-%m-%d', CAST(invoice_date AS STRING)) as invoice_date,
  CASE 
    WHEN CAST(due_date AS STRING) = '' THEN NULL
    ELSE PARSE_DATE('%Y-%m-%d', CAST(due_date AS STRING))
  END as due_date,
  CAST(tax AS FLOAT64) as tax_amount,
  CAST(shipping AS FLOAT64) as shipping_amount,
  CAST(amount_due AS FLOAT64) as amount_due,
  CURRENT_TIMESTAMP() as loaded_at
FROM {{ ref('invoices') }}