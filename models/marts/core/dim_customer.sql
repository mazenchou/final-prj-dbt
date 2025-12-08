-- models/marts/core/dim_customer.sql
{{ config(
    materialized='table',
    tags=['dimension', 'customer']
) }}

SELECT
  customer_id,
  company_name,
  contact_name,
  city,
  email,
  job_title,
  -- REMOVED non-existent phone/address columns
  loaded_at
FROM {{ ref('int_customers') }}
WHERE customer_id IS NOT NULL