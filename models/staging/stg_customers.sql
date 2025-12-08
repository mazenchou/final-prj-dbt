-- models/staging/stg_customers.sql
{{ config(materialized='view') }}

SELECT
  CAST(id AS INT64) as customer_id,
  company as company_name,
  last_name,
  first_name,
  city,
  email_address as email,
  job_title,
  CURRENT_TIMESTAMP() as loaded_at
FROM {{ ref('customer') }}