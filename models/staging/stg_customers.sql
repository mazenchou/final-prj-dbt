-- models/staging/stg_customers.sql
{{ config(materialized='view') }}

SELECT
  CAST(id AS INT64) as customer_id,
  company as company_name,
  last_name,
  first_name,
  INITCAP(TRIM(CAST(city AS STRING))) as city,
  UPPER(TRIM(CAST(state_province AS STRING))) as state_code,
  TRIM(CAST(zip_postal_code AS STRING)) as zip_code,
  INITCAP(TRIM(CAST(country_region AS STRING))) as country,
  web_page,
  email_address as email,
  job_title,
  CURRENT_TIMESTAMP() as loaded_at
FROM {{ ref('customer') }}