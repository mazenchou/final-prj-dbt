-- models/staging/stg_customers.sql
{{ config(materialized='view') }}

WITH customer_base AS (
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
    job_title
  FROM {{ ref('customer') }}
),
ranked_customers AS (
  SELECT
    customer_id,
    company_name,
    last_name,
    first_name,
    city,
    state_code,
    zip_code,
    country,
    web_page,
    email,
    job_title,
    CURRENT_TIMESTAMP() as loaded_at,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id) as rn
  FROM customer_base
)
SELECT
  customer_id,
  company_name,
  last_name,
  first_name,
  city,
  state_code,
  zip_code,
  country,
  web_page,
  email,
  job_title,
  loaded_at
FROM ranked_customers
WHERE rn = 1