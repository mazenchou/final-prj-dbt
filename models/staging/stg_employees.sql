{{ config(materialized='view') }}

SELECT
  CAST(id AS INT64) as employee_id,
  TRIM(CAST(company AS STRING)) as company_name,
  TRIM(CAST(last_name AS STRING)) as last_name,
  TRIM(CAST(first_name AS STRING)) as first_name,
  LOWER(TRIM(CAST(email_address AS STRING))) as email,
  TRIM(CAST(job_title AS STRING)) as job_title,
  business_phone,
  home_phone,
  mobile_phone,
  fax_number,
  TRIM(CAST(address AS STRING)) as address,
  INITCAP(TRIM(CAST(city AS STRING))) as city,
  UPPER(TRIM(CAST(state_province AS STRING))) as state_code,
  TRIM(CAST(zip_postal_code AS STRING)) as zip_code,
  INITCAP(TRIM(CAST(country_region AS STRING))) as country,
  web_page,
  notes,
  attachments,
  CONCAT(TRIM(CAST(first_name AS STRING)), ' ', TRIM(CAST(last_name AS STRING))) as full_name,
  CURRENT_TIMESTAMP() as loaded_at
FROM {{ ref('employees') }}