-- models/staging/stg_suppliers.sql
{{ config(materialized='view') }}

SELECT
  CAST(id AS INT64) as supplier_id,
  TRIM(CAST(company AS STRING)) as company_name,
  TRIM(CAST(last_name AS STRING)) as last_name,
  TRIM(CAST(first_name AS STRING)) as first_name,
  LOWER(TRIM(CAST(email_address AS STRING))) as email,
  TRIM(CAST(job_title AS STRING)) as job_title,
  TRIM(CAST(business_phone AS STRING)) as business_phone,
  TRIM(CAST(home_phone AS STRING)) as home_phone,
  TRIM(CAST(mobile_phone AS STRING)) as mobile_phone,
  TRIM(CAST(fax_number AS STRING)) as fax_number,
  TRIM(CAST(address AS STRING)) as address,
  INITCAP(TRIM(CAST(city AS STRING))) as city,
  UPPER(TRIM(CAST(state_province AS STRING))) as state_code,
  TRIM(CAST(zip_postal_code AS STRING)) as zip_code,
  INITCAP(TRIM(CAST(country_region AS STRING))) as country,
  TRIM(CAST(web_page AS STRING)) as website,
  TRIM(CAST(notes AS STRING)) as notes,
  TRIM(CAST(attachments AS STRING)) as attachments,
  CONCAT(TRIM(CAST(first_name AS STRING)), ' ', TRIM(CAST(last_name AS STRING))) as contact_full_name,
  CURRENT_TIMESTAMP() as loaded_at
FROM 
{{ source('raw_data', 'suppliers') }}