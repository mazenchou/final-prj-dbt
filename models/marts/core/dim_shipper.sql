-- models/core/dim_shippers.sql
{{ config(
    materialized='table',
    tags=['dimension', 'shipper']
) }}

SELECT
  shipper_id,
  company_name,
  contact_name,
  email,
  job_title,
  business_phone,
  home_phone,
  mobile_phone,
  fax_number,
  address,
  city,
  state_code,
  zip_code,
  country,
  website,
  notes,
  contact_full_name,
  loaded_at
FROM {{ ref('int_shippers') }}
WHERE shipper_id IS NOT NULL