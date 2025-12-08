-- models/marts/core/dim_employee.sql
{{ config(
    materialized='table',
    tags=['dimension', 'employee']
) }}

SELECT
  employee_id,
  company_name,
  employee_name,
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
  web_page as website,  -- FIXED: use web_page and alias to website
  notes,
  full_name,
  loaded_at
FROM {{ ref('int_employees') }}
WHERE employee_id IS NOT NULL