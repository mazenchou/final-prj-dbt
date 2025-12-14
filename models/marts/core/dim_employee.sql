-- models/marts/core/dim_employee.sql
{{ 
    config(
        materialized='table',
        tags=['dimension', 'employee']
    ) 
}}

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
    -- Add location_id (same calculation)
    MD5(
        CONCAT(
            UPPER(TRIM(city)),
            '|',
            UPPER(TRIM(state_code)),
            '|',
            UPPER(TRIM(country))
        )
    ) as location_id,
    web_page as website,
    notes,
    full_name,
    loaded_at
FROM {{ ref('int_employees') }}
WHERE employee_id IS NOT NULL
  AND city IS NOT NULL
  AND state_code IS NOT NULL
  AND country IS NOT NULL