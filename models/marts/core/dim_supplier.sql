-- models/marts/core/dim_supplier.sql
{{ 
    config(
        materialized='table',
        tags=['dimension', 'supplier']
    ) 
}}

SELECT
    supplier_id,
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
    website,
    notes,
    contact_full_name,
    loaded_at
FROM {{ ref('int_suppliers') }}
WHERE supplier_id IS NOT NULL
