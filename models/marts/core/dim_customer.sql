-- models/marts/core/dim_customer.sql
{{ 
    config(
        materialized='table',
        tags=['dimension', 'customer']
    ) 
}}

SELECT
    customer_id,
    company_name,
    contact_name,
    email,
    job_title,
    -- Calculate SAME location_id as dim_location
    TO_HEX(
        MD5(
            CONCAT(
                UPPER(TRIM(city)),
                '|',
                UPPER(TRIM(state_code)),
                '|',
                UPPER(TRIM(country))
            )
        )
    ) as location_id,  -- COMMA ADDED HERE
    loaded_at
FROM {{ ref('int_customers') }}
WHERE customer_id IS NOT NULL
  AND city IS NOT NULL
  AND state_code IS NOT NULL
  AND country IS NOT NULL