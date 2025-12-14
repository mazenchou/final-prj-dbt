-- models/marts/core/dim_location.sql
{{ 
    config(
        materialized='table',
        tags=['dimension', 'location']
    ) 
}}

WITH customer_locations AS (
    SELECT DISTINCT
        TRIM(city) as city,
        TRIM(state_code) as state_code,
        TRIM(country) as country
    FROM {{ ref('stg_customers') }}
    WHERE city IS NOT NULL 
      AND state_code IS NOT NULL 
      AND country IS NOT NULL
),

employee_locations AS (
    SELECT DISTINCT
        TRIM(city) as city,
        TRIM(state_code) as state_code,
        TRIM(country) as country
    FROM {{ ref('stg_employees') }}
    WHERE city IS NOT NULL 
      AND state_code IS NOT NULL 
      AND country IS NOT NULL
),

supplier_locations AS (
    SELECT DISTINCT
        TRIM(city) as city,
        TRIM(state_code) as state_code,
        TRIM(country) as country
    FROM {{ ref('stg_suppliers') }}
    WHERE city IS NOT NULL 
      AND state_code IS NOT NULL 
      AND country IS NOT NULL
),

all_locations AS (
    SELECT * FROM customer_locations
    UNION DISTINCT
    SELECT * FROM employee_locations
    UNION DISTINCT
    SELECT * FROM supplier_locations
)

SELECT
    -- Consistent location_id across all models
    MD5(
        CONCAT(
            UPPER(TRIM(city)),
            '|',
            UPPER(TRIM(state_code)),
            '|',
            UPPER(TRIM(country))
        )
    ) as location_id,
    INITCAP(TRIM(city)) as city,
    UPPER(TRIM(state_code)) as state_code,
    INITCAP(TRIM(country)) as country,
    CURRENT_TIMESTAMP() as loaded_at
FROM all_locations
WHERE city IS NOT NULL
ORDER BY country, state_code, city