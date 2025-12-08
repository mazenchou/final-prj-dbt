-- models/marts/core/dim_location.sql
{{ config(materialized='table') }}

SELECT
  -- Create a unique location ID using available data
  MD5(CONCAT(
    COALESCE(city, ''), 
    '|', 
    COALESCE(state_code, ''), 
    '|', 
    COALESCE(country, '')
  )) as location_id,
  city,
  state_code,
  country
FROM (
  -- Customer locations: only city available
  SELECT 
    city,
    NULL as state_code,  -- Customer doesn't have state_code
    NULL as country      -- Customer doesn't have country
  FROM {{ ref('dim_customer') }}
  WHERE city IS NOT NULL
  
  UNION DISTINCT
  
  -- Employee locations: full address available
  SELECT 
    city,
    state_code,
    country
  FROM {{ ref('dim_employee') }}
  WHERE city IS NOT NULL
  
  UNION DISTINCT
  
  -- Supplier locations: full address available
  SELECT 
    city,
    state_code,
    country
  FROM {{ ref('dim_supplier') }}
  WHERE city IS NOT NULL
) locations
WHERE city IS NOT NULL