-- models/core/dim_date.sql
{{ config(
    materialized='table',
    tags=['dimension', 'date']
) }}

WITH date_range AS (
  -- Create a complete date range independent of your data
  SELECT date_day
  FROM UNNEST(
    GENERATE_DATE_ARRAY(
      DATE('2004-01-01'),  -- Start: Way before your earliest data
      DATE('2025-12-31'),  -- End: Way after your latest data
      INTERVAL 1 DAY
    )
  ) AS date_day
)

SELECT
  -- Primary Key: Integer date representation (YYYYMMDD)
  CAST(FORMAT_DATE('%Y%m%d', date_day) AS INT64) as date_id,
  
  -- Full date
  date_day,
  
  -- Simple columns you asked for
  EXTRACT(YEAR FROM date_day) as year,
  EXTRACT(MONTH FROM date_day) as month,
  EXTRACT(DAY FROM date_day) as day,
  
  -- Basic useful attributes
  FORMAT_DATE('%B', date_day) as month_name,       -- "December"
  FORMAT_DATE('%A', date_day) as day_name,         -- "Monday"
  EXTRACT(DAYOFWEEK FROM date_day) as day_of_week, -- 1-7 (Sunday-Saturday)
  
  -- Simple flags
  CASE 
    WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN TRUE
    ELSE FALSE
  END as is_weekend,
  
  -- Quarter
  CONCAT('Q', EXTRACT(QUARTER FROM date_day)) as quarter,
  
  -- ETL metadata
  CURRENT_TIMESTAMP() as loaded_at
  
FROM date_range
ORDER BY date_day