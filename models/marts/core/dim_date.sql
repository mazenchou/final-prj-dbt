-- models/core/dim_date.sql
{{ config(
    materialized='table',
    tags=['dimension', 'date']
) }}

SELECT
  CAST(FORMAT_DATE('%Y%m%d', date_day) AS INT64) as date_id,
  date_day,
  EXTRACT(YEAR FROM date_day) as year,
  EXTRACT(MONTH FROM date_day) as month,
  EXTRACT(DAY FROM date_day) as day,
  EXTRACT(QUARTER FROM date_day) as quarter,
  EXTRACT(WEEK FROM date_day) as week_number,
  EXTRACT(DAYOFWEEK FROM date_day) as day_of_week,
  FORMAT_DATE('%A', date_day) as day_name,
  FORMAT_DATE('%B', date_day) as month_name,
  CASE 
    WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN 'Weekend'
    ELSE 'Weekday'
  END as day_type,
  FORMAT_DATE('%Y-%m', date_day) as year_month,
  DATE_TRUNC(date_day, MONTH) as first_day_of_month,
  LAST_DAY(date_day, MONTH) as last_day_of_month,
  CURRENT_TIMESTAMP() as loaded_at
FROM {{ ref('int_date') }}
ORDER BY date_day