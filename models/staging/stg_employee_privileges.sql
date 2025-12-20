{{ config(materialized='view') }}

SELECT
  CAST(employee_id AS INT64) as employee_id,
  CAST(privilege_id AS INT64) as privilege_id,
  CURRENT_TIMESTAMP() as loaded_at
FROM {{ source('raw_data', 'employee_privileges') }}
