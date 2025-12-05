

SELECT
  CAST(employee_id AS INT64) as employee_id,
  CAST(privilege_id AS INT64) as privilege_id,
  CURRENT_TIMESTAMP() as loaded_at
FROM `final-prj-480309`.`raw_data`.`employee_privileges`