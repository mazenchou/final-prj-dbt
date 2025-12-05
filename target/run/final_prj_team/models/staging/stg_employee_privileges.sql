

  create or replace view `final-prj-480309`.`dbt_dev`.`stg_employee_privileges`
  OPTIONS()
  as 

SELECT
  CAST(employee_id AS INT64) as employee_id,
  CAST(privilege_id AS INT64) as privilege_id,
  CURRENT_TIMESTAMP() as loaded_at
FROM `final-prj-480309`.`dbt_dev`.`employee_privileges`;

