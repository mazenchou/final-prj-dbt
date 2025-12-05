

  create or replace view `final-prj-480309`.`dbt_dev`.`stg_customers`
  OPTIONS()
  as -- models/staging/stg_customers.sql


SELECT
  CAST(id AS INT64) as customer_id,
  company as company_name,
  last_name,
  first_name,
  email_address as email,
  job_title,
  CURRENT_TIMESTAMP() as loaded_at
FROM `final-prj-480309`.`dbt_dev`.`customer`;

