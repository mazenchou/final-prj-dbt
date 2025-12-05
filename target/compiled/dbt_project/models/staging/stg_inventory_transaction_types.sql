

SELECT
  CAST(id AS INT64) as transaction_type_id,
  INITCAP(TRIM(type_name)) as transaction_type_name,
  CURRENT_TIMESTAMP() as loaded_at
FROM `final-prj-480309`.`raw_data`.`inventory_transaction_types`