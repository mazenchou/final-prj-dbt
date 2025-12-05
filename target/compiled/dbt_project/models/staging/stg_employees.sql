

SELECT
  CAST(id AS INT64) as employee_id,
  TRIM(company) as company_name,
  TRIM(last_name) as last_name,
  TRIM(first_name) as first_name,
  LOWER(TRIM(email_address)) as email,
  TRIM(job_title) as job_title,
  business_phone,
  home_phone,
  mobile_phone,
  fax_number,
  TRIM(address) as address,
  INITCAP(TRIM(city)) as city,
  UPPER(TRIM(state_province)) as state_code,
  TRIM(zip_postal_code) as zip_code,
  INITCAP(TRIM(country_region)) as country,
  web_page,
  notes,
  attachments,
  CONCAT(TRIM(first_name), ' ', TRIM(last_name)) as full_name,
  CURRENT_TIMESTAMP() as loaded_at
FROM `final-prj-480309`.`raw_data`.`employees`