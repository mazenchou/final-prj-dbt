-- models/intermediate/int_employees.sql
{{ config(materialized='view') }}

with employees as (
    select * from {{ ref('stg_employees') }}
)

select
    employee_id,
    company_name,
    concat(trim(first_name), ' ', trim(last_name)) as employee_name,
    email,
    job_title,
    business_phone,
    home_phone,
    mobile_phone,
    fax_number,
    address,
    city,
    state_code,
    zip_code,
    country,
    web_page, 
    notes,
    attachments,
    full_name,
    loaded_at
from employees
where employee_id is not null