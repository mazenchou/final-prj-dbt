-- models/intermediate/int_suppliers.sql
{{ config(materialized='view') }}

with suppliers as (
    select * from {{ ref('stg_suppliers') }}
)

select
    supplier_id,
    company_name,
    concat(trim(first_name), ' ', trim(last_name)) as contact_name,
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
    website,
    notes,
    attachments,
    contact_full_name,
    loaded_at
from suppliers
where supplier_id is not null