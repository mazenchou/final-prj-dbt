-- models/intermediate/int_shippers.sql
{{ config(materialized='view') }}

with shippers as (
    select * from {{ ref('stg_shippers') }}
)

select
    shipper_id,
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
    website,  -- ← Keep as website (staging renamed web_page to website)
    notes,
    attachments,
    contact_full_name,
    loaded_at
from shippers
where shipper_id is not null