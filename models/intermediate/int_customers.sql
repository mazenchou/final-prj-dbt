-- models/intermediate/int_customers.sql (SIMPLIFIED VERSION)
{{ config(materialized='view') }}

with customers as (
    select * from {{ ref('stg_customers') }}
)

select
    customer_id,
    company_name,
    concat(trim(first_name), ' ', trim(last_name)) as contact_name,
    city,
    state_code,
    zip_code,
    country,
    web_page,
    email,
    job_title,
    loaded_at  -- Only use columns that actually exist
from customers
where customer_id is not null