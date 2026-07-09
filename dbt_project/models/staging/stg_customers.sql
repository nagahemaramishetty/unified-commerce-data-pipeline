-- stg_customers.sql
-- Light standardization pass. No known messiness was injected into the customers
-- source in this project, so this model mainly exists to enforce consistent typing
-- and naming ahead of the marts layer.

select
    customer_id,
    trim(first_name)               as first_name,
    trim(last_name)                as last_name,
    lower(trim(email))             as email,
    trim(region)                   as region,
    signup_date::date              as signup_date
from {{ source('raw', 'customers') }}
