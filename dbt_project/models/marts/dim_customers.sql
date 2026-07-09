-- dim_customers.sql
select * from {{ ref('stg_customers') }}
