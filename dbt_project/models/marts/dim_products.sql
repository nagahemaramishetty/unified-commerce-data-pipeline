-- dim_products.sql
select * from {{ ref('stg_products') }}
