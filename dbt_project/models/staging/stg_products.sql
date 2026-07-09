-- stg_products.sql
-- Light standardization pass, matches stg_customers in purpose.

select
    product_id,
    trim(product_name)     as product_name,
    trim(category)          as category,
    unit_cost::numeric(10,2)   as unit_cost,
    unit_price::numeric(10,2)  as unit_price
from {{ source('raw', 'products') }}
