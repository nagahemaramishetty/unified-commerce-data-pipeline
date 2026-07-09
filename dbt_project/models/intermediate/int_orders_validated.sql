-- int_orders_validated.sql
--
-- FIX 3: Orphaned foreign keys.
--   A small percentage of web_platform orders reference a customer_id or
--   product_id that doesn't exist in the dimension tables, simulating a sync
--   lag between the order service and the customer/product services.
--
--   Rather than silently dropping these or silently keeping them (which would
--   break referential integrity in the marts and cause dashboard totals to
--   overcount), we explicitly flag them here with is_valid_customer and
--   is_valid_product. The marts layer filters to valid orders only, and the
--   count of excluded orders is surfaced as a monitoring metric, not hidden.
--   See _intermediate.yml for the relationship tests that catch this if the
--   issue re-appears in a future run.

select
    o.order_id,
    o.customer_id,
    o.product_id,
    o.quantity,
    o.order_date,
    o.load_timestamp,
    o.source_system,
    o.is_late_arriving,
    c.customer_id is not null   as is_valid_customer,
    p.product_id is not null    as is_valid_product,
    p.category,
    p.unit_price,
    p.unit_cost,
    c.region
from {{ ref('stg_orders') }} o
left join {{ ref('stg_customers') }} c on o.customer_id = c.customer_id
left join {{ ref('stg_products') }} p on o.product_id = p.product_id
