-- fct_orders.sql
--
-- ARCHITECTURAL TRADEOFF (documented, not accidental):
--   A strict star schema would keep this fact table fully normalized, with
--   category and region living only in dim_products and dim_customers, and
--   BI tools joining to them at query time. Instead, product category and
--   customer region are denormalized directly into this fact table.
--
--   Why: Power BI and Tableau dashboards built on top of this table filter and
--   group by category and region constantly (they're the two most common
--   slicers stakeholders ask for). Joining two dimension tables on every single
--   dashboard interaction adds query latency that compounds as the fact table
--   grows. Denormalizing these two low-cardinality, rarely-changing columns
--   trades a small amount of storage and a small risk of staleness (if a
--   product's category is reclassified, historical fact rows keep the old
--   category unless this model is fully rebuilt) for materially faster
--   dashboard queries. Customer name, email, and other higher-change or
--   higher-cardinality attributes are deliberately NOT denormalized here,
--   those stay in dim_customers and get joined only when needed.
--
--   At larger scale, this tradeoff would need revisiting, see the
--   "limitations" section of the project README.
--
-- Only orders with a valid customer and valid product are included here,
-- filtering out the orphaned foreign key records identified in
-- int_orders_validated. The excluded count is captured in the QA checklist,
-- not silently dropped without a trace.

select
    order_id,
    customer_id,
    product_id,
    order_date,
    source_system,
    is_late_arriving,
    category,
    region,
    quantity,
    unit_price,
    unit_cost,
    round(quantity * unit_price, 2)                as revenue,
    round(quantity * (unit_price - unit_cost), 2)   as gross_margin
from {{ ref('int_orders_validated') }}
where is_valid_customer = true
  and is_valid_product = true
