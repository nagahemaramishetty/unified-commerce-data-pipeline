-- mart_monthly_kpis.sql
--
-- Pre-aggregated monthly KPIs by region and category, built specifically so
-- Power BI, Tableau, and the Excel executive summary all read from the same
-- source of truth instead of each tool recalculating revenue and margin
-- independently, which is a common cause of dashboards quietly disagreeing
-- with each other in real companies.

select
    date_trunc('month', order_date)::date  as order_month,
    region,
    category,
    count(distinct order_id)               as order_count,
    sum(quantity)                          as units_sold,
    round(sum(revenue), 2)                 as total_revenue,
    round(sum(gross_margin), 2)            as total_gross_margin,
    round(sum(gross_margin) / nullif(sum(revenue), 0) * 100, 2) as margin_pct
from {{ ref('fct_orders') }}
group by 1, 2, 3
