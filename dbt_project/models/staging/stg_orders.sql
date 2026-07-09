-- stg_orders.sql
--
-- This is the model that fixes the two biggest data quality problems introduced
-- upstream. Both fixes are deliberate and documented here rather than being
-- silent transformations, since they're exactly the kind of thing a QA process
-- is supposed to catch.
--
-- FIX 1: Inconsistent date formats across source systems.
--   legacy_pos writes order_date as MM/DD/YYYY. web_platform writes YYYY-MM-DD.
--   We standardize both into a single DATE type based on source_system.
--
-- FIX 2: Duplicate orders from a legacy_pos retry bug.
--   Some order_ids appear twice in raw.orders_raw with a load_timestamp a few
--   seconds to minutes apart. We keep only the earliest occurrence per order_id
--   (the original write, not the retry), using ROW_NUMBER() partitioned by
--   order_id. This is verified downstream by a `unique` test on order_id in
--   this model, see _staging.yml.
--
-- We also compute a late_arriving flag here (order_date more than 30 days
-- before load_timestamp) rather than dropping those records, since late-arriving
-- data is a real reporting concern, not a data quality defect, records should
-- still count, but analysts need to know they landed after their period closed.

with parsed_dates as (

    select
        order_id,
        customer_id,
        product_id,
        quantity,
        source_system,
        load_timestamp,
        case
            when source_system = 'legacy_pos' then to_date(order_date, 'MM/DD/YYYY')
            when source_system = 'web_platform' then to_date(order_date, 'YYYY-MM-DD')
        end as order_date

    from {{ source('raw', 'orders_raw') }}

),

deduped as (

    select
        *,
        row_number() over (
            partition by order_id
            order by load_timestamp asc
        ) as row_num
    from parsed_dates

)

select
    order_id,
    customer_id,
    product_id,
    quantity,
    order_date,
    load_timestamp,
    source_system,
    (load_timestamp::date - order_date) > 30 as is_late_arriving
from deduped
where row_num = 1   -- drops the retry-bug duplicates, keeps first occurrence
