with orders as (
    select * from {{ ref('stg_orders') }}
    where order_status in ('delivered', 'invoiced', 'shipped', 'approved')
),

customers as (
    select customer_id, customer_unique_id from {{ ref('stg_customers') }}
),

order_items as (
    select order_id, sum(price) as item_value 
    from {{ ref('stg_order_items') }} 
    group by 1
),

metrics as (
    select
        c.customer_unique_id as customer_id,
        count(distinct o.order_id) as frequency,
        max(o.order_purchase_timestamp) as last_purchase_date,
        -- FIX: Added COALESCE to prevent NULLs
        round(cast(coalesce(sum(oi.item_value), 0) as numeric), 2) as monetary_value
    from orders o
    join customers c on o.customer_id = c.customer_id
    left join order_items oi on o.order_id = oi.order_id
    group by 1
),

calculated_metrics as (
    select
        customer_id,
        frequency,
        monetary_value,
        -- Use a fixed date (or MAX date from orders) to keep recency relevant
        date_diff(date '2018-09-03', cast(last_purchase_date as date), DAY) as recency
    from metrics
),

final_quartiles as (
    select
        *,
        -- NTILes to create the 1-4 scores (Higher is better)
        ntile(4) over (order by recency desc) as r_quartile,
        ntile(4) over (order by frequency asc) as f_quartile,
        ntile(4) over (order by monetary_value asc) as v_quartile
    from calculated_metrics
)

select
    *,
    -- Create the string score used in your segments model
    concat(
        cast(r_quartile as string),
        cast(f_quartile as string),
        cast(v_quartile as string)
    ) as rfv_score
from final_quartiles
