-- models/intermediate/int_orders_enriched.sql
with orders as (
    select * from {{ ref('stg_orders') }}
),

order_totals as (
    select * from {{ ref('int_order_items_aggregated') }}
),

joined as (
    select
        o.*,
        coalesce(t.total_item_value, 0) as total_item_value,
        coalesce(t.total_freight_value, 0) as total_freight_value,
        -- Round the final sum to prevent 3526.45999...
        round(
            cast(coalesce(t.total_item_value, 0) + coalesce(t.total_freight_value, 0) as numeric), 
            2
        ) as total_order_value,
        coalesce(t.total_items, 0) as total_items
    from orders o
    left join order_totals t on o.order_id = t.order_id
)

select * from joined
