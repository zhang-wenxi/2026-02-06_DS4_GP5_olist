with order_items as (
    select * from {{ ref('stg_order_items') }}
),

order_totals as (
    select
        order_id,
        round(cast(sum(price) as numeric), 2) as total_item_value,
        round(cast(sum(freight_value) as numeric), 2) as total_freight_value,
        count(order_item_id) as total_items
    from order_items
    group by 1
)

select * from order_totals
