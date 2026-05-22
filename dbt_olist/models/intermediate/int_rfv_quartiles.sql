with product_sales as (
    select 
        product_id,
        count(order_id) as total_orders,
        -- FIX: Use ROUND and CAST to Numeric to solve the long decimal issue
        round(cast(sum(price) as numeric), 2) as total_revenue
    from {{ ref('stg_order_items') }}
    group by 1
),

ranked_products as (
    select 
        product_id,
        total_orders,
        total_revenue,
        -- Ranking by total orders (Top sellers)
        cast(row_number() over (order by total_orders desc) as int64) as product_rank
    from product_sales
)

select 
    product_id,
    total_orders,
    total_revenue,
    product_rank
from ranked_products
where product_rank <= 15
