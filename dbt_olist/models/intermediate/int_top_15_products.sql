-- =====================================================
-- MODEL: int_top_15_products
-- PURPOSE: Identify top 15 products by order volume
-- DEPENDENCIES: stg_order_items
-- USED BY: dim_products
-- GRAIN: One row per product_id (only top 15)
-- =====================================================

{{ config(materialized='view') }}

with product_sales as (
    select 
        product_id,
        count(order_id) as total_orders,
        round(cast(sum(price) as numeric), 2) as total_revenue
    from {{ ref('stg_order_items') }}
    where product_id_is_invalid = false
      and price_issue = false
    group by 1
),

ranked_products as (
    select 
        product_id,
        total_orders,
        total_revenue,
        cast(row_number() over (order by total_orders desc) as int64) as product_rank
    from product_sales
    where total_orders > 0
)

select 
    product_id,
    total_orders,
    total_revenue,
    product_rank,
    current_timestamp() as processed_at
from ranked_products
where product_rank <= 15