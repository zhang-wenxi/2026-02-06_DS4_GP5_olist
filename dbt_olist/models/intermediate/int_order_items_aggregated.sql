-- =====================================================
-- MODEL: int_order_items_aggregated
-- PURPOSE: Aggregate order items to order level
-- DEPENDENCIES: stg_order_items
-- USED BY: int_orders_enriched
-- GRAIN: One row per order_id
-- =====================================================

{{ config(materialized='view') }}

with order_items_filtered as (
    select 
        order_id,
        price,
        freight_value,
        order_item_id
    from {{ ref('stg_order_items') }}
    where product_id_is_invalid = false
      and price_issue = false
),

order_totals as (
    select
        order_id,
        round(cast(sum(price) as numeric), 2) as total_item_value,
        round(cast(sum(freight_value) as numeric), 2) as total_freight_value,
        count(order_item_id) as total_items
    from order_items_filtered
    group by 1
)

select 
    order_id,
    total_item_value,
    total_freight_value,
    total_items,
    current_timestamp() as processed_at
from order_totals