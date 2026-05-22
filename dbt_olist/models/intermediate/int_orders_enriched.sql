-- =====================================================
-- MODEL: int_orders_enriched
-- PURPOSE: Enrich orders with aggregated item totals (value, freight, count)
-- DEPENDENCIES: stg_orders, int_order_items_aggregated
-- USED BY: dim_orders, fct_sales
-- GRAIN: One row per order_id
-- =====================================================

{{ config(materialized='view') }}

-- =====================================================
-- CTE 1: Orders with quality filters
-- =====================================================
with orders as (
    select 
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        
        -- Quality flags
        invalid_order_id,
        invalid_customer_id,
        missing_order_status,
        missing_purchase_timestamp,
        flag_approval_before_purchase,
        flag_delivered_after_estimated
        
    from {{ ref('stg_orders') }}
    where invalid_order_id = false
),

-- =====================================================
-- CTE 2: Order totals from intermediate
-- =====================================================
order_totals as (
    select 
        order_id,
        total_item_value,
        total_freight_value,
        total_items
    from {{ ref('int_order_items_aggregated') }}
),

-- =====================================================
-- CTE 3: Join with NULL handling
-- =====================================================
joined as (
    select
        -- Keys
        o.order_id,
        o.customer_id,
        
        -- Status
        o.order_status,
        
        -- Timestamps
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        
        -- Aggregated measures (with NULL handling)
        coalesce(t.total_item_value, 0) as total_item_value,
        coalesce(t.total_freight_value, 0) as total_freight_value,
        coalesce(t.total_items, 0) as total_items,
        
        -- Derived measure (order value = items + freight)
        round(
            cast(coalesce(t.total_item_value, 0) + coalesce(t.total_freight_value, 0) as numeric), 
            2
        ) as total_order_value,
        
        -- Quality flags (preserved from staging)
        o.missing_order_status,
        o.missing_purchase_timestamp,
        o.flag_approval_before_purchase,
        o.flag_delivered_after_estimated,
        
        -- Flag: Order has items but no value? (data issue)
        case 
            when coalesce(t.total_items, 0) > 0 and coalesce(t.total_item_value, 0) = 0 then true 
            else false 
        end as has_items_with_zero_value,
        
        -- Metadata
        current_timestamp() as processed_at
        
    from orders o
    left join order_totals t 
        on o.order_id = t.order_id
)

select * from joined