{{ config(materialized='table') }}

with orders_base as (
    -- This includes your item value/freight totals
    select * from {{ ref('int_orders_enriched') }}
),

payments as (
    -- This includes your total_order_payment and installments
    select * from {{ ref('int_order_payments_summary') }}
),

delivery_metrics as (
    -- This includes lead_time_days and is_on_time for delivered orders
    select 
        order_id,
        lead_time_days,
        is_on_time
    from {{ ref('int_orders_date_validity') }}
),

final as (
    select
        -- 1. Keys
        o.order_id,
        o.customer_id,

        -- 2. Order Attributes (The "Flags")
        o.order_status,
        
        -- Logic: Why is this order "different"?
        case 
            when o.order_status in ('canceled', 'unavailable') then 'Incomplete/Canceled'
            when o.order_status = 'delivered' then 'Completed'
            else 'In Progress'
        end as order_activity_status,

        case 
            when p.total_order_payment > 0 and o.total_items = 0 then true 
            else false 
        end as is_ghost_payment, -- Paid but no items (canceled/unavailable)

        -- 3. Dates & Timestamps
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,

        -- 4. Delivery Performance (from calculated_metrics)
        coalesce(d.lead_time_days, 0) as lead_time_days,
        coalesce(d.is_on_time, false) as is_delivered_on_time,

        -- 5. Financial Summary (The "Agreement" between items and payments)
        o.total_items,
        o.total_item_value,
        o.total_freight_value,
        o.total_order_value as expected_order_value, -- Items + Freight
        round(coalesce(p.total_order_payment, 0), 2) AS actual_amount_paid,
        p.unique_payment_methods,
        p.max_installments

    from orders_base o
    left join payments p on o.order_id = p.order_id
    left join delivery_metrics d on o.order_id = d.order_id
)

select * from final
