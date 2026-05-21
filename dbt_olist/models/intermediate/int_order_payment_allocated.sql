{{ config(materialized='view') }}

with order_item_counts as (
    select 
        order_id,
        count(*) as items_per_order
    from {{ ref('stg_order_items') }}
    group by order_id
),

payments_summary as (
    select 
        order_id,
        total_order_payment,
        max_installments,
        unique_payment_methods
    from {{ ref('int_order_payments_summary') }}
)

select
    p.order_id,
    p.total_order_payment,
    p.max_installments,
    p.unique_payment_methods,
    ic.items_per_order,
    
    -- PRE-CALCULATED ALLOCATION (not in fact!)
    round(p.total_order_payment / ic.items_per_order, 2) as allocated_payment_per_item,
    
    -- Safety: Flag orders where allocation won't be perfect
    abs(p.total_order_payment - (round(p.total_order_payment / ic.items_per_order, 2) * ic.items_per_order)) > 0.01 
        as has_rounding_discrepancy

from payments_summary p
join order_item_counts ic on p.order_id = ic.order_id