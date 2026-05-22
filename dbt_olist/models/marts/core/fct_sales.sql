{{ config(materialized='table') }}

with orders as (
    select 
        o.order_id, 
        -- NEW: Get the REAL human ID to match your dim_customers primary key
        c.customer_unique_id as customer_id, 
        o.order_purchase_timestamp 
    from {{ ref('stg_orders') }} o
    -- Join to customers to get the unique person ID
    left join {{ ref('stg_customers') }} c on o.customer_id = c.customer_id
),

items as (
    -- Efficient: Only select grain and price components
    select 
        order_id, 
        order_item_id, 
        product_id, 
        seller_id, 
        price, 
        freight_value 
    from {{ ref('stg_order_items') }}
),

payments as (
    -- Efficient: Only select summary values
    select 
        order_id, 
        total_order_payment, 
        max_installments 
    from {{ ref('int_order_payments_summary') }}
),

sellers as (
    -- Efficient: Only select IDs for mapping
    select 
        seller_id, 
        location_id 
    from {{ ref('dim_sellers') }}
),

final as (
    select
        -- 1. Unique ID
        coalesce(
            concat(o.order_id, '-', cast(i.order_item_id as string)),
            concat(o.order_id, CAST('-0' AS STRING))
        ) as sale_id,

        -- 2. Foreign Keys (Handling the NULLs)
        o.order_id,
        o.customer_id,
        coalesce(i.product_id, 'UNKNOWN_PRODUCT') as product_id,
        coalesce(s.seller_id, 'UNKNOWN_SELLER') as seller_id,
        coalesce(s.location_id, 'UNKNOWN_LOCATION') as location_id,
        cast(o.order_purchase_timestamp as date) as time_id,

        -- 3. Quantity / Sequence (Grain tracking)
        coalesce(cast(i.order_item_id as string), cast('0' as string)) as quantity,

        -- 4. Financial Logic (Fallback for canceled/missing items)
        case 
            when i.order_id is not null then cast(i.price as numeric)
            else cast(p.total_order_payment as numeric)
        end as price,

        coalesce(cast(i.freight_value as numeric), 0) as freight_value,

        -- 5. THE ALLOCATION & ROUNDING FIX
        -- We divide the total payment by the item count per order and round to 2 decimals.
        -- This ensures sum(total_payment_value) in Streamlit matches your real Revenue.
        round(
            cast(
                p.total_order_payment / count(*) over (partition by o.order_id) 
            as numeric), 
        2) as total_payment_value,

        p.max_installments as payment_installments

    from orders o
    inner join payments p on o.order_id = p.order_id
    left join items i on o.order_id = i.order_id
    left join sellers s on i.seller_id = s.seller_id
)

select * from final
