{{ config(materialized='view') }}

with products as (
    select * from {{ ref('stg_products') }}
),

-- Use the staging model you just shared
translations as (
    select * from {{ ref('stg_product_category_name_translation') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select order_id, order_status from {{ ref('stg_orders') }}
),

joined as (
    select 
        p.product_id,
        -- LOGIC: English Translation -> Original Portuguese -> 'Uncategorized' fallback
        coalesce(
            t.product_category_name_english, 
            nullif(trim(p.product_category_name), ''), 
            'Uncategorized'
        ) as product_category_name,
        
        oi.order_id,
        o.order_status
    from products p
    -- Join to get English names from your new stg model
    left join translations t on p.product_category_name = t.product_category_name
    left join order_items oi on p.product_id = oi.product_id
    left join orders o on oi.order_id = o.order_id
    
    where o.order_status in ('delivered', 'invoiced', 'shipped', 'approved')
)

select * from joined
