-- =====================================================
-- MODEL: int_products_categoried
-- PURPOSE: Map products to their English category names based on 
--          successful orders only (delivered/invoiced/shipped/approved)
-- DEPENDENCIES: stg_products, stg_product_category_name_translation, 
--               stg_order_items, stg_orders
-- USED BY: dim_products
-- GRAIN: One row per (product_id, order_id) for successful orders
-- =====================================================

{{ config(materialized='view') }}

-- =====================================================
-- CTE 1: Products with quality filters
-- =====================================================
with products as (
    select 
        product_id,
        product_category_name,
        product_id_is_invalid
    from {{ ref('stg_products') }}
    where product_id_is_invalid = false
),

-- =====================================================
-- CTE 2: Category translations (English mapping)
-- =====================================================
translations as (
    select 
        product_category_name,
        product_category_name_english,
        is_untranslated
    from {{ ref('stg_product_category_name_translation') }}
),

-- =====================================================
-- CTE 3: Order items (filtered for quality)
-- =====================================================
order_items as (
    select 
        product_id,
        order_id,
        price_issue,
        freight_issue
    from {{ ref('stg_order_items') }}
    where product_id_is_invalid = false
      and price_issue = false
),

-- =====================================================
-- CTE 4: Orders (only successful ones)
-- =====================================================
orders as (
    select 
        order_id, 
        order_status,
        missing_order_status,
        invalid_order_status
    from {{ ref('stg_orders') }}
    where missing_order_status = false
      and invalid_order_status = false
),

-- =====================================================
-- CTE 5: Join with business logic
-- =====================================================
joined as (
    select 
        p.product_id,
        
        -- Category fallback: English -> Portuguese -> 'uncategorized'
        coalesce(
            t.product_category_name_english, 
            nullif(trim(p.product_category_name), ''), 
            'uncategorized'
        ) as product_category_name,
        
        t.is_untranslated,
        oi.order_id,
        o.order_status

    from products p
    left join translations t 
        on p.product_category_name = t.product_category_name
    inner join order_items oi 
        on p.product_id = oi.product_id
    inner join orders o 
        on oi.order_id = o.order_id
        and o.order_status in ('delivered', 'invoiced', 'shipped', 'approved')
),

-- =====================================================
-- CTE 6: Final output
-- =====================================================
final as (
    select
        product_id,
        product_category_name,
        order_id,
        order_status,
        coalesce(is_untranslated, false) as is_untranslated,
        current_timestamp() as processed_at
    from joined
)

select * from final