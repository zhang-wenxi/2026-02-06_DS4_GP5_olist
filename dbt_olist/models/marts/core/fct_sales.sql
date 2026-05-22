{{ config(
    materialized='table',
    partition_by={"field": "order_date", "data_type": "date"},
    cluster_by=["order_key", "customer_key", "product_key", "seller_key"]
) }}

-- =====================================================
-- PURPOSE: Sales fact table at order item grain
-- GRAIN: One row per order_item_id
-- FOREIGN KEYS: All *_key columns point to dimensions
-- =====================================================

with order_items as (
    select 
        order_id,
        order_item_id,
        product_id,
        seller_id,
        price,
        freight_value,
        price_issue,
        freight_issue
    from {{ ref('stg_order_items') }}
),

orders as (
    select 
        order_id,
        customer_id,
        order_purchase_timestamp,
        order_delivered_customer_date,  -- Correct column name
        order_estimated_delivery_date,  -- Correct column name
        order_status
    from {{ ref('stg_orders') }}
),

-- Use pre-allocated payments from intermediate
payments_allocated as (
    select 
        order_id,
        allocated_payment_per_item,
        max_installments,
        unique_payment_methods,
        has_rounding_discrepancy
    from {{ ref('int_order_payment_allocated') }}
),

-- Join to dimensions to get surrogate keys
dim_customers as (
    select customer_id as business_customer_id, customer_key
    from {{ ref('dim_customers') }}
    where is_current = true
),

dim_products as (
    select product_id as business_product_id, product_key
    from {{ ref('dim_products') }}
    where is_current = true
),

dim_sellers as (
    select seller_id as business_seller_id, seller_key, seller_zip_code_prefix
    from {{ ref('dim_sellers') }}
    where is_current = true
),

dim_location as (
    select location_key, geolocation_zip_code_prefix
    from {{ ref('dim_location') }}
),

dim_orders_junk as (
    select order_status, order_attribute_key
    from {{ ref('dim_orders') }}
),

dim_time as (
    select time_key, order_date
    from {{ ref('dim_time') }}
),

final as (
    select
        -- SURROGATE FOREIGN KEYS (The fix!)
        o.order_id as order_key,  -- Using order_id as degenerate dimension key
        c.customer_key,
        p.product_key,
        s.seller_key,
        l.location_key,
        t.time_key,
        oa.order_attribute_key,
        
        -- BUSINESS KEYS (for reference only)
        oi.order_id,
        oi.order_item_id,
        
        -- DATE (for partitioning)
        t.order_date,
        
        -- MEASURES
        oi.price,
        oi.freight_value,
        pa.allocated_payment_per_item as total_payment_value,
        pa.max_installments as payment_installments,
        pa.unique_payment_methods,
        
        -- QUANTITY (always 1 at this grain)
        1 as quantity,
        
        -- DERIVED MEASURES
        case 
            when o.order_delivered_customer_date is not null then true 
            else false 
        end as delivered_flag,
        
        case 
            when o.order_delivered_customer_date is not null
            then date_diff(
                date(o.order_delivered_customer_date), 
                date(o.order_purchase_timestamp), 
                day
            )
            else null
        end as delivery_days,
        
        case 
            when o.order_delivered_customer_date is not null
            then greatest(
                date_diff(
                    date(o.order_delivered_customer_date), 
                    date(o.order_estimated_delivery_date), 
                    day
                ), 
                0
            )
            else null
        end as estimated_delay_days,
        
        -- QUALITY FLAGS (Preserved from staging)
        oi.price_issue,
        oi.freight_issue,
        pa.has_rounding_discrepancy,
        
        -- METADATA
        current_timestamp() as loaded_at

    from order_items oi
    
    -- Join to orders
    inner join orders o on oi.order_id = o.order_id
    
    -- Join to pre-allocated payments
    inner join payments_allocated pa on oi.order_id = pa.order_id
    
    -- Join to dimensions (using business keys)
    left join dim_customers c on o.customer_id = c.business_customer_id
    left join dim_products p on oi.product_id = p.business_product_id
    left join dim_sellers s on oi.seller_id = s.business_seller_id
    left join dim_location l on s.seller_zip_code_prefix = l.geolocation_zip_code_prefix
    left join dim_orders_junk oa on o.order_status = oa.order_status
    left join dim_time t on date(o.order_purchase_timestamp) = t.order_date
)

select * from final