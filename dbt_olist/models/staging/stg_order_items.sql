{{ config(materialized='view') }}

with source as (
    select * from {{ source('olist', 'order_items') }}
),

-- Uniqueness: Deduplicate based on order_id and order_item_id
deduplicated as (
    select 
        *,
        count(*) over (partition by order_id, order_item_id) as raw_occurrence_count
    from source
    qualify row_number() over (
        partition by order_id, order_item_id
        order by shipping_limit_date desc
    ) = 1
),

with_quality_flags as (
    select
        -- Original ID Fields
        order_id,
        product_id,
        seller_id,
        order_item_id,
        
        -- Date & Financial Fields (Casting to ensure BigQuery numeric types)
        cast(shipping_limit_date as timestamp) as shipping_limit_date,
        -- 1. Price: Standardize to 0.0 if missing or invalid
        coalesce(safe_cast(price as float64), 0.0) as price,
        
        -- 2. Freight: Standardize to 0.0 if missing or invalid
        coalesce(safe_cast(freight_value as float64), 0.0) as freight_value,
        
        -- Combined Quality Flags (Regex + Null check)
        (order_id is null or not regexp_contains(order_id, r'^[0-9a-fA-F]{32}$')) as order_id_is_invalid,
        (product_id is null or not regexp_contains(product_id, r'^[0-9a-fA-F]{32}$')) as product_id_is_invalid,
        (seller_id is null or not regexp_contains(seller_id, r'^[0-9a-fA-F]{32}$')) as seller_id_is_invalid,
        
        -- Logic-based Quality Flags
        (price is null or cast(price as float64) <= 0) as price_issue,
        (freight_value is null or cast(freight_value as float64) < 0) as freight_issue,
        
        -- Audit Fields
        (raw_occurrence_count > 1) as had_duplicates,
        current_timestamp() as ingestion_timestamp
    from deduplicated
)

select * from with_quality_flags
