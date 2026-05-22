{{ config(materialized='table') }}

with products_base as (
    select 
        *,
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key
    from {{ ref('stg_products') }}
),

category_mapping as (
    select distinct product_id, product_category_name 
    from {{ ref('int_products_categoried') }}
),

top_15 as (
    select product_id from {{ ref('int_top_15_products') }}
),

final as (
    select 
        -- SURROGATE KEY
        p.product_key,
        
        -- BUSINESS KEY
        p.product_id,
        
        -- Attributes
        coalesce(cm.product_category_name, 'uncategorized') as product_category_name,
        
        p.product_name_length,
        p.product_description_length,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm,

        -- Flags
        case when t.product_id is not null then true else false end as is_top_15_seller,
        p.product_id_is_invalid,
        p.product_category_name_is_missing,
        
        -- Metadata
        current_timestamp() as valid_from,
        null as valid_to,
        true as is_current,
        p.ingestion_timestamp as processed_at

    from products_base p
    left join category_mapping cm on p.product_id = cm.product_id
    left join top_15 t on p.product_id = t.product_id
)

select * from final