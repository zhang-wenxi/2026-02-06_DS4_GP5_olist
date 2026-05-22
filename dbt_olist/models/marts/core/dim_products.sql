{{ config(materialized='table') }}

with products as (
    select * from {{ ref('stg_products') }}
),

-- Map the translated names from the intermediate model
category_mapping as (
    select distinct product_id, product_category_name from {{ ref('int_products_categoried') }}
),

top_15 as (
    select product_id from {{ ref('int_top_15_products') }}
),

final as (
    select 
        p.product_id,
        -- Use the English name from category_mapping
        coalesce(cm.product_category_name, 'Uncategorized') as product_category_name,
        
        p.product_name_length,
        p.product_description_length,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm,

        case when t.product_id is not null then true else false end as is_top_15_seller,
        p.product_id_is_invalid,
        p.product_category_name_is_missing

    from products p
    left join category_mapping cm on p.product_id = cm.product_id
    left join top_15 t on p.product_id = t.product_id
)

select * from final
