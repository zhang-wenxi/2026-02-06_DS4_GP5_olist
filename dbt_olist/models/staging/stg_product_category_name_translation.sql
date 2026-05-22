{{ config(materialized='view') }}

with source as (
    select * from {{ source('olist', 'product_category_name_translation') }}
),

-- 1. Cleaning & Base Category Logic 
cleaned_source as (
    select
        regexp_replace(
            regexp_replace(normalize(nullif(product_category_name, 'not_defined'), NFD), r'\pM', ''), 
            r'_\d+$', ''
        ) as product_category_name,
        regexp_replace(
            nullif(product_category_name_english, 'not_defined'), 
            r'_\d+$', ''
        ) as product_category_name_english
    from source
    where product_category_name is not null
),

-- 2. Manual Translation Patching (Handles pc_gamer and kitchen portables)
manual_mapping as (
    select 'portateis_cozinha_e_preparadores_de_alimentos' as pc_name, 'portable_kitchen_and_food_preparators' as eng_name
    union all
    select 'pc_gamer', 'pc_gamer'
),

-- 3. Consolidate and Deduplicate
deduplicated as (
    select 
        src.product_category_name,
        -- Use manual translation if source is null
        coalesce(src.product_category_name_english, map.eng_name) as product_category_name_english,
        case when map.pc_name is not null then true else false end as is_manually_added,
        row_number() over (
            partition by src.product_category_name 
            order by src.product_category_name_english desc
        ) as row_num
    from cleaned_source src
    left join manual_mapping map on src.product_category_name = map.pc_name
),

-- 4. Final Staging with Quality Flags (Great Expectations alignment)
staging as (
    select
        product_category_name,
        product_category_name_english,
        (product_category_name_english is null) as is_untranslated,
        (length(product_category_name) > 100 or length(product_category_name_english) > 100) as length_warning,
        is_manually_added,
        current_timestamp() as ingestion_timestamp
    from deduplicated
    where row_num = 1
)

select * from staging
