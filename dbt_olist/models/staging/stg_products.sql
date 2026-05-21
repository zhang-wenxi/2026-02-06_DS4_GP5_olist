{{ config(materialized='view') }}

with source as (
    select * from {{ source('olist', 'products') }}
),

deduplicated as (
    select 
        *,
        count(*) over (partition by product_id) as duplicate_count,
        row_number() over (
            partition by product_id
            order by 
                case when product_category_name is not null then 0 else 1 end,
                -- Cast weight to float to ensure comparison works in order by
                safe_cast(product_weight_g as float64) desc
        ) as row_num 
    from source
),

unique_records as (
    select 
        * except(row_num),
        (duplicate_count > 1) as had_duplicates
    from deduplicated 
    where row_num = 1
),

cleaned_transformations as (
    select
        -- Keep original ID to satisfy YML 'not_null' test
        product_id,

        -- Normalization
        case 
            when product_category_name is null or trim(product_category_name) = '' or lower(product_category_name) = 'not_defined' then 'uncategorized'
            else 
                regexp_replace(
                    regexp_replace(normalize(product_category_name, NFD), r'\pM', ''),
                    r'_\d+$', ''
                )
        end as product_category_name,

        -- 1. Metadata: Default to 0 for missing lengths/quantities
        coalesce(safe_cast(nullif(trim(cast(product_name_length as string)), '') as int64), 0) as product_name_length,
        coalesce(safe_cast(nullif(trim(cast(product_description_length as string)), '') as int64), 0) as product_description_length,
        coalesce(safe_cast(nullif(trim(cast(product_photos_qty as string)), '') as int64), 0) as product_photos_qty,
        
        -- 2. Physical Dimensions: Default to 0.0 for weights/measures
        coalesce(safe_cast(nullif(trim(cast(product_weight_g as string)), '') as float64), 0.0) as product_weight_g,
        coalesce(safe_cast(nullif(trim(cast(product_length_cm as string)), '') as float64), 0.0) as product_length_cm,
        coalesce(safe_cast(nullif(trim(cast(product_height_cm as string)), '') as float64), 0.0) as product_height_cm,
        coalesce(safe_cast(nullif(trim(cast(product_width_cm as string)), '') as float64), 0.0) as product_width_cm,

        
        had_duplicates
    from unique_records
),

quality_flags as (
    select
        *,
        not regexp_contains(product_id, r'^[0-9a-fA-F]{32}$') as product_id_is_invalid,
        (product_category_name = 'uncategorized') as product_category_name_is_missing,
        
        -- Original column name preserved, but logic improved
        -- Now: NULL = invalid, 0 = invalid, negative = invalid
        (coalesce(product_weight_g, 0) <= 0) as product_weight_g_invalid,
        
        -- NEW: Separate flag for NULLs only (for debugging)
        (product_weight_g is null) as product_weight_g_was_null,

        false as is_synthetic, 
        
        current_timestamp() as ingestion_timestamp
    from cleaned_transformations
)

select * from quality_flags
