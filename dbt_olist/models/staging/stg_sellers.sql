{{ config(materialized='view') }}

with source as (
    select * from {{ source('olist', 'sellers') }}
),

-- 1. Deduplicate & Clean base fields
deduplicated as (
    select 
        *,
        -- UUID validation check 
        case 
            when regexp_contains(seller_id, r'^[0-9a-fA-F]{32}$') then seller_id 
            else null 
        end as cleaned_seller_id,
        
        -- NFD Normalization & Accent removal for cities 
        regexp_replace(normalize(seller_city, NFD), r'\pM', '') as cleaned_seller_city,
        
        count(*) over (partition by seller_id) as duplicate_count,
        row_number() over (
            partition by seller_id
            order by 
                case when seller_city is not null then 0 else 1 end,
                seller_city
        ) as row_num 
    from source
),

unique_records as (
    select * except(row_num)
    from deduplicated 
    where row_num = 1
),

-- 2. "Most Frequent City" Logic 
-- Resolves inconsistencies where one ZIP has multiple city name variants
city_rankings as (
    select 
        seller_zip_code_prefix,
        cleaned_seller_city as most_frequent_city,
        row_number() over (
            partition by seller_zip_code_prefix 
            order by count(*) desc, cleaned_seller_city asc
        ) as city_rank
    from unique_records
    group by 1, 2
),

top_cities as (
    select seller_zip_code_prefix, most_frequent_city
    from city_rankings
    where city_rank = 1
),

-- Back to the easy-to-spell name!
with_quality_flags as (
    select
        -- Core ID
        u.cleaned_seller_id as seller_id,
        
        -- ZIP Code: Standard 5-digit format
        lpad(cast(u.seller_zip_code_prefix as string), 5, '0') as seller_zip_code_prefix,
        
        -- Standardized City (Accent-free and most frequent for that ZIP)
        coalesce(t.most_frequent_city, u.cleaned_seller_city) as seller_city,
        
        -- [CLEANED] Standardize state format only (No hardcoding)
        -- Nullifies if empty, wrong length, or contains numbers (Databricks RLIKE equivalent)
        case 
            when u.seller_state is null 
                 or length(trim(u.seller_state)) != 2 
                 or not regexp_contains(trim(u.seller_state), r'^[A-Za-z]{2}$')
                 or upper(trim(u.seller_state)) in ('NA', 'NAN', 'NOT_DEFINED')
            then null
            else upper(trim(u.seller_state))
        end as seller_state,
        
        -- Quality Flags 
        u.cleaned_seller_id is null as seller_id_invalid_format,
        u.cleaned_seller_city != t.most_frequent_city as city_name_was_standardized,
        u.seller_state is null as seller_state_missing_or_invalid_format,
        
        -- Audit fields
        u.duplicate_count > 1 as had_duplicates,
        current_timestamp() as ingestion_timestamp
    from unique_records u
    left join top_cities t on u.seller_zip_code_prefix = t.seller_zip_code_prefix
)

select * from with_quality_flags
