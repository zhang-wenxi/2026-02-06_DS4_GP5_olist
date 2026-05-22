{{ config(materialized='table') }}

with sellers_base as (
    select 
        *,
        {{ dbt_utils.generate_surrogate_key(['seller_id']) }} as seller_key
    from {{ ref('stg_sellers') }}
),

official_geo as (
    select 
        geolocation_zip_code_prefix as zip_code,
        geolocation_lat as lat,
        geolocation_lng as lng
    from {{ ref('stg_geolocation') }}
    qualify row_number() over (partition by geolocation_zip_code_prefix order by geolocation_city) = 1
),

patched_geo as (
    select * from {{ ref('patch_missing_geolocations') }}
),

final as (
    select
        -- SURROGATE KEY
        s.seller_key,
        
        -- BUSINESS KEY
        s.seller_id,
        
        -- Attributes
        s.seller_zip_code_prefix,
        s.seller_city,
        s.seller_state,

        -- Geolocation
        coalesce(g.lat, p.lat) as latitude,
        coalesce(g.lng, p.lng) as longitude,
        case when g.zip_code is null and p.zip_code_prefix is not null then true else false end as is_patched_location,

        -- Quality flags
        s.seller_id_invalid_format,
        s.city_name_was_standardized,
        s.seller_state_missing_or_invalid_format,
        
        -- Metadata
        current_timestamp() as valid_from,
        null as valid_to,
        true as is_current,
        s.ingestion_timestamp as processed_at

    from sellers_base s
    left join official_geo g on s.seller_zip_code_prefix = g.zip_code
    left join patched_geo p on s.seller_zip_code_prefix = p.zip_code_prefix
)

select * from final