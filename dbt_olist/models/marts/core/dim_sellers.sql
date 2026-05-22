{{ config(materialized='table') }}

with sellers as (
    select * from {{ ref('stg_sellers') }}
),

-- Use the same logic we used for customers to get clean coordinates
official_geo as (
    select 
        geolocation_zip_code_prefix as zip_code,
        geolocation_lat as lat,
        geolocation_lng as lng
    from {{ ref('stg_geolocation') }}
    qualify row_number() over (partition by geolocation_zip_code_prefix order by geolocation_city) = 1
),

patched_geo as (
    -- This uses the seed file we created with Brasília and São Paulo fixes
    select * from {{ ref('patch_missing_geolocations') }}
),

final as (
    select
        -- 1. Core Seller Info
        s.seller_id,
        s.seller_zip_code_prefix,
        s.seller_city,
        s.seller_state,

        -- 2. Patched Geolocation (Lat/Lng)
        coalesce(g.lat, p.lat) as latitude,
        coalesce(g.lng, p.lng) as longitude,
        case when g.zip_code is null and p.zip_code_prefix is not null then true else false end as is_patched_location,

        -- 3. Star Schema Key
        {{ dbt_utils.generate_surrogate_key([
            's.seller_zip_code_prefix', 
            's.seller_city', 
            's.seller_state'
        ]) }} as location_id,

        -- 4. Metadata
        s.ingestion_timestamp as processed_at

    from sellers s
    left join official_geo g on s.seller_zip_code_prefix = g.zip_code
    left join patched_geo p on s.seller_zip_code_prefix = p.zip_code_prefix
)

select * from final
