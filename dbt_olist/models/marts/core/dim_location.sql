{{ config(materialized='table') }}

with geo_cleaned as (
    select 
        geolocation_zip_code_prefix,
        geolocation_city,
        geolocation_state,
        geolocation_lat,
        geolocation_lng,
        
        -- Quality flags from staging
        zip_code_is_invalid,
        city_is_invalid,
        state_is_invalid,
        coordinates_are_invalid,
        
        -- Deduplicate: one row per ZIP + city + state
        row_number() over (
            partition by geolocation_zip_code_prefix, geolocation_city, geolocation_state
            order by coordinates_are_invalid asc  -- Prefer rows with valid coordinates
        ) as rn
    from {{ ref('stg_geolocation') }}
    where zip_code_is_invalid = false  -- Only valid ZIPs in dimension
),

geo_unique as (
    select * from geo_cleaned where rn = 1
),

final as (
    select 
        -- SURROGATE KEY (FIXED: stable hash, not row_number)
        {{ dbt_utils.generate_surrogate_key([
            'geolocation_zip_code_prefix', 
            'geolocation_city', 
            'geolocation_state'
        ]) }} as location_key,
        
        -- BUSINESS KEYS / NATURAL ID
        geolocation_zip_code_prefix,
        geolocation_city as city,
        geolocation_state as state,
        
        -- Attributes
        geolocation_lat as latitude,
        geolocation_lng as longitude,
        
        -- Quality flags
        city_is_invalid,
        state_is_invalid,
        coordinates_are_invalid,
        
        -- Metadata
        current_timestamp() as processed_at
        
    from geo_unique
)

select * from final