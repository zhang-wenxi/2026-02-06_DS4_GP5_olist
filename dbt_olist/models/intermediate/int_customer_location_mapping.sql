-- =====================================================
-- MODEL: int_customer_location_mapping
-- PURPOSE: Map each customer to their best available geolocation 
--          using ZIP code lookup with fallback to patch data
-- DEPENDENCIES: stg_customers, stg_geolocation, patch_missing_geolocations (seed)
-- USED BY: dim_customers
-- GRAIN: One row per customer_unique_id
-- =====================================================

{{ config(materialized='view') }}

-- =====================================================
-- CTE 1: Customer base data with quality filters
-- =====================================================
with customers as (
    select
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        ingestion_timestamp,
        
        -- Quality flags from staging
        customer_id_is_invalid,
        customer_unique_id_is_invalid,
        customer_zip_code_prefix_is_null,
        customer_city_is_null,
        customer_state_is_null
        
    from {{ ref('stg_customers') }}
    
    -- Only include customers with valid IDs
    where customer_unique_id_is_invalid = false
),

-- =====================================================
-- CTE 2: Official geolocation (one row per ZIP code)
-- =====================================================
official_geo as (
    select 
        geolocation_zip_code_prefix as zip_code,
        geolocation_city as city,
        geolocation_state as state,
        geolocation_lat as lat,
        geolocation_lng as lng,
        
        -- Quality flags for debugging
        zip_code_is_invalid,
        city_is_invalid,
        state_is_invalid,
        coordinates_are_invalid
        
    from {{ ref('stg_geolocation') }}
    
    -- Only use valid geolocation records
    where zip_code_is_invalid = false
    
    -- Deduplicate: One row per ZIP code (prefer alphabetically first city)
    qualify row_number() over (
        partition by geolocation_zip_code_prefix 
        order by geolocation_city asc
    ) = 1
),

-- =====================================================
-- CTE 3: Patched geolocation seed (manual overrides)
-- =====================================================
patched_geo as (
    select 
        zip_code_prefix,
        city,
        state,
        lat,
        lng,
        patch_reason
    from {{ ref('patch_missing_geolocations') }}
),

-- =====================================================
-- CTE 4: Join and apply fallback logic
-- =====================================================
joined as (
    select
        -- Business key (grain identifier)
        c.customer_unique_id,
        
        -- Customer-provided ZIP code
        c.customer_zip_code_prefix,
        
        -- Fallback priority: Official Geo -> Patched Seed -> Customer Staging
        coalesce(g.city, p.city, c.customer_city) as city,
        coalesce(g.state, p.state, c.customer_state) as state,
        coalesce(g.lat, p.lat) as latitude,
        coalesce(g.lng, p.lng) as longitude,
        
        -- Flag: Was the location patched from seed?
        case 
            when g.zip_code is null and p.zip_code_prefix is not null then true 
            else false 
        end as is_patched_location,
        
        -- Flag: Did we fall back to customer-provided city?
        case 
            when g.zip_code is null and p.zip_code_prefix is null then true 
            else false 
        end as is_fallback_to_customer_data,
        
        -- Flag: Is any geolocation data missing?
        case 
            when g.zip_code is null and p.zip_code_prefix is null then true
            when coalesce(g.lat, p.lat) is null then true
            else false 
        end as is_geolocation_missing,
        
        -- Preserve ingestion timestamp for ordering
        c.ingestion_timestamp

    from customers c
    left join official_geo g 
        on c.customer_zip_code_prefix = g.zip_code
    left join patched_geo p 
        on c.customer_zip_code_prefix = p.zip_code_prefix
),

-- =====================================================
-- CTE 5: Deduplication (handle customers with multiple ZIPs)
-- =====================================================
deduplicated as (
    select
        customer_unique_id,
        customer_zip_code_prefix,
        city,
        state,
        latitude,
        longitude,
        is_patched_location,
        is_fallback_to_customer_data,
        is_geolocation_missing,
        
        -- For customers with multiple ZIP codes, take the most recent
        row_number() over (
            partition by customer_unique_id 
            order by ingestion_timestamp desc
        ) as rn
        
    from joined
),

-- =====================================================
-- CTE 6: Final output with NULL handling
-- =====================================================
final as (
    select
        customer_unique_id,
        
        -- ZIP code with fallback for NULL
        coalesce(customer_zip_code_prefix, '00000') as customer_zip_code_prefix,
        
        -- City with explicit 'Unknown' fallback
        coalesce(city, 'Unknown') as city,
        
        -- State with explicit 'Unknown' fallback
        coalesce(state, 'Unknown') as state,
        
        -- Coordinates (can be NULL if completely missing)
        latitude,
        longitude,
        
        -- Quality flags
        is_patched_location,
        is_fallback_to_customer_data,
        is_geolocation_missing,
        
        -- Metadata
        current_timestamp() as processed_at
        
    from deduplicated
    where rn = 1
)

-- =====================================================
-- FINAL SELECT
-- =====================================================
select * from final