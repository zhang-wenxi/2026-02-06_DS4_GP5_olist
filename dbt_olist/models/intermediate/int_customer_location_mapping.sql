with customers as (
    -- Get raw customer data with the Unique ID
    select * from {{ ref('stg_customers') }}
),

official_geo as (
    select 
        geolocation_zip_code_prefix as zip_code,
        geolocation_city as city,
        geolocation_state as state,
        geolocation_lat as lat,
        geolocation_lng as lng
    from {{ ref('stg_geolocation') }}
    qualify row_number() over (partition by geolocation_zip_code_prefix order by geolocation_city) = 1
),

patched_geo as (
    -- Your dbt seed file
    select * from {{ ref('patch_missing_geolocations') }}
),

final as (
    select
        -- 1. Use the Unique ID as the grain
        c.customer_unique_id,
        c.customer_zip_code_prefix,
        
        -- 2. Fallback Logic: Official -> Seed Patch -> Customer Staging
        coalesce(g.city, p.city, c.customer_city) as city,
        coalesce(g.state, p.state, c.customer_state) as state,
        coalesce(g.lat, p.lat) as latitude,
        coalesce(g.lng, p.lng) as longitude,
        
        -- 3. Patch Flag
        case when g.zip_code is null and p.zip_code_prefix is not null then true else false end as is_patched_location

    from customers c
    left join official_geo g on c.customer_zip_code_prefix = g.zip_code
    left join patched_geo p on c.customer_zip_code_prefix = p.zip_code_prefix
    
    -- 4. FIX: If a customer has 3 orders with 3 different ZIPs, pick the most recent one
    -- This prevents duplicates in your dim_customers
    qualify row_number() over (
        partition by c.customer_unique_id 
        order by c.ingestion_timestamp desc
    ) = 1
)

select * from final
