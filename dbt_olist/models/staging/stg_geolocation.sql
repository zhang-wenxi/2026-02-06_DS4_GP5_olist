{{ config(materialized='view') }}

with source as (
    select * from {{ source('olist', 'geolocation') }}
),

-- 1. Deduplication using QUALIFY
deduplicated as (
    select 
        *,
        count(*) over (partition by geolocation_zip_code_prefix) as duplicate_count
    from source
    qualify row_number() over (
        partition by geolocation_zip_code_prefix 
        order by 
            case when geolocation_city is not null then 0 else 1 end,
            case when geolocation_state is not null then 0 else 1 end,
            case when geolocation_lat is not null then 0 else 1 end,
            case when geolocation_lng is not null then 0 else 1 end,
            geolocation_city
    ) = 1
),

-- 2. Initial Standardization & Accent Removal
standardized_base as (
    select
        -- ZIP: 5-digit string format
        lpad(cast(geolocation_zip_code_prefix as string), 5, '0') as geolocation_zip_code_prefix,
        
        -- City: Trim, Lowercase, and Remove Accents
        lower(trim(regexp_replace(normalize(geolocation_city, NFD), r'\pM', ''))) as cleaned_city,
        
        -- State: Trim and Uppercase
        upper(trim(geolocation_state)) as cleaned_state,
        
        -- Ensure coordinates are floats for comparison
        safe_cast(geolocation_lat as float64) as geolocation_lat,
        safe_cast(geolocation_lng as float64) as geolocation_lng,
        
        duplicate_count > 1 as had_duplicates,
        current_timestamp() as ingestion_timestamp
    from deduplicated
),

-- 3. Data Quality Assessment
data_quality_assessment as (
    select 
        *,
        -- ZIP Validation: Explicitly cast to INT64 for range check
        case 
            when geolocation_zip_code_prefix is null then 'ZIP_NULL'
            when not regexp_contains(geolocation_zip_code_prefix, r'^[0-9]{5}$') then 'ZIP_INVALID_FORMAT'
            when safe_cast(geolocation_zip_code_prefix as int64) < 1000 
              or safe_cast(geolocation_zip_code_prefix as int64) > 99999 then 'ZIP_OUT_OF_RANGE'
            else 'ZIP_VALID'
        end as zip_status,

        -- City Validation
        case 
            when cleaned_city is null or length(cleaned_city) < 2 then 'CITY_INVALID'
            when regexp_contains(cleaned_city, r'[0-9]') then 'CITY_HAS_NUMBERS'
            else 'CITY_VALID'
        end as city_status,

        -- State Validation
        case 
            when cleaned_state is null then 'STATE_NULL'
            when not regexp_contains(cleaned_state, r'^[A-Z]{2}$') then 'STATE_INVALID_FORMAT'
            else 'STATE_VALID'
        end as state_status,

        -- Coordinate Validation (Brazil Bounds Check)
        case 
            when geolocation_lat is null or geolocation_lat < -35 or geolocation_lat > 5 then 'LAT_INVALID'
            else 'LAT_VALID'
        end as lat_status,

        case 
            when geolocation_lng is null or geolocation_lng < -75 or geolocation_lng > -30 then 'LNG_INVALID'
            else 'LNG_VALID'
        end as lng_status
    from standardized_base
),

-- 4. Final Enhanced Output
final_enhanced_output as (
    select
        geolocation_zip_code_prefix,
        cleaned_city as geolocation_city,
        cleaned_state as geolocation_state,
        geolocation_lat,
        geolocation_lng,

        -- Quality Flags 
        zip_status != 'ZIP_VALID' as zip_code_is_invalid,
        city_status != 'CITY_VALID' as city_is_invalid,
        state_status != 'STATE_VALID' as state_is_invalid,
        (lat_status != 'LAT_VALID' or lng_status != 'LNG_VALID') as coordinates_are_invalid,

        had_duplicates,
        ingestion_timestamp,
        
        -- Combined Log
        (
          select array_to_string(array_agg(x), '|')
          from unnest([
              if(zip_status != 'ZIP_VALID', zip_status, null),
              if(city_status != 'CITY_VALID', city_status, null),
              if(state_status != 'STATE_VALID', state_status, null),
              if(lat_status != 'LAT_VALID', lat_status, null),
              if(lng_status != 'LNG_VALID', lng_status, null)
          ]) as x where x is not null
        ) as quality_issue_log
    from data_quality_assessment
)

select * from final_enhanced_output
