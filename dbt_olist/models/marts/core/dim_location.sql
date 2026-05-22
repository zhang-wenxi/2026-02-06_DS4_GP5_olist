-- models/marts/core/dim_location.sql
with silver_geo as (
    select * from {{ ref('stg_geolocation') }}
)

select 
    -- 1. Generate the same key used in fct_sales
    {{ dbt_utils.generate_surrogate_key([
        'geolocation_zip_code_prefix', 
        'geolocation_city', 
        'geolocation_state'
    ]) }} as location_id,
    
    -- 2. Use correct column names from your stg_geolocation model
    geolocation_zip_code_prefix,
    geolocation_city,
    geolocation_state
    
from silver_geo
-- 3. Grouping to ensure uniqueness (Primary Key Integrity)
group by 1, 2, 3, 4
