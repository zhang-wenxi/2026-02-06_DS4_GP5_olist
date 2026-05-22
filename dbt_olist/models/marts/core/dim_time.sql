{{ config(materialized='table') }}

with date_spine as (
    -- Generate date range from your data
    select distinct 
        cast(order_purchase_timestamp as date) as full_date
    from {{ ref('stg_orders') }}
    
    union distinct
    
    select distinct 
        cast(order_delivered_customer_date as date) as full_date
    from {{ ref('stg_orders') }}
    where order_delivered_customer_date is not null
),

final as (
    select
        -- SURROGATE KEY (stable hash from full_date)
        {{ dbt_utils.generate_surrogate_key(['full_date']) }} as time_key,
        
        -- NATURAL KEY
        full_date as order_date,
        
        -- Date attributes
        extract(year from full_date) as year,
        extract(month from full_date) as month,
        extract(day from full_date) as day,
        extract(quarter from full_date) as quarter,
        format_date('%B', full_date) as month_name,
        extract(dayofweek from full_date) - 1 as weekday,
        extract(week from full_date) as week_number,
        
        -- Business flags
        case when extract(dayofweek from full_date) in (1, 7) then true else false end as is_weekend,
        case when extract(month from full_date) in (11, 12) then true else false end as is_peak_season
        
    from date_spine
    where full_date is not null
)

select * from final