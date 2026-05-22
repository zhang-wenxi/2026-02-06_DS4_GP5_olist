{{ config(materialized='table') }}

with orders as (
    -- Reference your staging orders to get all unique purchase dates
    select distinct 
        cast(order_purchase_timestamp as date) as time_id 
    from {{ ref('stg_orders') }}
),

final as (
    select
        time_id,
        -- BigQuery Date Extractions
        extract(year from time_id) as year,
        extract(month from time_id) as month,
        extract(day from time_id) as day,
        extract(quarter from time_id) as trimestre,
        
        -- BigQuery Day of Week (1 is Sunday, 7 is Saturday)
        -- To match Databricks Weekday logic (0-6), we subtract 1
        extract(dayofweek from time_id) - 1 as weekday,
        
        -- ISO Week Number
        extract(isoweek from time_id) as week_number,
        
        -- Logic: Weekend check (Saturday = 7, Sunday = 1 in BigQuery)
        case 
            when extract(dayofweek from time_id) in (1, 7) then true 
            else false 
        end as is_weekend

    from orders
)

select * from final
-- Ensure the time dimension is sorted for BI performance
order by time_id desc
