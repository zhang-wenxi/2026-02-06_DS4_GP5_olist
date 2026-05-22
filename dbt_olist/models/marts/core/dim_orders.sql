{{ config(materialized='table') }}

with order_attributes as (
    select distinct
        order_status,
        case 
            when order_status in ('canceled', 'unavailable') then 'Incomplete/Canceled'
            when order_status = 'delivered' then 'Completed'
            else 'In Progress'
        end as order_activity_status
    from {{ ref('stg_orders') }}
)

select
    -- SURROGATE KEY (stable hash from order_status)
    {{ dbt_utils.generate_surrogate_key(['order_status']) }} as order_attribute_key,
    
    -- ATTRIBUTES
    order_status,
    order_activity_status,
    
    -- Metadata
    current_timestamp() as processed_at
    
from order_attributes