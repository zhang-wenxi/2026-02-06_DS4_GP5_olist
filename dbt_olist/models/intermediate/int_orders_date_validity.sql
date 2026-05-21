{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

calculated_metrics as (
    select
        *,
        datetime_diff(...) as lead_time_days,
        cast(order_delivered_customer_date <= order_estimated_delivery_date as bool) as is_on_time,
        
        -- Quality flags instead of filtering
        case 
            when datetime_diff(...) < 0 then true 
            else false 
        end as lead_time_negative_flag,
        
        case 
            when datetime_diff(...) > 200 then true 
            else false 
        end as lead_time_outlier_flag
    from orders
    where order_status = 'delivered'
)

select * from calculated_metrics
-- NO FILTERING - Let downstream decide
