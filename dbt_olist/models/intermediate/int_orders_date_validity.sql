{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

calculated_metrics as (
    select
        *,
        -- Calculate lead time in days
        date_diff(
            date(order_delivered_customer_date), 
            date(order_purchase_timestamp), 
            day
        ) as lead_time_days,
        
        -- Flag if delivered on time
        order_delivered_customer_date <= order_estimated_delivery_date as is_on_time,
        
        -- Quality flags instead of filtering
        case 
            when date_diff(
                date(order_delivered_customer_date), 
                date(order_purchase_timestamp), 
                day
            ) < 0 then true 
            else false 
        end as lead_time_negative_flag,
        
        case 
            when date_diff(
                date(order_delivered_customer_date), 
                date(order_purchase_timestamp), 
                day
            ) > 200 then true 
            else false 
        end as lead_time_outlier_flag
    from orders
    where order_status = 'delivered'
)

select * from calculated_metrics
-- NO FILTERING - Let downstream decide
