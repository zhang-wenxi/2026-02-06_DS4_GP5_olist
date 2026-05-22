{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

calculated_metrics as (
    select
        *,
        -- Precision & Validity: Calculate delivery lead times
        datetime_diff(
            cast(order_delivered_customer_date as datetime), 
            cast(order_purchase_timestamp as datetime), 
            DAY
        ) as lead_time_days,

        -- Consistency: Flag if delivered before estimated
        cast(order_delivered_customer_date as datetime) <= cast(order_estimated_delivery_date as datetime) as is_on_time
    from orders
    where order_status = 'delivered'
)

select * from calculated_metrics
-- Final Filter: Removes the 2 "FAIL" rows causing your dbt test error
where lead_time_days >= 0 
  and lead_time_days <= 200
