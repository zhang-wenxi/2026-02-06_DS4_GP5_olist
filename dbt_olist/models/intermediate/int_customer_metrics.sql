with orders as (
    select * from {{ ref('stg_orders') }}
    where order_status in ('delivered', 'invoiced', 'shipped', 'approved')
),

-- NEW: Join to staging customers to get the REAL unique person ID
customers as (
    select 
        customer_id, 
        customer_unique_id 
    from {{ ref('stg_customers') }}
),

order_items as (
    select 
        order_id,
        sum(price) as item_monetary_value
    from {{ ref('stg_order_items') }}
    group by 1
),

payments as (
    select 
        order_id,
        sum(payment_value) as payment_monetary_value,
        count(distinct payment_type) as unique_payment_methods
    from {{ ref('stg_order_payments') }}
    group by 1
    having sum(payment_value) > 0 
       and count(distinct payment_type) > 0
),

metrics as (
    select
        -- GROUP BY UNIQUE ID: This is what allows Frequency > 1
        c.customer_unique_id,
        count(distinct o.order_id) as frequency,
        max(o.order_purchase_timestamp) as last_purchase_date,
        
        -- Summing total spend for the person across all orders
        round(
            cast(sum(coalesce(oi.item_monetary_value, p.payment_monetary_value)) as numeric), 
            2
        ) as monetary_value
        
    from orders o
    inner join customers c on o.customer_id = c.customer_id
    inner join payments p on o.order_id = p.order_id
    left join order_items oi on o.order_id = oi.order_id
    group by 1
)

select
    customer_unique_id as customer_id, -- Make sure this matches the CTE column name
    frequency,
    monetary_value,
    -- This calculates recency based on the dataset's 'present day'
    date_diff(
        (select max(cast(last_purchase_date as date)) from metrics), 
        cast(last_purchase_date as date), 
        DAY
    ) as recency
from metrics
