{{ config(materialized='table') }}

with customers_base as (
    -- Get one row per unique human, picking their most recent metadata
    select * from {{ ref('stg_customers') }}
    qualify row_number() over (
        partition by customer_unique_id 
        order by ingestion_timestamp desc
    ) = 1
),

customer_segments as (
    -- This is already at the unique_id grain (outputted as customer_id)
    select * from {{ ref('int_customer_segments') }}
),

location_mapping as (
    -- This uses our patched seed data and is at the unique_id grain
    select * from {{ ref('int_customer_location_mapping') }}
),

customer_order_status as (
    -- Aggregate order history for the HUMAN, not the order ID
    select 
        c.customer_unique_id,
        count(o.order_id) as total_orders,
        logical_and(o.order_status in ('canceled', 'unavailable')) as only_unsuccessful_orders
    from {{ ref('stg_orders') }} o
    join {{ ref('stg_customers') }} c on o.customer_id = c.customer_id
    group by 1
),

final as (
    select
        -- 1. IDs & Personal Info
        c.customer_unique_id as customer_id, 
        
        c.customer_id as latest_order_id_reference,
        l.customer_zip_code_prefix,
        l.city as customer_city,
        l.state as customer_state,
        l.latitude,
        l.longitude,
        l.is_patched_location,

        -- 3. RFV Metrics (Coalesce handles the nulls for new/canceled users)
        coalesce(s.rfv_score, '000') as rfv_score,
        coalesce(s.monetary_value, 0) as lifetime_value,
        coalesce(s.frequency, 0) as lifetime_frequency,
        s.recency as days_since_last_order,
        
        -- 4. Final Segment Logic
        case 
            when s.segment is not null then s.segment
            when c.customer_id_is_invalid then 'Invalid Data Format'
            when o.only_unsuccessful_orders then 'Inactive (Canceled/Unavailable)'
            when o.total_orders = 0 or o.total_orders is null then 'Lead (No Orders)'
            else 'Uncategorized' 
        end as customer_segment,
        
        -- 5. Quality Flags
        c.customer_id_is_invalid,
        coalesce(o.only_unsuccessful_orders, false) as has_only_failed_orders

    from customers_base c
    left join customer_segments s on c.customer_unique_id = s.customer_id
    left join customer_order_status o on c.customer_unique_id = o.customer_unique_id
    left join location_mapping l on c.customer_unique_id = l.customer_unique_id
)

select * from final
