{{ config(materialized='table') }}

with customers_base as (
    select 
        *,
        {{ dbt_utils.generate_surrogate_key(['customer_unique_id']) }} as customer_key
    from {{ ref('stg_customers') }}
    qualify row_number() over (
        partition by customer_unique_id 
        order by ingestion_timestamp desc
    ) = 1
),

customer_segments as (
    select * from {{ ref('int_customer_segments') }}
),

location_mapping as (
    select * from {{ ref('int_customer_location_mapping') }}
),

customer_order_status as (
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
        -- SURROGATE KEY (Primary Key for this dimension)
        c.customer_key,
        
        -- BUSINESS KEY (for reference/debugging)
        c.customer_unique_id as customer_id,
        c.customer_id as latest_order_id_reference,
        
        -- Attributes
        l.customer_zip_code_prefix,
        l.city as customer_city,
        l.state as customer_state,
        l.latitude,
        l.longitude,
        l.is_patched_location,

        -- RFV Metrics
        coalesce(s.rfv_score, '000') as rfv_score,
        coalesce(s.monetary_value, 0) as lifetime_value,
        coalesce(s.frequency, 0) as lifetime_frequency,
        s.recency as days_since_last_order,
        
        -- Segment
        case 
            when s.segment is not null then s.segment
            when c.customer_id_is_invalid then 'Invalid Data Format'
            when o.only_unsuccessful_orders then 'Inactive (Canceled/Unavailable)'
            when o.total_orders = 0 or o.total_orders is null then 'Lead (No Orders)'
            else 'Uncategorized' 
        end as customer_segment,
        
        -- Quality Flags
        c.customer_id_is_invalid,
        coalesce(o.only_unsuccessful_orders, false) as has_only_failed_orders,
        
        -- Metadata
        current_timestamp() as valid_from,
        null as valid_to,
        true as is_current,
        c.ingestion_timestamp as processed_at

    from customers_base c
    left join customer_segments s on c.customer_unique_id = s.customer_id
    left join customer_order_status o on c.customer_unique_id = o.customer_unique_id
    left join location_mapping l on c.customer_unique_id = l.customer_unique_id
)

select * from final