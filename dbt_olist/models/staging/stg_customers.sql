{{ config(materialized='view') }}

with source as (
    select * from {{ source('olist', 'customers') }}
),

deduplicated as (
    select 
        *,
        count(*) over (partition by customer_id) as duplicate_count,
        row_number() over (
            partition by customer_id 
            order by 
                case when customer_unique_id is not null then 0 else 1 end,
                customer_unique_id
        ) as row_num 
    from source
),

unique_records as (
    select 
        * except(row_num),
        case when duplicate_count > 1 then true else false end as had_duplicates
    from deduplicated 
    where row_num = 1
),

final as (
    select
        -- Business keys (cast only, no transformation)
        customer_id,
        customer_unique_id,

        -- ZIP: cast to string and zero-pad — minimal, reversible
        lpad(cast(customer_zip_code_prefix as string), 5, '0') as customer_zip_code_prefix,

        -- City and state: raw values, no accent removal, no normalization
        trim(customer_city)  as customer_city,
        trim(customer_state) as customer_state,

        -- Format validation flags (format checks belong in staging)
        not regexp_contains(customer_id, r'^[0-9a-fA-F]{32}$')
            or customer_id is null                                   as customer_id_is_invalid,
        not regexp_contains(customer_unique_id, r'^[0-9a-fA-F]{32}$')
            or customer_unique_id is null                            as customer_unique_id_is_invalid,

        -- Null / missing flags
        customer_zip_code_prefix is null                             as customer_zip_code_prefix_is_null,
        customer_city is null
            or upper(trim(customer_city)) in ('NA', 'NAN', 'NOT_DEFINED')
            or length(trim(customer_city)) = 0                       as customer_city_is_null,
        customer_state is null
            or upper(trim(customer_state)) in ('NA', 'NAN', 'NOT_DEFINED') as customer_state_is_null,

        -- Audit
        had_duplicates,
        current_timestamp() as ingestion_timestamp
    from unique_records
)

select * from final