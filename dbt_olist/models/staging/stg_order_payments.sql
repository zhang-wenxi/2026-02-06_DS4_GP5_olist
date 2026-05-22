{{ config(materialized='view') }}

with source as (
    select
        order_id,
        payment_type,
        -- Use 1 as logical minimum for sequential
        coalesce(safe_cast(payment_sequential as int64), 1) as payment_sequential,
        --  NULL installments = 0 (unknown), not 1
        coalesce(safe_cast(payment_installments as int64), 0) as payment_installments,
        --  Handle negative payment values
        case 
            when safe_cast(payment_value as float64) < 0 then 0.0
            else coalesce(safe_cast(payment_value as float64), 0.0)
        end as payment_value,
        -- Fix #2 bonus: Flag negative values for investigation
        (safe_cast(payment_value as float64) < 0) as payment_value_was_negative
    from {{ source('olist', 'order_payments') }}
),

-- 1. Deduplication (Uniqueness)
deduplicated as (
    select 
        *,
        count(*) over (partition by order_id, payment_sequential) as duplicate_count
    from source
    qualify row_number() over (
        partition by order_id, payment_sequential
        order by payment_value desc, payment_type asc
    ) = 1
),

-- 2. Final Staging with Combined Logic
staging as (
    select
        order_id,
        payment_sequential,
        nullif(lower(trim(payment_type)), 'not_defined') as payment_type,
        
        -- Pass through already-cleaned values from source CTE — no double-cleaning
        payment_installments,
        payment_value,
        
        (
            order_id is null 
            or not regexp_contains(order_id, r'^[0-9a-fA-F]{32}$')
        ) as order_id_is_invalid,

        (nullif(lower(trim(payment_type)), 'not_defined') is null) as payment_type_is_missing,

        -- Use the flag captured BEFORE cleaning in source CTE
        payment_value_was_negative as payment_value_invalid,
        
        -- 0 means "was null in source" (coalesced there); that is the invalid case
        (payment_installments = 0) as invalid_installments,
        
        (duplicate_count > 1) as had_duplicates,
        current_timestamp() as ingestion_timestamp
    from deduplicated
)

select * from staging
