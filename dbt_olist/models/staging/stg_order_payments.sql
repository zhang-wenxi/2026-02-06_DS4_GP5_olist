{{ config(materialized='view') }}

with source as (
    select
        order_id,
        payment_type,
        -- Use 1 as logical minimum for sequential/installments (Databricks standard)
        coalesce(safe_cast(payment_sequential as int64), 1) as payment_sequential,
        coalesce(safe_cast(payment_installments as int64), 1) as payment_installments,
        coalesce(safe_cast(payment_value as float64), 0.0) as payment_value
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
        -- Original ID & Sequence
        order_id,
        payment_sequential,
        
        -- Clean type but keep for auditing
        nullif(lower(trim(payment_type)), 'not_defined') as payment_type,
        
        -- Use original logic but ensure no "funny" zeros/negatives in installments
        case when payment_installments < 1 then 1 else payment_installments end as payment_installments,
        case when payment_value < 0 then 0.0 else payment_value end as payment_value,
        
        -- COMBINED FLAGS: Logic + Null check (Prevents overwriting)
        (
            order_id is null 
            or not regexp_contains(order_id, r'^[0-9a-fA-F]{32}$')
        ) as order_id_is_invalid,

        (payment_type is null) as payment_type_is_missing,

        (
            payment_value is null 
            or payment_value < 0
        ) as payment_value_invalid,
        
        (
            payment_installments is null 
            or payment_installments < 1
        ) as invalid_installments,
        
        -- Audit Fields
        (duplicate_count > 1) as had_duplicates,
        current_timestamp() as ingestion_timestamp
    from deduplicated
)

select * from staging
