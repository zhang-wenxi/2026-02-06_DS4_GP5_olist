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

cleaned_data as (
    select
        -- IDs and Format Checks (Your original Silver logic)
        customer_id as raw_customer_id,
        customer_unique_id as raw_customer_unique_id,

        regexp_contains(customer_id, r'^[0-9a-fA-F]{32}$') as is_valid_cid_format,
        regexp_contains(customer_unique_id, r'^[0-9a-fA-F]{32}$') as is_valid_uuid_format,

        -- ZIP code: Formatted as 5-digit string
        lpad(cast(customer_zip_code_prefix as string), 5, '0') as customer_zip_code_prefix,

        -- City: Original Normalization Logic
        case 
            when customer_city is null 
                 or upper(trim(customer_city)) in ('NA', 'NAN', 'NOT_DEFINED') then 'Unknown'
            
            -- Special case: 'maceia' appears in source data due to encoding issue with 'ç'
            -- Mapping to correct city 'maceio'
            when lower(trim(customer_city)) like 'maceia%' then 'maceio'
            
            else 
                -- FIX: Changed \u to \xB so BigQuery doesn't crash
                regexp_replace(
                    normalize(trim(customer_city), NFD), 
                    r'[\p{M}\xB2\xB3\xB9]', ''
                )
        end as customer_city,

        -- State: Standardize and default to 'Unknown'
        case 
            when customer_state is null 
                 or length(trim(customer_state)) != 2 
                 or not regexp_contains(customer_state, r'^[A-Za-z]{2}$')
                 or upper(trim(customer_state)) in ('NA', 'NAN', 'NOT_DEFINED') then 'Unknown'
            else upper(trim(customer_state))
        end as customer_state,

        had_duplicates,
        customer_city as raw_city_for_empty_check,
        customer_zip_code_prefix as raw_zip_for_range_check
    from unique_records
),

final_enhanced_output as (
    select
        -- Core Columns
        raw_customer_id as customer_id,
        raw_customer_unique_id as customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,

        -- All Original Quality Flags Restored
        case when not is_valid_cid_format or raw_customer_id is null then true else false end as customer_id_is_invalid,
        case when not is_valid_uuid_format or raw_customer_unique_id is null then true else false end as customer_unique_id_is_invalid,
        
        case when customer_zip_code_prefix is null then true else false end as customer_zip_code_prefix_is_null,
        case 
            when cast(raw_zip_for_range_check as int64) < 1000 
                 or cast(raw_zip_for_range_check as int64) > 99999 
            then true else false 
        end as customer_zip_code_prefix_invalid_range,
        case when length(customer_zip_code_prefix) != 5 then true else false end as customer_zip_code_prefix_invalid_length,
        
        case when customer_city = 'Unknown' then true else false end as customer_city_is_null,
        case when length(trim(coalesce(raw_city_for_empty_check, ''))) = 0 then true else false end as customer_city_is_empty,
        case when customer_state = 'Unknown' then true else false end as customer_state_is_null,

        had_duplicates,
        current_timestamp() as ingestion_timestamp
    from cleaned_data
)

select * from final_enhanced_output
