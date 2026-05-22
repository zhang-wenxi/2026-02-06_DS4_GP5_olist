{{ config(materialized='view') }}

with source as (
    -- Get everything from the source first
    select * from {{ source('olist', 'order_reviews') }}
),

-- 1. Deduplication: We must use the raw column names here
deduplicated as (
    select 
        *,
        row_number() over (
            partition by review_id 
            -- BigQuery needs to sort by these as timestamps
            order by safe_cast(review_creation_date as timestamp) desc, 
                     safe_cast(review_answer_timestamp as timestamp) desc
        ) as row_num
    from source
    where review_id is not null 
      and order_id is not null
      and regexp_contains(review_id, r'^[0-9a-fA-F]{32}$')
      and regexp_contains(order_id, r'^[0-9a-fA-F]{32}$')
),

-- 2. Cleaning & Standardization
cleaned as (
    select
        review_id,
        order_id,
        
        -- Strict 1-5 Scale: Keep as NULL if missing
        safe_cast(review_score as int64) as review_score,

        -- Accent removal and placeholder handling
        regexp_replace(normalize(
            nullif(nullif(nullif(trim(review_comment_title), 'not_defined'), 'NA'), 'nan'), 
        NFD), r'\pM', '') as review_comment_title,
        
        regexp_replace(normalize(
            nullif(nullif(nullif(trim(review_comment_message), 'not_defined'), 'NA'), 'nan'), 
        NFD), r'\pM', '') as review_comment_message,
        
        -- Now cast the dates for the final output
        safe_cast(review_creation_date as timestamp) as review_creation_date,
        safe_cast(review_answer_timestamp as timestamp) as review_answer_timestamp
    from deduplicated
    where row_num = 1
),

-- 3. Final Staging with Quality Flags
staging as (
    select
        *,
        (review_score is null) as is_review_score_missing,
        (review_comment_message is null) as is_empty_review,
        current_timestamp() as processed_at
    from cleaned
)

select * from staging
