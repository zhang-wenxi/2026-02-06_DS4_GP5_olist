-- =====================================================
-- MODEL: int_customer_segments
-- PURPOSE: Assign customer segments based on RFV quartiles
-- DEPENDENCIES: int_rfv_quartiles
-- BUSINESS RULES: 
--   - Champions: Top quartile in R, F, and V
--   - At Risk: Low recency but high frequency/value
--   - New: Recent but low frequency
--   - Loyal: Mid-high in R and F
--   - Hibernating: Bottom quartile in all
--   - Default: Potential Loyalists
-- =====================================================

{{ config(materialized='view') }}

with rfv_data as (
    -- Source: Pre-calculated RFV quartiles
    -- Note: Excludes canceled/unavailable orders
    select
        customer_id,
        rfv_score,
        r_quartile,
        f_quartile,
        v_quartile,
        frequency,
        monetary_value,
        recency
    from {{ ref('int_rfv_quartiles') }}
),

segment_rules as (
    select
        customer_id,
        rfv_score,
        r_quartile,
        f_quartile,
        v_quartile,
        frequency,
        monetary_value,
        recency,

        -- Business logic: Segment assignment
        -- Order matters: Most specific rules first
        case 
            -- Rule 1: Top performers (4/4/4)
            when r_quartile >= 4 and f_quartile >= 4 and v_quartile >= 4 
                then 'Champions'
            
            -- Rule 2: High value, losing interest (Low R, High F/V)
            when r_quartile <= 2 and f_quartile >= 3 
                then 'At Risk - Cannot Lose Them'
            
            -- Rule 3: promising (High R, Low F)
            when r_quartile >= 4 and f_quartile <= 2 
                then 'New Customers - Promising'
            
            -- Rule 4: Solid mid-tier (Mid-High R & F)
            when r_quartile >= 3 and f_quartile >= 3 
                then 'Loyal Customers'
            
            -- Rule 5: Inactive (Bottom quartile both)
            when r_quartile <= 1 and f_quartile <= 1 
                then 'Hibernating - Lost'
            
            -- Rule 6: Everyone else
            else 'Potential Loyalists'
        end as segment,

        -- Metadata: Track when rules applied
        current_timestamp() as segmented_at

    from rfv_data
)

select * from segment_rules