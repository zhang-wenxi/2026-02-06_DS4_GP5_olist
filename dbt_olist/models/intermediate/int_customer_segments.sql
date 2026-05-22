with rfv as (
    select * from {{ ref('int_rfv_quartiles') }}
)

select
    customer_id,
    rfv_score,
    r_quartile,
    f_quartile,
    v_quartile,
    frequency,
    monetary_value,
    recency,
    case 
        -- 1. High Value & Recent (The Best)
        when r_quartile >= 4 and f_quartile >= 4 and v_quartile >= 4 then 'Champions'
        
        -- 2. High Frequency/Value but getting older (FIXED SYNTAX)
        when r_quartile <= 2 and f_quartile >= 3 then 'At Risk - Cannot Lose Them'
        
        -- 3. Recent, but low frequency (Newer leads)
        when r_quartile >= 4 and f_quartile <= 2 then 'New Customers - Promising'
        
        -- 4. Mid-range (Loyal but not top tier)
        when r_quartile >= 3 and f_quartile >= 3 then 'Loyal Customers'
        
        -- 5. Low everything
        when r_quartile <= 1 and f_quartile <= 1 then 'Hibernating - Lost'
        
        -- 6. Catch-all for everyone else
        else 'Potential Loyalists'
    end as segment
from rfv
