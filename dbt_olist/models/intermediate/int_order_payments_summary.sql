with payments as (
    select 
        order_id,
        payment_type,
        payment_value,
        payment_installments,
        -- Include quality flags from staging (use actual column names)
        payment_value_was_negative,
        payment_value_invalid,  -- Correct column name
        order_id_is_invalid
    from {{ ref('stg_order_payments') }}
),

payments_valid as (
    select * from payments
    where payment_value > 0
    and order_id_is_invalid = false
),

filtered_out as (
    select count(*) as excluded_count
    from payments
    where payment_value <= 0 or order_id_is_invalid = true
)

select
    order_id,
    count(distinct payment_type) as unique_payment_methods,
    sum(payment_value) as total_order_payment,
    max(payment_installments) as max_installments,
    (select excluded_count from filtered_out) > 0 as had_excluded_payments  
from payments_valid
group by 1
having unique_payment_methods > 0