with payments as (
    select * from {{ ref('stg_order_payments') }}
    -- Filter out zero or negative payments here
    where payment_value > 0 
)

select
    order_id,
    count(distinct payment_type) as unique_payment_methods,
    sum(payment_value) as total_order_payment,
    max(payment_installments) as max_installments
from payments
group by 1
-- Final Safety: Ensure we only pass orders that have at least 1 payment method
having unique_payment_methods > 0
