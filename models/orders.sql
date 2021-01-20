WITH
orders as (
    select 
        order_id,
        customer_id
    from {{ ref('stg_orders') }}
),
payments AS (
    select
        orderid AS order_id,
        SUM(amount) / 100 AS amount_usd
    from {{ ref('stg_payments') }}
    where 
        status = 'success'
    group by 
        order_id
)
select
    orders.order_id,
    customer_id,
    payments.amount_usd
from orders
left join payments
    on payments.order_id = orders.order_id
ORDER BY order_id ASC