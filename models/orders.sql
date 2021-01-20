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
        amount
    from r{{ ref('stg_payments') }}
)
select
    orders.order_id,
    customer_id,
    payments.amount
from orders
left join payments
    on payments.order_id = orders.order_id
