-- how can we dynamical select payment_methods?
-- cann we remove all the white space?

{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}


WITH
payments AS 
(
    select
        *
    from {{ ref('stg_payments')}}
),
pivoted AS
(
    select
        orderid,
        {% for payment_method in payment_methods %}
        sum(case when paymentmethod = '{{ payment_method }}' then amount else 0 end) AS {{ payment_method }}_amt
        {% if not loop.last %}
        ,
        {% else %}

        {% endif %}
        {% endfor %}
    from payments
    group by
        orderid
)
select * from pivoted