{% macro generate_profit_model(customer_state_query) %}

select
    o.orderdate,
    sum(o.revenue) as total_revenue,
    sum(o.cost) as total_cost
from 
    {{ ref('orders_fact') }} o
join
    {{ ref('customers_stg') }} c on c.customerid = o.customerid
where
    c.state = ( {{ customer_state_query }} )
group by
    o.orderdate

{% endmacro %}