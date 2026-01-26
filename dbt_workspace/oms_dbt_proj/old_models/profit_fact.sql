select
    o.orderdate,
    sum(o.revenue) as total_revenue,
    sum(o.cost) as total_cost
from 
    {{ ref('orders_fact') }} o
group by
    o.orderdate


