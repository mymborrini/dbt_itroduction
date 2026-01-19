-- We use CTE in this case 

{{ config(materialized='table') }}

with customerrevenue as (
    select  c.customerid, 
            concat(c.firstname, ' ', c.lastname) as customername, 
            count(o.orderid) as no_of_orders,
            sum(oi.quantity * oi.unitprice) as revenue
    from customers c 
    join orders o on c.customerid = o.customerid
    join orderitems oi on o.orderid = oi.orderid
    group by c.customerid, customername
    order by revenue desc

)

select customerid, customername, no_of_orders, revenue
from customerrevenue