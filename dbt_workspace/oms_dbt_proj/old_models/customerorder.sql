-- We use CTE in this case 

{{ config(materialized='table') }}

with customerorders as (
    select c.customerid, concat(c.firstname, ' ', c.lastname) as customername, count(o.orderid) as no_of_orders
    from customers c 
    join orders o on c.customerid = o.customerid
    group by c.customerid, customername
    order by no_of_orders desc
)

select customerid, customername, no_of_orders
from customerorders