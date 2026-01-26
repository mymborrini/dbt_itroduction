{{ config(materialized='table') }}


select  o.orderid,
        o.orderdate,
        o.customerid,
        o.statuscd,
        o.statusdesc,
        o.updatedat,
        count(o.orderid) as ordercount,
        sum(oi.totalprice) as revenue,
        sum(oi.quantity * p.supplierprice) as cost
from
        {{ ref('orders_stg') }} o
join
        {{ ref('orderitems_stg') }} oi on o.orderid = oi.orderid
join 
        {{ ref('products_stg') }} p on p.productid = oi.productid
group by
        o.orderid,
        o.orderdate,
        o.customerid,
        o.statuscd,
        o.statusdesc,
        o.updatedat