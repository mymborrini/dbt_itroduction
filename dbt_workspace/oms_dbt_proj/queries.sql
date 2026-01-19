select c.customerid, concat(c.firstname, ' ', c.lastname) as customername, count(o.orderid) as no_of_orders 
analytics-# from customers c
analytics-# join orders o on c.customerid = o.customerid
analytics-# group by c.customerid, customername
analytics-# order by no_of_orders desc;

---