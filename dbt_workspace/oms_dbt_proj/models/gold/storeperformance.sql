select
        os.storeid,
        sum(ofact.revenue) as actualsales,
        sum(st.salestarget) as targetsales
from
        {{ ref('orders_stg') }} os
join
        {{ ref('orders_fact') }} ofact on os.orderid = ofact.orderid
join
        {{ ref('salestargets') }} st on st.storeid = os.storeid
group by
        os.storeid