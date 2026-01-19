select  orderid,
        orderdate,
        customerid,
        storeid,
        status as statuscd,
        case
            when status = '01' then 'In Progress'
            when status = '02' then 'Completed'
            when status = '03' then 'Cancelled'
            else NULL
        end as statusdesc,
        updatedat
from orders