select  orderitemid,
        orderid,
        quantity,
        unitprice,
        unitprice * quantity as totalprice,
        updatedat
from orderitems