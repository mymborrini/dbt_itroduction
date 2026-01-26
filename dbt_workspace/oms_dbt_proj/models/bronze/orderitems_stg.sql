select  orderitemid,
        orderid,
        quantity,
        unitprice,
        productid,
        unitprice * quantity as totalprice,
        updatedat
from orderitems 