select orderid
from {{ ref('orders_fact') }}
where revenue < 0