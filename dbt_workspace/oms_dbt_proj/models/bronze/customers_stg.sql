select customerid, firstname, lastname, email, phone, address, city, state, zipcode, updatedat, concat(firstname, ' ', lastname) as customername
from customers