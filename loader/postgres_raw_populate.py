import random
from datetime import datetime, timedelta, UTC

import psycopg2
from faker import Faker
import faker_commerce


print("Starting Raw Populate")
# ======================
# CONFIG
# ======================
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "analytics",   
    "user": "dbt",    
    "password": "dbt" 
}

CUSTOMERS_COUNT = 50
EMPLOYEES_COUNT = 20
ORDERS_COUNT = 1000
PRODUCTS_COUNT = 100
ORDER_ITEMS_COUNT = 5000
STORES_COUNT = 10
SUPPLIERS_COUNT = 5

fake = Faker("it_IT")
fake.add_provider(faker_commerce.Provider)

# ======================
# DB CONNECTION
# ======================
conn = psycopg2.connect(**DB_CONFIG)
conn.autocommit = True
cur = conn.cursor()

print("✔ Connected to db successfully")

# ======================
# TABLE CREATION
# ======================
cur.execute("""            
DROP TABLE IF EXISTS orderitems CASCADE;

DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS orders CASCADE;

DROP TABLE IF EXISTS suppliers CASCADE;
DROP TABLE IF EXISTS employees CASCADE;
DROP TABLE IF EXISTS stores CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
""")

print("✔ Tables dropped")

cur.execute("""
CREATE TABLE customers (
    customerid INT PRIMARY KEY,
    firstname VARCHAR(100),
    lastname VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(100),
    address VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    zipcode VARCHAR(100),
    updatedat TIMESTAMP
);
""")

cur.execute("""
CREATE TABLE stores (
    storeid INT PRIMARY KEY,
    storename VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(100),
    address VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    zipcode VARCHAR(100),
    updatedat TIMESTAMP
);
""")

cur.execute("""
CREATE TABLE suppliers (
    supplierid INT PRIMARY KEY,
    suppliername VARCHAR(100),
    contactperson VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(100),
    address VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    zipcode VARCHAR(100),
    updatedat TIMESTAMP
);
""")

cur.execute("""
CREATE TABLE employees (
    employeeid INT PRIMARY KEY,
    firstname VARCHAR(100),
    lastname VARCHAR(100),
    email VARCHAR(100),
    jobtitle VARCHAR(100),
    hiredate DATE,
    managerid INT REFERENCES employees(employeeid),
    address VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    zipcode VARCHAR(100),
    updatedat TIMESTAMP
);
""")

cur.execute("""
CREATE TABLE orders (
    orderid INT PRIMARY KEY,
    orderdate DATE,
    customerid INT REFERENCES customers(customerid),
    storeid INT REFERENCES stores(storeid),
    employeeid INT REFERENCES employees(employeeid),
    status VARCHAR(100),
    updatedat TIMESTAMP
);
""")


cur.execute("""
CREATE TABLE products (
    productid INT PRIMARY KEY,
    name VARCHAR(100),
    category VARCHAR(100),
    retailprice DECIMAL(10,2),
    supplierprice DECIMAL(10,2),
    supplierid INT REFERENCES suppliers(supplierid),
    updatedat TIMESTAMP
);
""")

cur.execute("""
CREATE TABLE orderitems (
    orderitemid INT PRIMARY KEY,
    orderid INT REFERENCES orders(orderid),
    productid INT REFERENCES products(productid),
    quantity INT,
    unitprice DECIMAL(10,2),
    updatedat TIMESTAMP
);
""")

print("✔ Tables created")

# ======================
# INSERT CUSTOMERS
# ======================
customers = []

for cid in range(1, CUSTOMERS_COUNT + 1):
    customers.append((
        cid,
        fake.first_name(),
        fake.last_name(),
        fake.email(),
        fake.phone_number(),
        fake.street_address(),
        fake.city(),
        fake.state(),
        fake.postcode(),
        datetime.now(UTC)
    ))

cur.executemany("""
INSERT INTO customers (
    customerid, firstname, lastname, email, phone,
    address, city, state, zipcode, updatedat
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
""", customers)

print(f"✔ {CUSTOMERS_COUNT} customers inserted")


# ======================
# INSERT STORES
# ======================

store_types = [
        "Market", "Shop", "Store", "Mart", "Boutique", "Outlet",
        "Emporium", "Depot", "Bazaar", "Corner", "Center", "Gallery"
    ]

stores = []
for sid in range(1, STORES_COUNT + 1):
    stores.append((
        sid,
        f"{fake.company()} {random.choice(store_types)}",
        fake.email(),
        fake.phone_number(),
        fake.street_address(),
        fake.city(),
        fake.state(),
        fake.postcode(),
        datetime.now(UTC)
    ))

cur.executemany("""
INSERT INTO stores (
    storeid, storename, email, phone,
    address, city, state, zipcode, updatedat
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
""", stores)

print(f"✔ {STORES_COUNT} stores inserted")


# ======================
# INSERT SUPPLIERS
# ======================
suppliers = []

for sid in range(1, SUPPLIERS_COUNT + 1):
    suppliers.append((
        sid,
        fake.company(),
        fake.first_name() + ' ' + fake.last_name(),
        fake.email(),
        fake.phone_number(),
        fake.street_address(),
        fake.city(),
        fake.state(),
        fake.postcode(),
        datetime.now(UTC)
    ))

cur.executemany("""
INSERT INTO suppliers (
    supplierid,
    suppliername,
    contactperson,
    email,
    phone,
    address,
    city,
    state,
    zipcode,
    updatedat
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
""", suppliers)

print(f"✔ {SUPPLIERS_COUNT} suppliers inserted")



# ======================
# INSERT EMPLOYEES
# ======================
employees = []

job_titles = ['Worker', 'Manager', 'Leader']

for eid in range(1, EMPLOYEES_COUNT + 1):
    employees.append((
        eid,
        fake.first_name(),
        fake.last_name(),
        fake.email(),
        random.choice(job_titles),
        fake.date_between(start_date="-2y", end_date="today"),
        random.randint(1, eid - 1 or 1),  # MTO
        fake.street_address(),
        fake.city(),
        fake.state(),
        fake.postcode(),
        datetime.now(UTC)
    ))

cur.executemany("""
INSERT INTO employees (
    employeeid,
    firstname,
    lastname,
    email,
    jobtitle,
    hiredate,
    managerid,
    address,
    city,
    state,
    zipcode,
    updatedat
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s, %s)
""", employees)

print(f"✔ {EMPLOYEES_COUNT} employees inserted")

# ======================
# INSERT ORDERS
# ======================
statuses = ["01", "02", "03", None]

orders = []

for oid in range(1, ORDERS_COUNT + 1):
    orders.append((
        oid,
        fake.date_between(start_date="-2y", end_date="today"),
        random.randint(1, CUSTOMERS_COUNT),  # MTO
        random.randint(1, STORES_COUNT),  # MTO
        random.randint(1, EMPLOYEES_COUNT),  # MTO
        random.choice(statuses),
        datetime.now(UTC)
    ))

cur.executemany("""
INSERT INTO orders (
    orderid, orderdate, customerid, storeid, employeeid, status, updatedat
) VALUES (%s,%s,%s,%s,%s, %s, %s)
""", orders)

print(f"✔ {ORDERS_COUNT} orders inserted")

# ======================
# INSERT PRODUCTS
# ======================
category = ["01", "02", "03", None]

products = []

for pid in range(1, PRODUCTS_COUNT + 1):
    products.append((
        pid,
        fake.ecommerce_name(),
        random.choice(category),
        round(random.uniform(10, 50), 2),
        round(random.uniform(10, 50), 2),
        random.randint(1, SUPPLIERS_COUNT),  # MTO
        datetime.now(UTC)
    ))


cur.executemany("""
INSERT INTO products (
    productid,
    name,
    category,
    retailprice,
    supplierprice,
    supplierid,
    updatedat
) VALUES (%s,%s,%s,%s,%s,%s,%s)
""", products)

print(f"✔ {PRODUCTS_COUNT} products inserted")



# ======================
# INSERT ORDER ITEMS
# ======================

orderItems = []

for oitid in range(1, ORDER_ITEMS_COUNT + 1):
    orderItems.append((
        oitid,
        random.randint(1, ORDERS_COUNT),  # MTO
        random.randint(1, PRODUCTS_COUNT),  # 
        random.randint(1, 10),
        round(random.uniform(10, 50), 2),
        datetime.now(UTC)
    ))

cur.executemany("""
INSERT INTO orderitems (
    orderitemid, orderid, productid, quantity, unitprice, updatedat
) VALUES (%s,%s,%s,%s,%s, %s)
""", orderItems)

print(f"✔ {ORDER_ITEMS_COUNT} order items inserted")

# ======================
# CLEANUP
# ======================
cur.close()
conn.close()

print("🎉 Database properly populated")
