import psycopg2
from urllib.parse import urlparse, uses_netloc
# The following functions are REQUIRED - you should REPLACE their implementation
# with the appropriate code to interact with your PostgreSQL database.
def initialize():
    # this function will get called once, when the application starts.
    # this would be a good place to initalize your connection!
    uses_netloc.append("postgres")
    url = urlparse('postgres://ldhticnp:aJ4kMidrEibot8Syj8OiA9kf8v8qmBXH@isilo.db.elephantsql.com:5432/ldhticnp')

    conn = psycopg2.connect(database=url.path[1:],
        user = url.username,
        password = url.password,
        host = url.hostname,
        port = url.port
        )
    cursor = conn.cursor()
    #cursor.execute('drop table if exists customers; drop table if exists orders; drop table if exists products;')
    cursor.execute('CREATE TABLE IF NOT EXISTS Customers (id SERIAL PRIMARY KEY, firstName Varchar(25), lastName Varchar(25), street Varchar(30), city Varchar(15), state Varchar(15), zip INTEGER);')
    cursor.execute('CREATE TABLE IF NOT EXISTS Products (id SERIAL PRIMARY KEY, name Varchar(25), price REAL);')
    cursor.execute('CREATE TABLE IF NOT EXISTS Orders (id SERIAL PRIMARY KEY, customerId INTEGER, productId INTEGER, date Date, FOREIGN KEY(customerId) REFERENCES Customers(id) ON DELETE CASCADE ON UPDATE CASCADE, FOREIGN KEY(productId) REFERENCES Products(id) ON DELETE CASCADE ON UPDATE CASCADE)')
    conn.commit()

    return conn

conn = initialize()
def get_customers():
    with conn.cursor() as cursor:
        cursor.execute('SELECT * FROM Customers')
        #customers = () atuple of customers
            for customer in cursor:
            customers = {'id':customer[0], 'firstName': customer[1],'lastName': customer[2],'street': customer[3],'city': customer[4],'state': customer[5],'zip': customer[6]}
            yield customers

def get_customer(id):
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM Customers WHERE id = %s',(id,))
    customerInfo = cursor.fetchone()
    customer = {'id':customerInfo[0], 'firstName': customerInfo[1],'lastName': customerInfo[2],'street': customerInfo[3],'city': customerInfo[4],'state': customerInfo[5],'zip': customerInfo[6]}
    conn.commit()
    return customer

def upsert_customer(customer):
    cursor = conn.cursor()
    if 'id' in customer:
        cursor.execute('UPDATE Customers SET firstName = %s, lastName = %s, street=%s, city=%s, state=%s, zip=%s WHERE id=%s', 
        (customer['firstName'], customer['lastName'], customer['street'], customer['city'], customer['state'], customer['zip'], customer['id']))
    else:
        cursor.execute('INSERT INTO Customers(firstName, lastName, street, city, state, zip) VALUES(%s,%s,%s,%s,%s,%s)', (customer['firstName'], customer['lastName'], customer['street'], customer['city'], customer['state'], customer['zip']))
    conn.commit()

def delete_customer(id):
    cursor = conn.cursor()
    cursor.execute('DELETE FROM Customers WHERE id = %s',(id,))
    conn.commit()
    
def get_products():
    with conn.cursor() as cursor:
        cursor.execute('SELECT * FROM Products')
       # products = () #a tuple of products
        for product in cursor:
            products = {'id':product[0], 'name':product[1], 'price':product[2]}   
            yield products

def get_product(id):
   cursor = conn.cursor()
   cursor.execute('SELECT * FROM Products WHERE id=%s',(id,))
   products = cursor.fetchone() 
   product = {'id':products[0], 'name':products[1], 'price':products[2]}
   conn.commit()
   return product

def upsert_product(product):
    cursor = conn.cursor()
    if 'id' in product:
        cursor.execute('UPDATE Products SET name=%s, price=%s WHERE id=%s', (product['name'], product['price'], product['id']))
    else:
        cursor.execute('INSERT INTO Products(name, price) Values(%s,%s)', (product['name'], product['price']))
    conn.commit()

def delete_product(id):
    cursor = conn.cursor()
    cursor.execute('DELETE FROM Products WHERE id=%s',(id,))
    conn.commit()

def get_orders():
    with conn.cursor() as cursor:
        cursor.execute('SELECT * FROM Orders')
   #     orders = ()
        for order in cursor:
            orders = {'id':order[0], 'customerId':order[1], 'productId':order[2], 'date':order[3], 'customer':get_customer(int(order[1])), 'product':get_product(int(order[2]))}
            yield orders

def get_order(id):
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM Orders WHERE id = %s',(id,))
    orders = cursor.fetchone()
    order = {'id':orders[0], 'customerId':orders[1], 'productId':orders[2]}
    return order
    conn.commit()

def upsert_order(order):
    cursor = conn.cursor()
    if 'id' in order:
        cursor.execute('UPDATE Orders SET customerId=%s, productId=%s, date=%s WHERE id=%s', (order['customerId'], order['productId'],order['date'], order['id']),)
    else:
        cursor.execute('INSERT INTO Orders(customerId, productId, date) Values(%s,%s, %s)', (order['customerId'], order['productId'], order['date'],))
    conn.commit()

def delete_order(id):
    cursor = conn.cursor()
    cursor.execute('DELETE From Orders where id=%s',(id,))
    conn.commit()
    
# Return the customer, with a list of orders.  Each order should have a product 
# property as well.
def customer_report(id):
    cursor = conn.cursor()
    customer = get_customer(id)
    cursor.execute('Select * From Orders where customerId=%s', (id,))
    customer_order = list()
    for result in cursor:
        customer_order.append({'id':result[0], 'customerId':result[1], 'productId':result[2], 'date':result[3]})
    report = list()
    for data in customer_order:
        data['product'] = get_product(int(data['productId']))
        report.append(data)
    customer['orders']=report
    print(customer)
    conn.commit()
    return customer


# Return a list of products.  For each product, build
# create and populate a last_order_date, total_sales, and
# gross_revenue property.  Use JOIN and aggregation to avoid
# accessing the database more than once, and retrieving unnecessary
# information
def sales_report():
    with conn.cursor() as cursor:       
        cursor.execute('SELECT name, COUNT(Products.id), price, MAX(date) FROM Products JOIN Orders ON Products.id = Orders.productId GROUP BY Products.id')
        #count = cursor.rowcount
        for data in cursor:
            sales = {'name':data[0], 'total_sales':data[1], 'gross_revenue':data[1]*data[2], 'last_order_date':data[3]}
            yield sales
