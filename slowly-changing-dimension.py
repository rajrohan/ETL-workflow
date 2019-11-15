import psycopg2
import pygrametl
from pygrametl.tables import TypeOneSlowlyChangingDimension
import mysql.connector


# Input is a list of "rows" which in pygrametl is modelled as dict
products = [
    {'ProductID': 1, 'Product_Name' : 'Adjustable Race', 'Product_Category' : 'Accessories', 'Product_Price' : 5}
]

cnx = mysql.connector.connect(user='root1', password='root123',
                              host='127.0.0.1', database='product_sales',
                              auth_plugin='mysql_native_password')

# This ConnectionWrapper will be set as default and is then implicitly used.
# A reference to the wrapper is saved to allow for easy access of it later
conn = pygrametl.ConnectionWrapper(connection=cnx)
# An instance of a Type 1 slowly changing dimension is created with 'price'
# as a slowly changing attribute.
productDimension = TypeOneSlowlyChangingDimension (
    name='dim_product',
    key='ProductID',
    attributes=['ProductID','Product_Name', 'Product_Price','Product_Category'],
    lookupatts=['ProductID'],
    type1atts=['Product_Price'])

#type1atts: A sequence of attributes that should have type1 updates
#applied, it cannot intersect with lookupatts. If not given, it is
#assumed that type1atts = attributes - lookupatts
# scdensure determines whether the row already exists in the database
# and either inserts a new row, or updates the changed attributes in the
# existing row.

cursor=cnx.cursor()
cursor.execute("SELECT * FROM product_sales.dim_product where ProductID=1")
myresult = cursor.fetchall()

for x in myresult:
  print(x)
for row in products:
    productDimension.scdensure(row)
    print(row)

conn.commit()



cursor.execute("SELECT * FROM product_sales.dim_product where ProductID=1")
myresult = cursor.fetchall()

for x in myresult:
  print(x)
# To ensure all cached data is inserted and the transaction committed
# both the commit and close function should be called when done
conn.commit()
conn.close()