import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import errorcode
from prefect import task,Flow
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings("ignore")

@task
def extract():
    # Loading the dataset from csv File or also from mysql database
    complete_df = pd.read_csv("./data/Clothing_Sales_Data_Unique_category.csv")
    return complete_df

@task
def transform(clothing_df):
    # Remove all the special character from numeric values like customer Id, price , quantity etc
    
    # transforing or removing the non-numeric from numeric value
    # type cast is not performed here because of the error of float nan
    # first we need to fix that then convert the dtypes
    
    #clothing_df['ProductID'] = clothing_df.ProductID.str.extract('(\d+)')    
    clothing_df['Product_Price'] = clothing_df.Product_Price.str.extract('(\d+)')
    clothing_df['Quantity'] = clothing_df.Quantity.str.extract('(\d+)')
    
    # drop all the nan value or convert to something like -1
    
    clothing_df['Employee_ID'] = clothing_df['Employee_ID'].replace(np.nan, -1, regex=True)
    clothing_df['Employee_ID'].astype(int)
    
    clothing_df['CustomerID'] = clothing_df['CustomerID'].replace(np.nan, -1, regex=True)
    clothing_df['CustomerID'].astype(int)
    
    clothing_df['ProductID'] = clothing_df['ProductID'].replace(np.nan, -1, regex=True)
    clothing_df['ProductID'].astype(int)
    
    # clothing_df = clothing_df.dropna(subset=['Product_Price'])    #to remove
    clothing_df['Product_Price'] = clothing_df['Product_Price'].replace(np.nan, -1, regex=True)
    clothing_df['Product_Price'] = clothing_df['Product_Price'].replace('0', -1)
    clothing_df['Product_Price'].astype(float)
    
    
    clothing_df['Quantity'] = clothing_df['Quantity'].replace(np.nan, -1, regex=True)
    clothing_df['Quantity'].astype(int)
    
    # staging in flat files
    clothing_df.to_csv("./data/stage/Clothing_Sales_Data_Clean_transform.csv")
    
    
    
    return clothing_df

@task
def load(df_tosql):
    
    tableName = 'clothing_sales_clean'
    sqlEngine = create_engine('mysql+pymysql://root:root123@localhost/product_sales')
    dbConnection    = sqlEngine.connect()
    try:
        frame = df_tosql.to_sql('clothing_sales_clean', con = dbConnection, if_exists = 'replace',chunksize = 1000 )
        
    except ValueError as vx:
        print(vx)
    except Exception as ex: 
        print(ex)

    else:
        print("Table %s created successfully."%tableName);
    finally:
        dbConnection.close()

@task
def starSchema(complete_df):
    # Dimention and fact staging file name
    dim_customer_df = complete_df.loc[ : , ['CustomerID', 'Customer_Name', 'Customer_Middle', 
    'Customer_LastName'] ]
    dim_customer_df = dim_customer_df.sort_values(by = ['CustomerID']).drop_duplicates(['CustomerID'])
    dim_customer_df.to_csv('./data/stage/dim_customer.csv',index = False)

    dim_employee_df = complete_df.loc[ : , ['Employee_ID', 'Employee_Name', 'Employee_Middle', 
    'Employee_Lastname'] ]
    dim_employee_df = dim_employee_df.sort_values(by = ['Employee_ID']).drop_duplicates(['Employee_ID'])
    dim_employee_df.to_csv('./data/stage/dim_employee.csv',index = False)

    dim_product_df = complete_df.loc[ : , ['ProductID', 'Product_Name', 'Product_Price',
    'Product_Category'] ]
    dim_product_df = dim_product_df.sort_values(by = ['ProductID']).drop_duplicates(['ProductID'])
    dim_product_df.to_csv('./data/stage/dim_product.csv',index = False)

    fact_sales_df =  complete_df.loc[ : , ['SalesID', 'CustomerID','ProductID',
    'Employee_ID', 'Quantity', 'Product_Price'] ]
    fact_sales_df['Total_price'] = fact_sales_df['Product_Price'].astype(float) * fact_sales_df['Quantity'].astype(int)
    fact_sales_df = fact_sales_df.sort_values(by = ['SalesID']).drop_duplicates(['SalesID','CustomerID','Employee_ID','ProductID'])
    fact_sales_df.to_csv('./data/stage/fact_sales.csv',index = False)

    # creating dimesnion and fcat in sql
    sqlEngine = create_engine('mysql+pymysql://root:root123@localhost/product_sales')
    dbConnection    = sqlEngine.connect()
    dim_customer_df.to_sql('dim_customer', con = dbConnection, if_exists = 'replace',chunksize = 1000,index=False)
    dim_employee_df.to_sql('dim_employee', con = dbConnection, if_exists = 'replace',chunksize = 1000,index=False)
    dim_product_df.to_sql('dim_product', con = dbConnection, if_exists = 'replace',chunksize = 1000,index=False)
    fact_sales_df.to_sql('fact_sales', con = dbConnection, if_exists = 'replace',chunksize = 1000,index=False)
    dbConnection.close()
    



with Flow('ETL Workflow') as flow:
    e = extract()
    t = transform(e)
    s = starSchema(t)
    l = load(t)
    

flow.run()
flow.visualize()