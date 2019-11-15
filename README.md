# ETL-workflow
ETL workflow and data analysis. ETL-workflow using prefect and pygrametl (SCD, slow changing dimension). 
Product classification based on product name.  

How to guide:

Please run the sql script to create db or please create manually database with name :product_sales.
# CREATE DATABASE  IF NOT EXISTS `product_sales` ;
I had security inplace with user:root and password root123. Please update the code accordingly.

All the tables will be created from the input CSV file. CSV file can found in the data repository.


Run this Program
1. ETL-workflow.py: 

	Dependency: Input filename:./data/Clothing_Sales_Data_Unique_category.csv
		    Sql Database should exist.
		    
Output: 4 distinct csv files for staging purpose of star schema and 
	1 csv for transformed file which will be created after all the transformation file can found in . ./data/stage/*

2. Slowly-changing-dimension.py:
	Dependency: sql connection if the Databse and table is there then you can run this script.
output update one column.

3. Knn.py
	Dependency: dataframe-utils.py
			vectorize.py
Output will be one csv file with result comparison. ./data/results/TFIDF_distinctProduct_result.csv


Refrences:
Dataset Related information https://demos.componentone.com/aspnet/adventureworks/Products.aspx
Prefect Library https://docs.prefect.io/core/tutorials/etl.html
Pygrametl  https://chrthomsen.github.io/pygrametl/doc/examples/dimensions.html
Classification https://github.com/gallib2/product-categorization

