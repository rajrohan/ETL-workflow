# ETL-workflow
ETL workflow and data analysis. ETL-workflow using prefect and pygrametl (SCD, slow changing dimension). 
Product classification based on product name.  

How to guide:

Please run the sql script to create db or please create manually database with name :product_sales.
# CREATE DATABASE  IF NOT EXISTS `product_sales` ;

All the tables will be created from the input CSV file. CSV file can found in the data repository.
filename:./data/Clothing_Sales_Data_Unique_category.csv

Run this Program
1. ETL-workflow.py: 
	Dependency: Clothing_Sales_Data_Unique_category.csv
			Sql Database
Output: 4 distinct csv files for staging purpose of star schema and 1 csv for transformed file. ./data/stage/*

2. Slowly-changing-dimension.py:
	Dependency: sql connection if the Databse and table is there then you can run this script.
output update one column.

3. Knn.py
	Dependency: dataframe-utils.py
			vectorize.py
Output will be one csv file with result comparison. ./data/results/TFIDF_distinctProduct_result.csv
