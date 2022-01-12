# Databricks notebook source
# spark context - sc
# spark session - entry point for Spark SQL, DataFrame etc

# Any spark driver applications will have only one Spark Context
# may have more than one spark session [isolation of tables, views]

spark

# COMMAND ----------

products = [ 
          # (product_id, product_name, brand_id)  
         (1, 'iPhone', 100),
         (2, 'Galaxy', 200),
         (3, 'Redme', 300), # orphan record, no matching brand
         (4, 'Pixel', 400),
]

brands = [
    #(brand_id, brand_name)
    (100, "Apple"),
    (200, "Samsung"),
    (400, "Google"),
    (500, "Sony"), # no matching products
]
 
# DataFrame is structured data, schema, column name, column type etc
# column type automatically derived by spark by inspecting data/interference
productDf = spark.createDataFrame(data=products, schema=["product_id", "product_name", "brand_id"])
brandDf = spark.createDataFrame(data=brands, schema=["brand_id", "brand_name"])
productDf.show()
brandDf.show()

# COMMAND ----------

# print schema for the dataframe/structured data
productDf.printSchema()

# COMMAND ----------

brandDf.printSchema()

# COMMAND ----------

# every data frame has RDD internally 
# Row objects, each row has column and values
# Dataframe is an API, actual data is still stored and distributed as RDD/partititions
print (productDf.rdd.collect())
print (productDf.rdd.getNumPartitions())

# COMMAND ----------

# dataframe APIs
# python API

# get products where brand id = 100
# RDD, Dataframe are immutable, once created, we cannot update/delete/insert data in RDD/Dataframe
# every transformation on DF/RDD, returns new DataFrame, RDD
# filteredProductsDf is new dataframe, has its own rdd, partition
filteredProductsDf = productDf.where (productDf["brand_id"] == 100)
filteredProductsDf.printSchema()
filteredProductsDf.show()
# SQL API
# select * from products where brand_id = 100
filteredProductDf2 = productDf.where("brand_id = 100")
filteredProductDf2.show()

# COMMAND ----------

# we can create a temp view/temp table in spark database, avaiable as long as driver, session active
# every dataframe can be exposed as temp table in a database called default
# this create temp table called products in spark session spark
productDf.createOrReplaceTempView("products")
# return a dataframe
filteredDf = spark.sql("select * from products where brand_id = 100")
filteredDf.printSchema()
filteredDf.show()

# databricks, not apache .
# display the result html 5, interactive bar chart, table etc, line chart pie etc
display(productDf)

# COMMAND ----------

# MAGIC %md
# MAGIC Majic function - jupyter, databricks cell etc
# MAGIC 
# MAGIC %sql
# MAGIC select * from products 
# MAGIC 
# MAGIC run the query Shift + Enter
# MAGIC 
# MAGIC the query we enterd in %sql shall be taken, wrapped in spark.sql(<<query here>>), the output 
# MAGIC   is displayed by using display - databricks specific

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from products

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, upper(product_name) as name from products

# COMMAND ----------

# using select example
productDf.select("product_id", "product_name")\
          .show()

# COMMAND ----------

# upper case using pyspark.sql.functions package
# import specific function and use it, or import all function as alias
from pyspark.sql.functions import upper

productDf.select("product_id", upper("product_name"))\
          .show()

# COMMAND ----------

# import all functions with alias name
import pyspark.sql.functions as F

productDf.select("product_id", "product_name", F.upper("product_name"), F.lower("product_name"))\
         .show()

# COMMAND ----------

# Inner Join
# productDf is left
# brandDf is right
# select/pick only matching record, discord if no matches found
productDf.join(brandDf, productDf["brand_id"] ==  brandDf["brand_id"], "inner")\
         .drop("product_id")\
         .drop(brandDf.brand_id)\
         .select("product_name", "brand_name")\
         .show()

# COMMAND ----------

