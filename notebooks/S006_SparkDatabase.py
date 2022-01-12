# Databricks notebook source
# spark - spark session
spark


# COMMAND ----------

spark.sql('SHOW DATABASES').show()

# COMMAND ----------

spark.sql('SHOW TABLES').show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- will create a table, data shall be stored as parquet format default
# MAGIC -- permanent table, available for other notebooks. spark applications..
# MAGIC -- not limited to spark session, shall be available after closing notebook/sessions
# MAGIC CREATE TABLE brands(brand_id INT, brand_name STRING) 

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- show tables in database
# MAGIC SHOW TABLES IN default

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * from brands;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO BRANDS (brand_id, brand_name) VALUES (1, 'Apple'), (2, 'Samsung')

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO BRANDS (brand_id, brand_name) VALUES (3, 'LG')

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC CREATE DATABASE moviedb;

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- by default uses parquet format, we can override with using datasource CSV
# MAGIC -- a folder to be created under datawhare house directory called moviedb
# MAGIC -- within moviedb, we will view reviews folder
# MAGIC CREATE TABLE moviedb.reviews (id INT, movieId INT, comment STRING) USING CSV

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN moviedb

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO moviedb.reviews (id, movieId, comment) VALUES (1, 1, 'Nice Movie')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM moviedb.reviews

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * from brands;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- FIXME: delete based on limit or range query 
# MAGIC -- delete possible here due to delta lakes, parquet
# MAGIC DELETE FROM brands where brand_id = 3

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- using csv,json, without delta lake we cannot delete, update
# MAGIC DELETE from moviedb.reviews where id = 1

# COMMAND ----------

