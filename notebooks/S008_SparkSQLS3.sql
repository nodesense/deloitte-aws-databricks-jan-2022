-- Databricks notebook source
-- delta table
-- s3 bucket as location
-- the meta like default database, table brands shall be wiht databricks cluster
CREATE TABLE brands(id INT, name STRING) LOCATION '/mnt/aws/brands'


-- COMMAND ----------

insert into brands values(1, 'apple')

-- COMMAND ----------

insert into brands values(2, 'lg')

-- COMMAND ----------

SELECT * from brands

-- COMMAND ----------

--   delete the old record[previous insert]
-- add new record
update brands set name = 'APPLE2' where id = 1

-- COMMAND ----------

delete from brands where id = 1;

-- COMMAND ----------

