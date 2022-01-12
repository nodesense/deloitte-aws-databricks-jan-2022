# Databricks notebook source
# Spark Driver, your application
# Spark driver shall have Spark context
# in DataBricks, Spark Context already created
# sc - spark context already created
sc

# COMMAND ----------

# Intellisence
# 3 times tab key
data = [1,2,3,4,5,6,7,8]
# loading hardcoded data into RDD, Spark
numbersRdd = sc.parallelize(data) # creating RDD 

print(numbersRdd)
print("partitions", numbersRdd.getNumPartitions())
# to know what data present in what partitions
dataSet = numbersRdd.glom().collect()

print(dataSet)


# COMMAND ----------

# filter is transformation method
# all transformation methods are lazy, no partititions, no tasks, no cpu, no system allocated
# oddNumbersRdd[child] drived from numbersRdd [parent]
oddNumbersRdd = numbersRdd.filter( lambda n: n % 2 == 1)
# when spark create tasks, partititon, execute the tasks? Whenever we apply action method

# collect, take - action
# collect collect data from all the partitions, bring data to driver [this program
results = oddNumbersRdd.collect() # this will ensure distribute data across partitition, apply filter, collect result
print(results)

# COMMAND ----------

# take it reads data from first partittion, we can mention number of items we need
# take is action method
results = oddNumbersRdd.take(2)
print(results)

# COMMAND ----------

oddNumbersBy10Rdd = oddNumbersRdd.map (lambda n: n * 10)
# every action would create a job, DAG, DAG scheduler, tasks, task scheduler, partittions, , executors, 
results = oddNumbersBy10Rdd.collect() # action, spark distribute data, create partitions, execute tasks
print(results)

results = oddNumbersBy10Rdd.collect() # action, spark distribute data, create partitions, execute tasks
print(results)

dataSet = oddNumbersBy10Rdd.glom().collect()
print(dataSet)


# COMMAND ----------

